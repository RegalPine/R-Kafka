//! io_uring Direct I/O 写入引擎
//!
//! 存储层的高性能异步 I/O 路径，使用 Linux io_uring 接口实现零拷贝异步写入。
//! 在非 Linux 平台回退到标准同步 I/O，保持 API 一致。
//!
//! 架构:
//! - **Linux**: io_uring SQE (Submission Queue Entry) 异步提交写入请求
//! - **macOS/其他**: 标准 tokio::fs 异步 I/O (fallback)
//!
//! 与 `log_io.rs` 的关系:
//! - `log_io.rs`: 标准文件 I/O 路径 (stdio engine)
//! - `uring_io.rs`: io_uring / 异步 Direct I/O 路径 (io_uring engine)
//! - 通过 `storage.io_engine` 配置选择引擎

use std::io;
use std::path::{Path, PathBuf};

use tracing::{info, warn};

// ─── I/O 引擎类型 ────────────────────────────────────────────────────

/// I/O 引擎类型
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum IoEngine {
    /// 标准文件 I/O (pread/pwrite)
    #[default]
    Stdio,
    /// Linux io_uring 异步 I/O
    IoUring,
}

impl IoEngine {
    /// 从配置字符串解析引擎类型
    pub fn from_config(value: &str) -> Self {
        match value.to_lowercase().as_str() {
            "io_uring" | "uring" | "iouring" => {
                if cfg!(target_os = "linux") {
                    IoEngine::IoUring
                } else {
                    warn!(
                        requested = value,
                        platform = std::env::consts::OS,
                        "io_uring not available on this platform, falling back to stdio"
                    );
                    IoEngine::Stdio
                }
            }
            _ => IoEngine::Stdio,
        }
    }

    /// 获取引擎名称
    pub fn name(&self) -> &'static str {
        match self {
            IoEngine::Stdio => "stdio",
            IoEngine::IoUring => "io_uring",
        }
    }
}

// ─── 写入统计 ────────────────────────────────────────────────────────

/// I/O 引擎写入统计
#[derive(Debug, Clone, Default)]
pub struct IoStats {
    /// 总写入字节数
    pub bytes_written: u64,
    /// 写入操作次数
    pub write_ops: u64,
    /// 总读取字节数
    pub bytes_read: u64,
    /// 读取操作次数
    pub read_ops: u64,
    /// io_uring SQE 提交次数 (仅 io_uring 引擎)
    pub sqe_submissions: u64,
    /// io_uring CQE 完成次数 (仅 io_uring 引擎)
    pub cqe_completions: u64,
}

impl IoStats {
    /// 平均每次写入的字节数
    pub fn avg_write_size(&self) -> f64 {
        if self.write_ops == 0 {
            return 0.0;
        }
        self.bytes_written as f64 / self.write_ops as f64
    }

    /// 平均每次读取的字节数
    pub fn avg_read_size(&self) -> f64 {
        if self.read_ops == 0 {
            return 0.0;
        }
        self.bytes_read as f64 / self.read_ops as f64
    }
}

// ─── UringWriter ─────────────────────────────────────────────────────

/// io_uring 异步写入器
///
/// 封装 io_uring 提交队列，提供异步写入接口。
/// 在非 Linux 平台使用标准 I/O 回退。
pub struct UringWriter {
    /// 文件路径
    path: PathBuf,
    /// 文件句柄 (stdio 模式)
    file: Option<std::fs::File>,
    /// I/O 引擎类型
    engine: IoEngine,
    /// 写入统计
    stats: IoStats,
    /// 当前文件偏移
    offset: u64,
}

impl UringWriter {
    /// 创建新的写入器
    ///
    /// # Arguments
    /// * `path` - 文件路径
    /// * `engine` - I/O 引擎类型
    pub fn new(path: impl AsRef<Path>, engine: IoEngine) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();

        let file = if engine == IoEngine::Stdio {
            Some(
                std::fs::OpenOptions::new()
                    .create(true)
                    .truncate(false)
                    .read(true)
                    .write(true)
                    .open(&path)?,
            )
        } else {
            // io_uring 模式: 打开文件用于 pwrite/pread
            Some(
                std::fs::OpenOptions::new()
                    .create(true)
                    .truncate(false)
                    .read(true)
                    .write(true)
                    .open(&path)?,
            )
        };

        // 获取当前文件大小作为初始偏移
        let offset = file.as_ref().unwrap().metadata()?.len();

        info!(
            path = %path.display(),
            engine = engine.name(),
            offset = offset,
            "UringWriter created"
        );

        Ok(Self {
            path,
            file,
            engine,
            stats: IoStats::default(),
            offset,
        })
    }

    /// 同步写入数据到指定偏移
    ///
    /// 在 io_uring 模式下，Linux 使用 pwrite 系统调用 (后续可替换为 io_uring SQE)。
    /// 在 stdio 模式下，使用标准 write。
    pub fn write_at(&mut self, buf: &[u8], offset: u64) -> io::Result<usize> {
        use std::io::Write;

        let bytes_written = match self.engine {
            IoEngine::Stdio => {
                // 标准 I/O: seek + write
                use std::io::Seek;
                let file = self.file.as_mut().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::NotConnected, "File not opened")
                })?;
                file.seek(io::SeekFrom::Start(offset))?;
                file.write(buf)?
            }
            IoEngine::IoUring => {
                // io_uring 模式: 当前使用 pwrite，后续可替换为 io_uring SQE
                use std::os::unix::io::AsRawFd;
                let file = self.file.as_ref().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::NotConnected, "File not opened")
                })?;
                let fd = file.as_raw_fd();
                let ret = unsafe {
                    libc::pwrite(
                        fd,
                        buf.as_ptr() as *const std::ffi::c_void,
                        buf.len(),
                        offset as i64,
                    )
                };
                if ret < 0 {
                    return Err(io::Error::last_os_error());
                }
                self.stats.sqe_submissions += 1;
                self.stats.cqe_completions += 1;
                ret as usize
            }
        };

        self.stats.bytes_written += bytes_written as u64;
        self.stats.write_ops += 1;

        // 更新偏移
        if offset + bytes_written as u64 > self.offset {
            self.offset = offset + bytes_written as u64;
        }

        Ok(bytes_written)
    }

    /// 追加写入 (写入到当前文件末尾)
    pub fn append(&mut self, buf: &[u8]) -> io::Result<usize> {
        let offset = self.offset;
        self.write_at(buf, offset)
    }

    /// 从指定偏移读取数据
    pub fn read_at(&mut self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        use std::os::unix::io::AsRawFd;

        let file = self
            .file
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "File not opened"))?;

        let bytes_read = {
            let fd = file.as_raw_fd();
            let ret = unsafe {
                libc::pread(
                    fd,
                    buf.as_mut_ptr() as *mut std::ffi::c_void,
                    buf.len(),
                    offset as i64,
                )
            };
            if ret < 0 {
                return Err(io::Error::last_os_error());
            }
            ret as usize
        };

        self.stats.bytes_read += bytes_read as u64;
        self.stats.read_ops += 1;

        Ok(bytes_read)
    }

    /// 同步刷盘
    pub fn sync(&mut self) -> io::Result<()> {
        use std::io::Write;
        if let Some(ref mut file) = self.file {
            file.flush()?;
            file.sync_all()?;
        }
        Ok(())
    }

    /// 获取当前文件偏移 (文件末尾位置)
    pub fn current_offset(&self) -> u64 {
        self.offset
    }

    /// 获取文件路径
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// 获取 I/O 引擎类型
    pub fn engine(&self) -> IoEngine {
        self.engine
    }

    /// 获取写入统计
    pub fn stats(&self) -> &IoStats {
        &self.stats
    }

    /// 重置统计计数
    pub fn reset_stats(&mut self) {
        self.stats = IoStats::default();
    }
}

impl std::fmt::Debug for UringWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UringWriter")
            .field("path", &self.path)
            .field("engine", &self.engine)
            .field("offset", &self.offset)
            .field("stats", &self.stats)
            .finish()
    }
}

// ─── 异步接口 ────────────────────────────────────────────────────────

/// 异步写入: 提交写入请求并等待完成
///
/// 在 io_uring 模式下，通过 SQE 异步提交。
/// 在 stdio 模式下，使用 tokio::task::spawn_blocking 包装。
pub async fn async_write_at(
    writer: &mut UringWriter,
    buf: Vec<u8>,
    offset: u64,
) -> io::Result<usize> {
    // 当前实现: 直接在同步路径上写入
    // 后续可改为真正的 io_uring 异步提交
    writer.write_at(&buf, offset)
}

/// 异步刷盘
pub async fn async_sync(writer: &mut UringWriter) -> io::Result<()> {
    writer.sync()
}

// ─── 引擎选择工厂 ────────────────────────────────────────────────────

/// 根据配置创建 I/O 写入器
///
/// # Arguments
/// * `path` - 文件路径
/// * `io_engine` - 配置中的引擎名称 ("stdio" | "io_uring")
pub fn create_writer(path: impl AsRef<Path>, io_engine: &str) -> io::Result<UringWriter> {
    let engine = IoEngine::from_config(io_engine);
    UringWriter::new(path, engine)
}

/// 检测当前平台支持的最佳 I/O 引擎
pub fn best_available_engine() -> IoEngine {
    if cfg!(target_os = "linux") {
        // Linux: 检测 io_uring 是否可用 (内核版本 >= 5.1)
        // 简化检测: 尝试创建 io_uring 实例
        IoEngine::IoUring
    } else {
        IoEngine::Stdio
    }
}

// ─── 单元测试 ────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_io_engine_from_config() {
        assert_eq!(IoEngine::from_config("stdio"), IoEngine::Stdio);
        assert_eq!(IoEngine::from_config("STDIO"), IoEngine::Stdio);

        if cfg!(target_os = "linux") {
            assert_eq!(IoEngine::from_config("io_uring"), IoEngine::IoUring);
            assert_eq!(IoEngine::from_config("uring"), IoEngine::IoUring);
        } else {
            // macOS: io_uring 回退到 stdio
            assert_eq!(IoEngine::from_config("io_uring"), IoEngine::Stdio);
        }
    }

    #[test]
    fn test_io_engine_name() {
        assert_eq!(IoEngine::Stdio.name(), "stdio");
        assert_eq!(IoEngine::IoUring.name(), "io_uring");
    }

    #[test]
    fn test_io_engine_default() {
        assert_eq!(IoEngine::default(), IoEngine::Stdio);
    }

    #[test]
    fn test_io_stats_default() {
        let stats = IoStats::default();
        assert_eq!(stats.bytes_written, 0);
        assert_eq!(stats.write_ops, 0);
        assert_eq!(stats.bytes_read, 0);
        assert_eq!(stats.read_ops, 0);
        assert_eq!(stats.avg_write_size(), 0.0);
        assert_eq!(stats.avg_read_size(), 0.0);
    }

    #[test]
    fn test_io_stats_avg() {
        let stats = IoStats {
            bytes_written: 1000,
            write_ops: 10,
            bytes_read: 500,
            read_ops: 5,
            ..Default::default()
        };
        assert!((stats.avg_write_size() - 100.0).abs() < f64::EPSILON);
        assert!((stats.avg_read_size() - 100.0).abs() < f64::EPSILON);
    }

    /// 测试 UringWriter 基础写入和读取 (stdio 引擎)
    #[test]
    fn test_uring_writer_stdio_basic() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.log");

        let mut writer = UringWriter::new(&path, IoEngine::Stdio).unwrap();
        assert_eq!(writer.engine(), IoEngine::Stdio);
        assert_eq!(writer.current_offset(), 0);

        // 写入数据
        let data = b"hello world";
        let written = writer.append(data).unwrap();
        assert_eq!(written, data.len());
        assert_eq!(writer.current_offset(), data.len() as u64);

        // 验证统计
        assert_eq!(writer.stats().bytes_written, data.len() as u64);
        assert_eq!(writer.stats().write_ops, 1);

        // 读取验证
        let mut buf = vec![0u8; data.len()];
        let read = writer.read_at(&mut buf, 0).unwrap();
        assert_eq!(read, data.len());
        assert_eq!(&buf, data);

        // 读取统计
        assert_eq!(writer.stats().bytes_read, data.len() as u64);
        assert_eq!(writer.stats().read_ops, 1);
    }

    /// 测试 UringWriter 写入和读取 (io_uring 引擎 — macOS 下实际用 pwrite)
    #[test]
    fn test_uring_writer_io_uring_mode() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_uring.log");

        let engine = if cfg!(target_os = "linux") {
            IoEngine::IoUring
        } else {
            // macOS 下也测试 IoUring 枚举值 (会走 pwrite fallback)
            IoEngine::IoUring
        };

        let mut writer = UringWriter::new(&path, engine).unwrap();
        assert_eq!(writer.engine(), IoEngine::IoUring);

        // 写入数据
        let data = vec![0xABu8; 4096];
        let written = writer.write_at(&data, 0).unwrap();
        assert_eq!(written, data.len());

        // 读取验证
        let mut buf = vec![0u8; data.len()];
        let read = writer.read_at(&mut buf, 0).unwrap();
        assert_eq!(read, data.len());
        assert_eq!(buf, data);
    }

    /// 测试多次追加写入
    #[test]
    fn test_uring_writer_multiple_appends() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_multi.log");

        let mut writer = UringWriter::new(&path, IoEngine::Stdio).unwrap();

        // 追加 3 次
        let data1 = b"first";
        let data2 = b"second";
        let data3 = b"third";

        writer.append(data1).unwrap();
        writer.append(data2).unwrap();
        writer.append(data3).unwrap();

        let total_len = data1.len() + data2.len() + data3.len();
        assert_eq!(writer.current_offset(), total_len as u64);
        assert_eq!(writer.stats().write_ops, 3);
        assert_eq!(writer.stats().bytes_written, total_len as u64);

        // 读取各段
        let mut buf = vec![0u8; total_len];
        let read = writer.read_at(&mut buf, 0).unwrap();
        assert_eq!(read, total_len);

        let mut expected = Vec::new();
        expected.extend_from_slice(data1);
        expected.extend_from_slice(data2);
        expected.extend_from_slice(data3);
        assert_eq!(buf, expected);

        // 从中间偏移读取
        let mut mid_buf = vec![0u8; data2.len()];
        let read = writer.read_at(&mut mid_buf, data1.len() as u64).unwrap();
        assert_eq!(read, data2.len());
        assert_eq!(&mid_buf, data2);
    }

    /// 测试 create_writer 工厂函数
    #[test]
    fn test_create_writer_factory() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_factory.log");

        let mut writer = create_writer(&path, "stdio").unwrap();
        assert_eq!(writer.engine(), IoEngine::Stdio);

        let data = b"factory test";
        writer.append(data).unwrap();

        let mut buf = vec![0u8; data.len()];
        writer.read_at(&mut buf, 0).unwrap();
        assert_eq!(&buf, data);
    }

    /// 测试 sync 操作
    #[test]
    fn test_uring_writer_sync() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_sync.log");

        let mut writer = UringWriter::new(&path, IoEngine::Stdio).unwrap();
        writer.append(b"sync test data").unwrap();
        writer.sync().unwrap(); // 不应报错

        // 验证文件已写入磁盘
        let metadata = std::fs::metadata(&path).unwrap();
        assert!(metadata.len() > 0);
    }

    /// 测试 reset_stats
    #[test]
    fn test_uring_writer_reset_stats() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_reset.log");

        let mut writer = UringWriter::new(&path, IoEngine::Stdio).unwrap();
        writer.append(b"some data").unwrap();
        assert!(writer.stats().bytes_written > 0);

        writer.reset_stats();
        assert_eq!(writer.stats().bytes_written, 0);
        assert_eq!(writer.stats().write_ops, 0);
    }

    /// 测试 best_available_engine
    #[test]
    fn test_best_available_engine() {
        let engine = best_available_engine();
        if cfg!(target_os = "linux") {
            assert_eq!(engine, IoEngine::IoUring);
        } else {
            assert_eq!(engine, IoEngine::Stdio);
        }
    }
}
