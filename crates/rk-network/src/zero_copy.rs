//! Zero-Copy 数据传输 — splice / sendfile
//!
//! Fetch 响应路径的零拷贝优化: 将磁盘文件数据直接传输到 TCP Socket，
//! 避免数据拷贝到用户态缓冲区，减少 CPU 和内存带宽消耗。
//!
//! 平台实现:
//! - **Linux**: `splice(fd_file → pipe → fd_socket)` — 完全内核态零拷贝
//! - **macOS**: `sendfile(fd_file → fd_socket)` — 内核态直接发送
//!
//! 使用场景:
//! 当 Fetch 响应中的 record_set 完整位于一个 Segment 文件中时，
//! 可以使用 zero_copy_send 替代传统的 read → write 路径。
//!
//! 性能收益:
//! - 减少 1-2 次用户态/内核态上下文切换
//! - 避免数据在 Page Cache → 用户缓冲区 → Socket 缓冲区之间的拷贝
//! - 降低 CPU 占用，提升吞吐

use std::io;
use std::os::unix::io::AsRawFd;

use tokio::net::TcpStream;
use tracing::trace;

// ─── Zero-Copy 统计 ─────────────────────────────────────────────────

/// Zero-Copy 操作统计
#[derive(Debug, Clone, Default)]
pub struct ZeroCopyStats {
    /// 成功传输的字节数
    pub bytes_transferred: u64,
    /// 操作次数
    pub operations: u64,
    /// 回退到传统 read+write 的次数 (zero-copy 失败时)
    pub fallback_count: u64,
}

impl ZeroCopyStats {
    /// 平均每次传输的字节数
    pub fn avg_bytes_per_op(&self) -> f64 {
        if self.operations == 0 {
            return 0.0;
        }
        self.bytes_transferred as f64 / self.operations as f64
    }
}

// ─── 平台特定实现 ────────────────────────────────────────────────────

/// 零拷贝发送: 从文件描述符直接传输到 TCP Socket
///
/// # Arguments
/// * `file` - 源文件 (Segment Log 文件)
/// * `offset` - 文件中的起始偏移
/// * `len` - 要传输的字节数
/// * `socket` - 目标 TCP Socket
///
/// # Returns
/// 实际传输的字节数
///
/// # Platform
/// - Linux: 使用 splice() 通过 pipe 在内核态完成传输
/// - macOS: 使用 sendfile() 直接发送到 socket
#[cfg(target_os = "linux")]
pub fn zero_copy_send(
    file: &std::fs::File,
    offset: u64,
    len: usize,
    socket: &TcpStream,
) -> io::Result<usize> {
    use std::os::unix::io::AsRawFd;

    let file_fd = file.as_raw_fd();
    let socket_fd = socket.as_raw_fd();

    // Linux splice 需要通过 pipe 中转
    let (pipe_rd, pipe_wrt) = create_pipe()?;

    let mut total_sent = 0usize;
    let mut current_offset = offset as i64;

    while total_sent < len {
        let remaining = len - total_sent;
        // splice 单次最大 0x7ffff000 (约 2GB)
        let chunk = std::cmp::min(remaining, 0x7ffff000);

        // file → pipe
        let n_read = unsafe {
            libc::splice(
                file_fd,
                &mut current_offset,
                pipe_wrt,
                std::ptr::null_mut(),
                chunk as libc::size_t,
                0,
            )
        };

        if n_read < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            // 关闭 pipe 并返回错误
            unsafe {
                libc::close(pipe_rd);
                libc::close(pipe_wrt);
            }
            return Err(err);
        }
        if n_read == 0 {
            break; // EOF
        }

        // pipe → socket
        let mut pipe_read = n_read;
        while pipe_read > 0 {
            let n_sent = unsafe {
                libc::splice(
                    pipe_rd,
                    std::ptr::null_mut(),
                    socket_fd,
                    std::ptr::null_mut(),
                    pipe_read as libc::size_t,
                    0,
                )
            };

            if n_sent < 0 {
                let err = io::Error::last_os_error();
                if err.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                unsafe {
                    libc::close(pipe_rd);
                    libc::close(pipe_wrt);
                }
                return Err(err);
            }
            if n_sent == 0 {
                break;
            }
            pipe_read -= n_sent as usize;
            total_sent += n_sent as usize;
        }
    }

    unsafe {
        libc::close(pipe_rd);
        libc::close(pipe_wrt);
    }

    trace!(
        offset = offset,
        len = len,
        sent = total_sent,
        "zero_copy_send (Linux splice) completed"
    );

    Ok(total_sent)
}

/// 零拷贝发送: macOS sendfile 实现
#[cfg(target_os = "macos")]
pub fn zero_copy_send(
    file: &std::fs::File,
    offset: u64,
    len: usize,
    socket: &TcpStream,
) -> io::Result<usize> {
    let file_fd = file.as_raw_fd();
    let socket_fd = socket.as_raw_fd();

    let mut total_sent = 0usize;
    let mut current_offset = offset;

    while total_sent < len {
        let remaining = (len - total_sent) as i64;
        let mut sfbytes = remaining;

        let ret = unsafe {
            libc::sendfile(
                file_fd,
                socket_fd,
                current_offset as i64,
                &mut sfbytes,
                std::ptr::null_mut(),
                0,
            )
        };

        if ret < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::Interrupted {
                // sendfile 可能已传输了部分数据
                total_sent += sfbytes as usize;
                current_offset += sfbytes as u64;
                continue;
            }
            // EAGAIN: 非阻塞 socket 缓冲区满，已传输部分数据
            if err.raw_os_error() == Some(libc::EAGAIN) {
                total_sent += sfbytes as usize;
                break;
            }
            return Err(err);
        }

        total_sent += sfbytes as usize;
        current_offset += sfbytes as u64;

        if sfbytes == 0 {
            break; // EOF
        }
    }

    trace!(
        offset = offset,
        len = len,
        sent = total_sent,
        "zero_copy_send (macOS sendfile) completed"
    );

    Ok(total_sent)
}

/// 其他平台的 fallback (不应在生产环境使用)
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub fn zero_copy_send(
    _file: &std::fs::File,
    _offset: u64,
    _len: usize,
    _socket: &TcpStream,
) -> io::Result<usize> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "zero_copy_send not supported on this platform",
    ))
}

// ─── Linux pipe 辅助 ────────────────────────────────────────────────

#[cfg(target_os = "linux")]
fn create_pipe() -> io::Result<(i32, i32)> {
    let mut fds = [0i32; 2];
    let ret = unsafe { libc::pipe(fds.as_mut_ptr()) };
    if ret < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok((fds[0], fds[1]))
}

// ─── 同步零拷贝发送 (用于非 async 上下文) ────────────────────────────

/// 同步版本的零拷贝发送 (使用 std::net::TcpStream)
///
/// 用于在同步上下文中 (如测试或内部调用) 进行零拷贝传输。
#[cfg(target_os = "macos")]
pub fn zero_copy_send_sync(
    file: &std::fs::File,
    offset: u64,
    len: usize,
    socket: &std::net::TcpStream,
) -> io::Result<usize> {
    let file_fd = file.as_raw_fd();
    let socket_fd = socket.as_raw_fd();

    let mut total_sent = 0usize;
    let mut current_offset = offset;

    while total_sent < len {
        let remaining = (len - total_sent) as i64;
        let mut sfbytes = remaining;

        let ret = unsafe {
            libc::sendfile(
                file_fd,
                socket_fd,
                current_offset as i64,
                &mut sfbytes,
                std::ptr::null_mut(),
                0,
            )
        };

        if ret < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::Interrupted {
                total_sent += sfbytes as usize;
                current_offset += sfbytes as u64;
                continue;
            }
            if err.raw_os_error() == Some(libc::EAGAIN) {
                total_sent += sfbytes as usize;
                break;
            }
            return Err(err);
        }

        total_sent += sfbytes as usize;
        current_offset += sfbytes as u64;

        if sfbytes == 0 {
            break;
        }
    }

    Ok(total_sent)
}

#[cfg(target_os = "linux")]
pub fn zero_copy_send_sync(
    file: &std::fs::File,
    offset: u64,
    len: usize,
    socket: &std::net::TcpStream,
) -> io::Result<usize> {
    let file_fd = file.as_raw_fd();
    let socket_fd = socket.as_raw_fd();

    let (pipe_rd, pipe_wrt) = create_pipe()?;
    let mut total_sent = 0usize;
    let mut current_offset = offset as i64;

    while total_sent < len {
        let remaining = len - total_sent;
        let chunk = std::cmp::min(remaining, 0x7ffff000);

        let n_read = unsafe {
            libc::splice(
                file_fd,
                &mut current_offset,
                pipe_wrt,
                std::ptr::null_mut(),
                chunk as libc::size_t,
                0,
            )
        };

        if n_read <= 0 {
            break;
        }

        let mut pipe_read = n_read;
        while pipe_read > 0 {
            let n_sent = unsafe {
                libc::splice(
                    pipe_rd,
                    std::ptr::null_mut(),
                    socket_fd,
                    std::ptr::null_mut(),
                    pipe_read as libc::size_t,
                    0,
                )
            };
            if n_sent <= 0 {
                break;
            }
            pipe_read -= n_sent as usize;
            total_sent += n_sent as usize;
        }
    }

    unsafe {
        libc::close(pipe_rd);
        libc::close(pipe_wrt);
    }

    Ok(total_sent)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub fn zero_copy_send_sync(
    _file: &std::fs::File,
    _offset: u64,
    _len: usize,
    _socket: &std::net::TcpStream,
) -> io::Result<usize> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "zero_copy_send_sync not supported on this platform",
    ))
}

// ─── 能力检测 ────────────────────────────────────────────────────────

/// 检查当前平台是否支持零拷贝传输
pub fn is_zero_copy_supported() -> bool {
    cfg!(any(target_os = "linux", target_os = "macos"))
}

/// 获取零拷贝实现名称
pub fn zero_copy_implementation() -> &'static str {
    if cfg!(target_os = "linux") {
        "splice"
    } else if cfg!(target_os = "macos") {
        "sendfile"
    } else {
        "unsupported"
    }
}

// ─── 单元测试 ────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use tempfile::NamedTempFile;

    #[test]
    fn test_zero_copy_stats_default() {
        let stats = ZeroCopyStats::default();
        assert_eq!(stats.bytes_transferred, 0);
        assert_eq!(stats.operations, 0);
        assert_eq!(stats.fallback_count, 0);
        assert_eq!(stats.avg_bytes_per_op(), 0.0);
    }

    #[test]
    fn test_zero_copy_stats_avg() {
        let stats = ZeroCopyStats {
            bytes_transferred: 1000,
            operations: 10,
            fallback_count: 0,
        };
        assert!((stats.avg_bytes_per_op() - 100.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_platform_detection() {
        if cfg!(any(target_os = "linux", target_os = "macos")) {
            assert!(is_zero_copy_supported());
            let impl_name = zero_copy_implementation();
            assert!(impl_name == "splice" || impl_name == "sendfile");
        }
    }

    /// 测试 zero_copy_send_sync: 从临时文件传输到 socket pair
    #[test]
    fn test_zero_copy_send_sync_basic() {
        if !is_zero_copy_supported() {
            return;
        }

        // 创建临时文件并写入测试数据
        let mut tmp = NamedTempFile::new().unwrap();
        let test_data = vec![0xABu8; 4096];
        tmp.write_all(&test_data).unwrap();
        tmp.flush().unwrap();

        let file = tmp.as_file();

        // 创建 TCP socket pair
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let sender = std::net::TcpStream::connect(addr).unwrap();
        let mut receiver = listener.accept().unwrap().0;

        sender.set_nonblocking(false).unwrap();
        receiver.set_nonblocking(false).unwrap();

        // 使用 zero_copy_send_sync 传输
        let sent = zero_copy_send_sync(file, 0, test_data.len(), &sender).unwrap();
        assert_eq!(sent, test_data.len(), "Should transfer all bytes");

        // 从接收端读取并验证
        let mut received = vec![0u8; test_data.len()];
        receiver
            .set_read_timeout(Some(std::time::Duration::from_secs(2)))
            .unwrap();
        let mut total_read = 0;
        while total_read < received.len() {
            match receiver.read(&mut received[total_read..]) {
                Ok(0) => break,
                Ok(n) => total_read += n,
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => panic!("Read error: {}", e),
            }
        }
        assert_eq!(total_read, test_data.len());
        assert_eq!(received, test_data, "Data should match");
    }

    /// 测试 partial transfer: 从文件中间偏移开始传输
    #[test]
    fn test_zero_copy_send_sync_offset() {
        if !is_zero_copy_supported() {
            return;
        }

        let mut tmp = NamedTempFile::new().unwrap();
        let test_data: Vec<u8> = (0..255).collect();
        tmp.write_all(&test_data).unwrap();
        tmp.flush().unwrap();

        let file = tmp.as_file();

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let sender = std::net::TcpStream::connect(addr).unwrap();
        let mut receiver = listener.accept().unwrap().0;
        sender.set_nonblocking(false).unwrap();
        receiver.set_nonblocking(false).unwrap();

        // 从偏移 100 开始传输 50 字节
        let sent = zero_copy_send_sync(file, 100, 50, &sender).unwrap();
        assert_eq!(sent, 50);

        let mut received = vec![0u8; 50];
        receiver
            .set_read_timeout(Some(std::time::Duration::from_secs(2)))
            .unwrap();
        let mut total_read = 0;
        while total_read < 50 {
            match receiver.read(&mut received[total_read..]) {
                Ok(0) => break,
                Ok(n) => total_read += n,
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => panic!("Read error: {}", e),
            }
        }
        assert_eq!(&received, &test_data[100..150]);
    }

    /// 测试空文件传输
    #[test]
    fn test_zero_copy_send_sync_empty() {
        if !is_zero_copy_supported() {
            return;
        }

        let tmp = NamedTempFile::new().unwrap();
        let file = tmp.as_file();

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let sender = std::net::TcpStream::connect(addr).unwrap();
        let _receiver = listener.accept().unwrap().0;
        sender.set_nonblocking(false).unwrap();

        // 传输 0 字节
        let sent = zero_copy_send_sync(file, 0, 0, &sender).unwrap();
        assert_eq!(sent, 0);
    }
}
