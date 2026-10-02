//! Flush 策略: Async / Sync / Hybrid
//!
//! | 模式 | 行为 | 适用场景 |
//! |:---|:---|:---|
//! | **Async Flush** | 写入 Page Cache 即返回，后台刷盘 | 高吞吐，允许少量丢失 |
//! | **Sync Flush** | `fsync` 后才返回 ACK | 强一致，金融级 |
//! | **Hybrid (推荐)** | 每 N ms 或 M bytes 触发一次 fsync | 吞吐与安全的平衡 |

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Flush 模式
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushMode {
    /// 写入 Page Cache 即返回，后台刷盘
    Async,
    /// fsync 后才返回 ACK
    Sync,
    /// 每 N ms 或 M bytes 触发一次 fsync
    Hybrid,
}

impl FlushMode {
    pub fn parse(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "async" => Self::Async,
            "sync" => Self::Sync,
            "hybrid" => Self::Hybrid,
            _ => Self::Hybrid, // 默认 hybrid
        }
    }
}

/// Flush 策略控制器
///
/// 跟踪自上次 flush 以来的写入字节数和时间，
/// 判断是否需要触发 flush。
pub struct FlushPolicy {
    mode: FlushMode,
    /// Hybrid 模式: flush 间隔 (毫秒)
    flush_interval_ms: u64,
    /// Hybrid 模式: flush 间隔 (字节数)
    flush_interval_bytes: u64,
    /// 自上次 flush 以来的写入字节数
    bytes_since_flush: AtomicU64,
    /// 上次 flush 的时间
    last_flush_time: std::sync::Mutex<Instant>,
}

impl FlushPolicy {
    pub fn new(mode: FlushMode, flush_interval_ms: u64) -> Self {
        Self {
            mode,
            flush_interval_ms,
            flush_interval_bytes: 1_048_576, // 1MB
            bytes_since_flush: AtomicU64::new(0),
            last_flush_time: std::sync::Mutex::new(Instant::now()),
        }
    }

    /// 记录一次写入
    pub fn record_write(&self, bytes: u64) {
        self.bytes_since_flush.fetch_add(bytes, Ordering::Relaxed);
    }

    /// 检查是否需要 flush
    pub fn should_flush(&self) -> bool {
        match self.mode {
            FlushMode::Async => false, // Async 模式不主动 flush
            FlushMode::Sync => true,   // Sync 模式每次写入后都 flush
            FlushMode::Hybrid => {
                // 检查字节阈值
                let bytes = self.bytes_since_flush.load(Ordering::Relaxed);
                if bytes >= self.flush_interval_bytes {
                    return true;
                }

                // 检查时间阈值
                if let Ok(last) = self.last_flush_time.lock() {
                    last.elapsed() >= Duration::from_millis(self.flush_interval_ms)
                } else {
                    false
                }
            }
        }
    }

    /// 标记 flush 已完成
    pub fn mark_flushed(&self) {
        self.bytes_since_flush.store(0, Ordering::Relaxed);
        if let Ok(mut last) = self.last_flush_time.lock() {
            *last = Instant::now();
        }
    }

    /// 获取模式
    pub fn mode(&self) -> FlushMode {
        self.mode
    }

    /// 获取自上次 flush 以来的字节数
    pub fn pending_bytes(&self) -> u64 {
        self.bytes_since_flush.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_flush_mode_from_str() {
        assert_eq!(FlushMode::parse("async"), FlushMode::Async);
        assert_eq!(FlushMode::parse("sync"), FlushMode::Sync);
        assert_eq!(FlushMode::parse("hybrid"), FlushMode::Hybrid);
        assert_eq!(FlushMode::parse("unknown"), FlushMode::Hybrid);
    }

    #[test]
    fn test_sync_always_flush() {
        let policy = FlushPolicy::new(FlushMode::Sync, 100);
        assert!(policy.should_flush());
    }

    #[test]
    fn test_async_never_flush() {
        let policy = FlushPolicy::new(FlushMode::Async, 100);
        policy.record_write(1_000_000_000);
        assert!(!policy.should_flush());
    }

    #[test]
    fn test_hybrid_bytes_threshold() {
        let policy = FlushPolicy::new(FlushMode::Hybrid, 60000); // 60s interval
        assert!(!policy.should_flush());

        // 写入超过 1MB
        policy.record_write(2_000_000);
        assert!(policy.should_flush());

        policy.mark_flushed();
        assert!(!policy.should_flush());
        assert_eq!(policy.pending_bytes(), 0);
    }

    #[test]
    fn test_hybrid_time_threshold() {
        let policy = FlushPolicy::new(FlushMode::Hybrid, 10); // 10ms interval

        // 刚初始化时 last_flush_time 是 now，所以不触发
        assert!(!policy.should_flush());

        // 等待超过阈值
        thread::sleep(Duration::from_millis(20));
        assert!(policy.should_flush());

        policy.mark_flushed();
        assert!(!policy.should_flush());
    }
}
