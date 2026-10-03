//! Metrics 公共接口
//!
//! 定义跨 crate 共享的指标 trait 和快照类型。
//! 放置在 rk-core 中，使 rk-broker 和 rk-network 均可引用而不产生循环依赖。

/// 指标快照 (只读，可跨层传递)
#[derive(Debug, Clone, Default)]
pub struct MetricsSnapshot {
    pub uptime_secs: u64,
    pub total_requests: u64,
    pub total_responses: u64,
    pub total_errors: u64,
    pub produce_requests: u64,
    pub fetch_requests: u64,
    pub total_messages_produced: u64,
    pub total_bytes_produced: u64,
    pub total_bytes_fetched: u64,
    pub active_connections: u64,
    pub total_connections: u64,
}

/// 指标提供者 trait
///
/// 实现此 trait 的类型可以提供运行时指标快照。
/// 供 rk-network 的 HttpMetricsServer 调用，无需依赖 rk-broker 具体类型。
pub trait MetricsProvider: Send + Sync + 'static {
    /// 获取当前指标快照
    fn snapshot(&self) -> MetricsSnapshot;
}
