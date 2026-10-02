//! Broker Metrics
//!
//! Broker 运行时指标收集器。
//! 使用 AtomicU64 实现无锁计数器，支持实时查询。

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Broker 运行时指标收集器
pub struct BrokerMetrics {
    /// 启动时间
    start_time: Instant,
    /// 总请求数
    total_requests: AtomicU64,
    /// 总响应数
    total_responses: AtomicU64,
    /// 总错误数
    total_errors: AtomicU64,
    /// Produce 请求数
    produce_requests: AtomicU64,
    /// Fetch 请求数
    fetch_requests: AtomicU64,
    /// 总 Produce 消息数
    total_messages_produced: AtomicU64,
    /// 总 Produce 字节数
    total_bytes_produced: AtomicU64,
    /// 总 Fetch 字节数
    total_bytes_fetched: AtomicU64,
    /// 活跃连接数
    active_connections: AtomicU64,
    /// 总连接数
    total_connections: AtomicU64,
}

impl BrokerMetrics {
    /// 创建新的指标收集器
    pub fn new() -> Self {
        Self {
            start_time: Instant::now(),
            total_requests: AtomicU64::new(0),
            total_responses: AtomicU64::new(0),
            total_errors: AtomicU64::new(0),
            produce_requests: AtomicU64::new(0),
            fetch_requests: AtomicU64::new(0),
            total_messages_produced: AtomicU64::new(0),
            total_bytes_produced: AtomicU64::new(0),
            total_bytes_fetched: AtomicU64::new(0),
            active_connections: AtomicU64::new(0),
            total_connections: AtomicU64::new(0),
        }
    }

    /// 记录一次请求
    pub fn record_request(&self, api_key: i16) {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
        match api_key {
            0 => { self.produce_requests.fetch_add(1, Ordering::Relaxed); }
            1 => { self.fetch_requests.fetch_add(1, Ordering::Relaxed); }
            _ => {}
        }
    }

    /// 记录一次响应
    pub fn record_response(&self) {
        self.total_responses.fetch_add(1, Ordering::Relaxed);
    }

    /// 记录一次错误
    pub fn record_error(&self) {
        self.total_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// 记录 Produce 消息
    pub fn record_produce(&self, message_count: u64, byte_count: u64) {
        self.total_messages_produced.fetch_add(message_count, Ordering::Relaxed);
        self.total_bytes_produced.fetch_add(byte_count, Ordering::Relaxed);
    }

    /// 记录 Fetch 字节数
    pub fn record_fetch(&self, byte_count: u64) {
        self.total_bytes_fetched.fetch_add(byte_count, Ordering::Relaxed);
    }

    /// 记录新连接
    pub fn record_connection(&self) {
        self.active_connections.fetch_add(1, Ordering::Relaxed);
        self.total_connections.fetch_add(1, Ordering::Relaxed);
    }

    /// 记录连接断开
    pub fn record_disconnect(&self) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }

    /// 获取指标快照
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            uptime_secs: self.start_time.elapsed().as_secs(),
            total_requests: self.total_requests.load(Ordering::Relaxed),
            total_responses: self.total_responses.load(Ordering::Relaxed),
            total_errors: self.total_errors.load(Ordering::Relaxed),
            produce_requests: self.produce_requests.load(Ordering::Relaxed),
            fetch_requests: self.fetch_requests.load(Ordering::Relaxed),
            total_messages_produced: self.total_messages_produced.load(Ordering::Relaxed),
            total_bytes_produced: self.total_bytes_produced.load(Ordering::Relaxed),
            total_bytes_fetched: self.total_bytes_fetched.load(Ordering::Relaxed),
            active_connections: self.active_connections.load(Ordering::Relaxed),
            total_connections: self.total_connections.load(Ordering::Relaxed),
        }
    }
}

impl Default for BrokerMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// 指标快照 (只读)
#[derive(Debug, Clone)]
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

impl std::fmt::Display for MetricsSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "uptime={}s requests={} responses={} errors={} produce={} fetch={} \
             msgs={} bytes_in={} bytes_out={} conns_active={} conns_total={}",
            self.uptime_secs,
            self.total_requests,
            self.total_responses,
            self.total_errors,
            self.produce_requests,
            self.fetch_requests,
            self.total_messages_produced,
            self.total_bytes_produced,
            self.total_bytes_fetched,
            self.active_connections,
            self.total_connections,
        )
    }
}

impl MetricsSnapshot {
    /// 输出 Prometheus text exposition format
    ///
    /// 格式参考: https://prometheus.io/docs/instrumenting/exposition_formats/
    pub fn to_prometheus(&self, broker_id: i32) -> String {
        let labels = format!("broker_id=\"{}\"", broker_id);
        let mut out = String::with_capacity(1024);

        // uptime
        out.push_str("# HELP rk_broker_uptime_seconds Broker uptime in seconds\n");
        out.push_str("# TYPE rk_broker_uptime_seconds gauge\n");
        out.push_str(&format!("rk_broker_uptime_seconds{{{}}} {}\n", labels, self.uptime_secs));

        // requests
        out.push_str("# HELP rk_broker_requests_total Total number of requests\n");
        out.push_str("# TYPE rk_broker_requests_total counter\n");
        out.push_str(&format!("rk_broker_requests_total{{{}}} {}\n", labels, self.total_requests));

        // responses
        out.push_str("# HELP rk_broker_responses_total Total number of responses\n");
        out.push_str("# TYPE rk_broker_responses_total counter\n");
        out.push_str(&format!("rk_broker_responses_total{{{}}} {}\n", labels, self.total_responses));

        // errors
        out.push_str("# HELP rk_broker_errors_total Total number of errors\n");
        out.push_str("# TYPE rk_broker_errors_total counter\n");
        out.push_str(&format!("rk_broker_errors_total{{{}}} {}\n", labels, self.total_errors));

        // produce requests
        out.push_str("# HELP rk_broker_produce_requests_total Total Produce requests\n");
        out.push_str("# TYPE rk_broker_produce_requests_total counter\n");
        out.push_str(&format!("rk_broker_produce_requests_total{{{}}} {}\n", labels, self.produce_requests));

        // fetch requests
        out.push_str("# HELP rk_broker_fetch_requests_total Total Fetch requests\n");
        out.push_str("# TYPE rk_broker_fetch_requests_total counter\n");
        out.push_str(&format!("rk_broker_fetch_requests_total{{{}}} {}\n", labels, self.fetch_requests));

        // messages produced
        out.push_str("# HELP rk_broker_messages_produced_total Total messages produced\n");
        out.push_str("# TYPE rk_broker_messages_produced_total counter\n");
        out.push_str(&format!("rk_broker_messages_produced_total{{{}}} {}\n", labels, self.total_messages_produced));

        // bytes produced
        out.push_str("# HELP rk_broker_bytes_produced_total Total bytes produced\n");
        out.push_str("# TYPE rk_broker_bytes_produced_total counter\n");
        out.push_str(&format!("rk_broker_bytes_produced_total{{{}}} {}\n", labels, self.total_bytes_produced));

        // bytes fetched
        out.push_str("# HELP rk_broker_bytes_fetched_total Total bytes fetched\n");
        out.push_str("# TYPE rk_broker_bytes_fetched_total counter\n");
        out.push_str(&format!("rk_broker_bytes_fetched_total{{{}}} {}\n", labels, self.total_bytes_fetched));

        // active connections
        out.push_str("# HELP rk_broker_active_connections Current active connections\n");
        out.push_str("# TYPE rk_broker_active_connections gauge\n");
        out.push_str(&format!("rk_broker_active_connections{{{}}} {}\n", labels, self.active_connections));

        // total connections
        out.push_str("# HELP rk_broker_total_connections_total Total connections accepted\n");
        out.push_str("# TYPE rk_broker_total_connections_total counter\n");
        out.push_str(&format!("rk_broker_total_connections_total{{{}}} {}\n", labels, self.total_connections));

        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_new() {
        let m = BrokerMetrics::new();
        let s = m.snapshot();
        assert_eq!(s.total_requests, 0);
        assert_eq!(s.total_responses, 0);
        assert_eq!(s.total_errors, 0);
        assert_eq!(s.active_connections, 0);
    }

    #[test]
    fn test_metrics_record_request() {
        let m = BrokerMetrics::new();
        m.record_request(0); // Produce
        m.record_request(0); // Produce
        m.record_request(1); // Fetch
        m.record_request(3); // Metadata
        let s = m.snapshot();
        assert_eq!(s.total_requests, 4);
        assert_eq!(s.produce_requests, 2);
        assert_eq!(s.fetch_requests, 1);
    }

    #[test]
    fn test_metrics_record_produce() {
        let m = BrokerMetrics::new();
        m.record_produce(10, 1024);
        m.record_produce(5, 512);
        let s = m.snapshot();
        assert_eq!(s.total_messages_produced, 15);
        assert_eq!(s.total_bytes_produced, 1536);
    }

    #[test]
    fn test_metrics_connections() {
        let m = BrokerMetrics::new();
        m.record_connection();
        m.record_connection();
        m.record_connection();
        m.record_disconnect();
        let s = m.snapshot();
        assert_eq!(s.active_connections, 2);
        assert_eq!(s.total_connections, 3);
    }

    #[test]
    fn test_metrics_snapshot_display() {
        let m = BrokerMetrics::new();
        m.record_request(0);
        m.record_response();
        let s = m.snapshot();
        let display = format!("{}", s);
        assert!(display.contains("requests=1"));
        assert!(display.contains("responses=1"));
    }

    #[test]
    fn test_metrics_prometheus_format() {
        let m = BrokerMetrics::new();
        m.record_request(0);
        m.record_request(0);
        m.record_request(1);
        m.record_response();
        m.record_produce(10, 2048);
        m.record_connection();
        let s = m.snapshot();
        let prom = s.to_prometheus(1);
        assert!(prom.contains("# HELP rk_broker_uptime_seconds"));
        assert!(prom.contains("# TYPE rk_broker_requests_total counter"));
        assert!(prom.contains("rk_broker_requests_total{broker_id=\"1\"} 3"));
        assert!(prom.contains("rk_broker_produce_requests_total{broker_id=\"1\"} 2"));
        assert!(prom.contains("rk_broker_fetch_requests_total{broker_id=\"1\"} 1"));
        assert!(prom.contains("rk_broker_messages_produced_total{broker_id=\"1\"} 10"));
        assert!(prom.contains("rk_broker_bytes_produced_total{broker_id=\"1\"} 2048"));
        assert!(prom.contains("rk_broker_active_connections{broker_id=\"1\"} 1"));
    }
}
