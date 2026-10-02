//! Prometheus Metrics — Broker 指标导出
//!
//! 基于 `prometheus` crate 提供 Kafka Broker 标准指标:
//!
//! - **Counter**: 累计递增指标 (请求数、字节数、错误数)
//! - **Gauge**: 可增可减指标 (活跃连接数、磁盘使用量)
//! - **Histogram**: 分布统计 (延迟分位数)
//!
//! 通过 HTTP `/metrics` 端点暴露 Prometheus text format。

use prometheus::{
    Encoder, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge,
    IntGaugeVec, Opts, Registry, TextEncoder,
};
use std::sync::Arc;

/// Broker Prometheus 指标集合
///
/// 所有指标注册在独立的 Registry 中，通过 `encode()` 输出 Prometheus text format。
pub struct PrometheusMetrics {
    registry: Registry,

    // ─── Counters ────────────────────────────────────────────────
    /// 入站消息总数
    pub messages_in_total: IntCounter,
    /// 入站字节总数
    pub bytes_in_total: IntCounter,
    /// 出站字节总数
    pub bytes_out_total: IntCounter,
    /// 请求总数 (按 API Key 分类)
    pub requests_total: IntCounterVec,
    /// 错误总数 (按 API Key 分类)
    pub errors_total: IntCounterVec,
    /// Produce 消息总数
    pub produce_messages_total: IntCounter,

    // ─── Gauges ──────────────────────────────────────────────────
    /// 活跃连接数
    pub active_connections: IntGauge,
    /// 磁盘使用字节数
    pub disk_usage_bytes: IntGauge,
    /// 副本复制延迟 (按 Partition)
    pub replication_lag: IntGaugeVec,
    /// 分区数
    pub partition_count: IntGauge,
    /// Topic 数
    pub topic_count: IntGauge,
    /// Under-replicated 分区数
    pub under_replicated_partitions: IntGauge,

    // ─── Histograms ──────────────────────────────────────────────
    /// Append 延迟 (秒)
    pub append_latency_seconds: Histogram,
    /// Fetch 延迟 (秒)
    pub fetch_latency_seconds: Histogram,
    /// 请求处理延迟 (按 API Key 分类)
    pub request_latency_seconds: HistogramVec,
}

impl PrometheusMetrics {
    /// 创建并注册全部指标
    pub fn new() -> Self {
        let registry = Registry::new();

        // Counters
        let messages_in_total = IntCounter::with_opts(
            Opts::new("rk_messages_in_total", "Total number of messages produced"),
        )
        .expect("metric creation failed");

        let bytes_in_total = IntCounter::with_opts(
            Opts::new("rk_bytes_in_total", "Total bytes received"),
        )
        .expect("metric creation failed");

        let bytes_out_total = IntCounter::with_opts(
            Opts::new("rk_bytes_out_total", "Total bytes sent to consumers"),
        )
        .expect("metric creation failed");

        let requests_total = IntCounterVec::new(
            Opts::new("rk_requests_total", "Total requests by API key"),
            &["api_key"],
        )
        .expect("metric creation failed");

        let errors_total = IntCounterVec::new(
            Opts::new("rk_errors_total", "Total errors by API key"),
            &["api_key"],
        )
        .expect("metric creation failed");

        let produce_messages_total = IntCounter::with_opts(
            Opts::new("rk_produce_messages_total", "Total messages in produce requests"),
        )
        .expect("metric creation failed");

        // Gauges
        let active_connections = IntGauge::with_opts(
            Opts::new("rk_active_connections", "Number of active client connections"),
        )
        .expect("metric creation failed");

        let disk_usage_bytes = IntGauge::with_opts(
            Opts::new("rk_disk_usage_bytes", "Total disk usage in bytes"),
        )
        .expect("metric creation failed");

        let replication_lag = IntGaugeVec::new(
            Opts::new("rk_replication_lag", "Replication lag per partition"),
            &["topic", "partition"],
        )
        .expect("metric creation failed");

        let partition_count = IntGauge::with_opts(
            Opts::new("rk_partition_count", "Total number of partitions"),
        )
        .expect("metric creation failed");

        let topic_count = IntGauge::with_opts(
            Opts::new("rk_topic_count", "Total number of topics"),
        )
        .expect("metric creation failed");

        let under_replicated_partitions = IntGauge::with_opts(
            Opts::new(
                "rk_under_replicated_partitions",
                "Number of under-replicated partitions",
            ),
        )
        .expect("metric creation failed");

        // Histograms
        let append_latency_seconds = Histogram::with_opts(
            HistogramOpts::new(
                "rk_append_latency_seconds",
                "Log append latency in seconds",
            )
            .buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]),
        )
        .expect("metric creation failed");

        let fetch_latency_seconds = Histogram::with_opts(
            HistogramOpts::new(
                "rk_fetch_latency_seconds",
                "Fetch request latency in seconds",
            )
            .buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]),
        )
        .expect("metric creation failed");

        let request_latency_seconds = HistogramVec::new(
            HistogramOpts::new(
                "rk_request_latency_seconds",
                "Request handling latency by API key",
            )
            .buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0]),
            &["api_key"],
        )
        .expect("metric creation failed");

        // 注册全部指标
        registry.register(Box::new(messages_in_total.clone())).unwrap();
        registry.register(Box::new(bytes_in_total.clone())).unwrap();
        registry.register(Box::new(bytes_out_total.clone())).unwrap();
        registry.register(Box::new(requests_total.clone())).unwrap();
        registry.register(Box::new(errors_total.clone())).unwrap();
        registry.register(Box::new(produce_messages_total.clone())).unwrap();
        registry.register(Box::new(active_connections.clone())).unwrap();
        registry.register(Box::new(disk_usage_bytes.clone())).unwrap();
        registry.register(Box::new(replication_lag.clone())).unwrap();
        registry.register(Box::new(partition_count.clone())).unwrap();
        registry.register(Box::new(topic_count.clone())).unwrap();
        registry.register(Box::new(under_replicated_partitions.clone())).unwrap();
        registry.register(Box::new(append_latency_seconds.clone())).unwrap();
        registry.register(Box::new(fetch_latency_seconds.clone())).unwrap();
        registry.register(Box::new(request_latency_seconds.clone())).unwrap();

        Self {
            registry,
            messages_in_total,
            bytes_in_total,
            bytes_out_total,
            requests_total,
            errors_total,
            produce_messages_total,
            active_connections,
            disk_usage_bytes,
            replication_lag,
            partition_count,
            topic_count,
            under_replicated_partitions,
            append_latency_seconds,
            fetch_latency_seconds,
            request_latency_seconds,
        }
    }

    /// 编码为 Prometheus text format
    pub fn encode(&self) -> String {
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer).unwrap();
        String::from_utf8(buffer).unwrap()
    }

    /// 记录请求 (按 API Key)
    pub fn record_request(&self, api_key: i16) {
        self.requests_total
            .with_label_values(&[&api_key.to_string()])
            .inc();
    }

    /// 记录错误 (按 API Key)
    pub fn record_error(&self, api_key: i16) {
        self.errors_total
            .with_label_values(&[&api_key.to_string()])
            .inc();
    }

    /// 记录 Produce
    pub fn record_produce(&self, message_count: u64, byte_count: u64) {
        self.messages_in_total.inc_by(message_count);
        self.bytes_in_total.inc_by(byte_count);
        self.produce_messages_total.inc_by(message_count);
    }

    /// 记录 Fetch
    pub fn record_fetch(&self, byte_count: u64) {
        self.bytes_out_total.inc_by(byte_count);
    }
}

impl Default for PrometheusMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// 共享 Prometheus 指标 (线程安全)
pub type SharedPrometheusMetrics = Arc<PrometheusMetrics>;

/// 创建共享 Prometheus 指标
pub fn shared_metrics() -> SharedPrometheusMetrics {
    Arc::new(PrometheusMetrics::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_creation() {
        let metrics = PrometheusMetrics::new();
        metrics.messages_in_total.inc();
        metrics.bytes_in_total.inc_by(1024);
        metrics.active_connections.set(5);
        metrics.append_latency_seconds.observe(0.005);

        let output = metrics.encode();
        assert!(output.contains("rk_messages_in_total"));
        assert!(output.contains("rk_bytes_in_total"));
        assert!(output.contains("rk_active_connections"));
        assert!(output.contains("rk_append_latency_seconds"));
    }

    #[test]
    fn test_record_request() {
        let metrics = PrometheusMetrics::new();
        metrics.record_request(0); // Produce
        metrics.record_request(0); // Produce
        metrics.record_request(1); // Fetch

        let output = metrics.encode();
        assert!(output.contains("rk_requests_total"));
    }

    #[test]
    fn test_record_produce() {
        let metrics = PrometheusMetrics::new();
        metrics.record_produce(10, 1024);

        let output = metrics.encode();
        assert!(output.contains("rk_messages_in_total 10"));
        assert!(output.contains("rk_bytes_in_total 1024"));
    }

    #[test]
    fn test_record_fetch() {
        let metrics = PrometheusMetrics::new();
        metrics.record_fetch(2048);

        let output = metrics.encode();
        assert!(output.contains("rk_bytes_out_total 2048"));
    }

    #[test]
    fn test_shared_metrics() {
        let m1 = shared_metrics();
        let m2 = m1.clone();
        m1.messages_in_total.inc();
        assert_eq!(m2.messages_in_total.get(), 1);
    }
}
