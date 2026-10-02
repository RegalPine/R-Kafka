//! rk-observability: R-Kafka 可观测性框架
//!
//! 提供统一的 Tracing / Metrics / Logging 基础设施。
//! - Tracing: 结构化日志 + OpenTelemetry 集成
//! - Metrics: Prometheus 指标导出

pub mod metrics;
pub mod tracing_setup;

// Re-exports
pub use metrics::{shared_metrics, PrometheusMetrics, SharedPrometheusMetrics};
pub use tracing_setup::{
    init_tracing, init_tracing_with_config, TracingConfig, TracingGuard, TracingLayer,
};
