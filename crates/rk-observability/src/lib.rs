//! rk-observability: R-Kafka 可观测性框架
//!
//! 提供统一的 Tracing / Metrics / Logging 基础设施。
//! - Tracing: 结构化日志 + OpenTelemetry 集成
//! - Metrics: Prometheus 指标导出

pub mod tracing_setup;
pub mod metrics;

// Re-exports
pub use tracing_setup::{
    TracingConfig, TracingLayer, TracingGuard,
    init_tracing, init_tracing_with_config,
};
pub use metrics::{PrometheusMetrics, SharedPrometheusMetrics, shared_metrics};
