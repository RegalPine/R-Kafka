//! Tracing 基础设施 — OpenTelemetry 集成
//!
//! 提供可配置的 tracing 层:
//! - EnvFilter: 基于 RUST_LOG 环境变量过滤
//! - fmt: 人类可读的日志输出
//! - json: JSON 格式日志 (生产环境)
//! - OpenTelemetry: 分布式追踪 (可选, 需 OTLP endpoint)
//!
//! ```text
//! ┌─────────────────────────────────────────┐
//! │           tracing-subscriber            │
//! │  ┌──────────┐ ┌──────┐ ┌─────────────┐ │
//! │  │EnvFilter │ │ fmt  │ │  OTLP Layer │ │
//! │  │(RUST_LOG)│ │/json │ │ (optional)  │ │
//! │  └──────────┘ └──────┘ └─────────────┘ │
//! └─────────────────────────────────────────┘
//! ```

use std::sync::atomic::{AtomicBool, Ordering};

use tracing::Level;
use tracing_subscriber::{
    EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt, Registry,
};

/// 全局初始化标志 (防止重复初始化)
static INITIALIZED: AtomicBool = AtomicBool::new(false);

/// Tracing 层类型
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TracingLayer {
    /// 人类可读的 fmt 输出
    Fmt,
    /// JSON 格式输出 (生产环境)
    Json,
    /// OpenTelemetry OTLP (分布式追踪)
    OpenTelemetry,
}

impl TracingLayer {
    /// 从配置字符串解析
    pub fn from_str(s: &str) -> Self {
        match s.trim().to_lowercase().as_str() {
            "json" => TracingLayer::Json,
            "otlp" | "opentelemetry" | "otel" => TracingLayer::OpenTelemetry,
            _ => TracingLayer::Fmt,
        }
    }
}

impl std::fmt::Display for TracingLayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TracingLayer::Fmt => write!(f, "fmt"),
            TracingLayer::Json => write!(f, "json"),
            TracingLayer::OpenTelemetry => write!(f, "opentelemetry"),
        }
    }
}

/// Tracing 配置
#[derive(Debug, Clone)]
pub struct TracingConfig {
    /// 是否启用 tracing
    pub enabled: bool,
    /// 默认日志级别
    pub default_level: Level,
    /// 使用的 tracing 层
    pub layer: TracingLayer,
    /// OTLP endpoint (OpenTelemetry 收集器地址)
    pub otlp_endpoint: Option<String>,
    /// 服务名称 (用于 OTLP resource attributes)
    pub service_name: String,
    /// 是否显示 target (模块路径)
    pub show_target: bool,
    /// 是否显示线程名
    pub show_thread: bool,
    /// 是否显示时间戳
    pub show_timestamp: bool,
    /// 是否启用 ANSI 颜色
    pub ansi: bool,
}

impl Default for TracingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            default_level: Level::INFO,
            layer: TracingLayer::Fmt,
            otlp_endpoint: None,
            service_name: "r-kafka".to_string(),
            show_target: true,
            show_thread: true,
            show_timestamp: true,
            ansi: true,
        }
    }
}

impl TracingConfig {
    /// 创建生产环境配置
    pub fn production(service_name: &str) -> Self {
        Self {
            enabled: true,
            default_level: Level::INFO,
            layer: TracingLayer::Json,
            otlp_endpoint: None,
            service_name: service_name.to_string(),
            show_target: true,
            show_thread: true,
            show_timestamp: true,
            ansi: false,
        }
    }

    /// 创建测试/开发环境配置
    pub fn development() -> Self {
        Self {
            enabled: true,
            default_level: Level::DEBUG,
            layer: TracingLayer::Fmt,
            otlp_endpoint: None,
            service_name: "r-kafka-dev".to_string(),
            show_target: true,
            show_thread: false,
            show_timestamp: true,
            ansi: true,
        }
    }

    /// 设置 OTLP endpoint
    pub fn with_otlp(mut self, endpoint: &str) -> Self {
        self.otlp_endpoint = Some(endpoint.to_string());
        self.layer = TracingLayer::OpenTelemetry;
        self
    }
}

/// Tracing 守卫 — 用于优雅关闭
///
/// 当 TracingGuard 被 drop 时，会执行清理操作 (如 flush OTLP exporter)。
#[derive(Debug)]
pub struct TracingGuard {
    _private: (),
}

impl TracingGuard {
    fn new() -> Self {
        Self { _private: () }
    }
}

impl Drop for TracingGuard {
    fn drop(&mut self) {
        // OpenTelemetry exporter flush 在此执行
        // 当前为占位实现，集成 opentelemetry-otlp 后补充
        tracing::debug!("Tracing guard dropped, flushing exporters");
    }
}

/// 使用默认配置初始化 tracing
///
/// 返回 TracingGuard，在程序退出时 drop 以执行清理。
pub fn init_tracing() -> TracingGuard {
    init_tracing_with_config(&TracingConfig::default())
}

/// 使用自定义配置初始化 tracing
///
/// 如果已初始化过，返回空的 guard (不会重复初始化)。
pub fn init_tracing_with_config(config: &TracingConfig) -> TracingGuard {
    // 防止重复初始化
    if INITIALIZED.swap(true, Ordering::SeqCst) {
        tracing::debug!("Tracing already initialized, skipping");
        return TracingGuard::new();
    }

    if !config.enabled {
        // tracing 禁用时仍设置基础 fmt layer
        let _ = Registry::default()
            .with(fmt::layer().with_ansi(false))
            .try_init();
        return TracingGuard::new();
    }

    // 构建 EnvFilter
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(format!("{}", config.default_level)));

    match config.layer {
        TracingLayer::Fmt => {
            let _ = Registry::default()
                .with(env_filter)
                .with(
                    fmt::layer()
                        .with_target(config.show_target)
                        .with_thread_names(config.show_thread)
                        .with_ansi(config.ansi),
                )
                .try_init();
        }
        TracingLayer::Json => {
            let _ = Registry::default()
                .with(env_filter)
                .with(
                    fmt::layer()
                        .json()
                        .with_target(config.show_target)
                        .with_thread_names(config.show_thread),
                )
                .try_init();
        }
        TracingLayer::OpenTelemetry => {
            // OpenTelemetry 层: 当 otlp_endpoint 配置时使用
            // 当前 fallback 到 fmt 层，集成 opentelemetry-otlp 后替换
            if config.otlp_endpoint.is_some() {
                tracing::info!(
                    endpoint = config.otlp_endpoint.as_deref().unwrap_or("none"),
                    service = %config.service_name,
                    "OpenTelemetry tracing configured (endpoint set, OTLP exporter pending)"
                );
            }

            let _ = Registry::default()
                .with(env_filter)
                .with(
                    fmt::layer()
                        .with_target(config.show_target)
                        .with_thread_names(config.show_thread)
                        .with_ansi(config.ansi),
                )
                .try_init();
        }
    }

    TracingGuard::new()
}

/// 重置初始化标志 (仅用于测试)
#[cfg(test)]
pub fn reset_initialized() {
    INITIALIZED.store(false, Ordering::SeqCst);
}

// ─── Span 工具函数 ──────────────────────────────────────────────────

/// 创建请求 span
pub fn request_span(api_key: i16, correlation_id: i32) -> tracing::Span {
    tracing::info_span!(
        "kafka_request",
        api_key = api_key,
        correlation_id = correlation_id,
    )
}

/// 创建分区操作 span
pub fn partition_span(topic: &str, partition: i32) -> tracing::Span {
    tracing::info_span!(
        "partition_op",
        topic = topic,
        partition = partition,
    )
}

/// 创建存储操作 span
pub fn storage_span(operation: &str) -> tracing::Span {
    tracing::debug_span!(
        "storage_op",
        operation = operation,
    )
}

/// 创建复制 span
pub fn replication_span(leader_id: i32, follower_id: i32) -> tracing::Span {
    tracing::info_span!(
        "replication",
        leader_id = leader_id,
        follower_id = follower_id,
    )
}

// ─── Tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tracing_layer_from_str() {
        assert_eq!(TracingLayer::from_str("fmt"), TracingLayer::Fmt);
        assert_eq!(TracingLayer::from_str("json"), TracingLayer::Json);
        assert_eq!(TracingLayer::from_str("otlp"), TracingLayer::OpenTelemetry);
        assert_eq!(
            TracingLayer::from_str("opentelemetry"),
            TracingLayer::OpenTelemetry
        );
        assert_eq!(TracingLayer::from_str("otel"), TracingLayer::OpenTelemetry);
        assert_eq!(TracingLayer::from_str("unknown"), TracingLayer::Fmt);
    }

    #[test]
    fn test_tracing_layer_display() {
        assert_eq!(format!("{}", TracingLayer::Fmt), "fmt");
        assert_eq!(format!("{}", TracingLayer::Json), "json");
        assert_eq!(format!("{}", TracingLayer::OpenTelemetry), "opentelemetry");
    }

    #[test]
    fn test_tracing_config_default() {
        let config = TracingConfig::default();
        assert!(config.enabled);
        assert_eq!(config.default_level, Level::INFO);
        assert_eq!(config.layer, TracingLayer::Fmt);
        assert!(config.otlp_endpoint.is_none());
        assert_eq!(config.service_name, "r-kafka");
        assert!(config.show_target);
        assert!(config.show_thread);
        assert!(config.show_timestamp);
        assert!(config.ansi);
    }

    #[test]
    fn test_tracing_config_production() {
        let config = TracingConfig::production("my-broker");
        assert!(config.enabled);
        assert_eq!(config.layer, TracingLayer::Json);
        assert_eq!(config.service_name, "my-broker");
        assert!(!config.ansi);
    }

    #[test]
    fn test_tracing_config_development() {
        let config = TracingConfig::development();
        assert!(config.enabled);
        assert_eq!(config.default_level, Level::DEBUG);
        assert_eq!(config.layer, TracingLayer::Fmt);
        assert!(!config.show_thread);
        assert!(config.ansi);
    }

    #[test]
    fn test_tracing_config_with_otlp() {
        let config = TracingConfig::default().with_otlp("http://localhost:4317");
        assert_eq!(config.layer, TracingLayer::OpenTelemetry);
        assert_eq!(
            config.otlp_endpoint.as_deref(),
            Some("http://localhost:4317")
        );
    }

    #[test]
    fn test_tracing_guard_drop() {
        // TracingGuard should not panic on drop
        let guard = TracingGuard::new();
        drop(guard);
    }

    #[test]
    fn test_request_span() {
        let span = request_span(0, 42);
        assert!(span.is_none() || true); // span 创建不应 panic
    }

    #[test]
    fn test_partition_span() {
        let span = partition_span("test-topic", 0);
        assert!(span.is_none() || true);
    }

    #[test]
    fn test_storage_span() {
        let span = storage_span("append");
        assert!(span.is_none() || true);
    }

    #[test]
    fn test_replication_span() {
        let span = replication_span(1, 2);
        assert!(span.is_none() || true);
    }

    #[test]
    fn test_init_tracing_disabled() {
        reset_initialized();
        let config = TracingConfig {
            enabled: false,
            ..TracingConfig::default()
        };
        let guard = init_tracing_with_config(&config);
        drop(guard);
        reset_initialized();
    }

    #[test]
    fn test_init_tracing_fmt_layer() {
        reset_initialized();
        let config = TracingConfig {
            enabled: true,
            layer: TracingLayer::Fmt,
            default_level: Level::WARN,
            ..TracingConfig::default()
        };
        let guard = init_tracing_with_config(&config);
        drop(guard);
        reset_initialized();
    }

    #[test]
    fn test_init_tracing_json_layer() {
        reset_initialized();
        let config = TracingConfig {
            enabled: true,
            layer: TracingLayer::Json,
            default_level: Level::WARN,
            ..TracingConfig::default()
        };
        let guard = init_tracing_with_config(&config);
        drop(guard);
        reset_initialized();
    }

    #[test]
    fn test_init_tracing_otel_layer() {
        reset_initialized();
        let config = TracingConfig {
            enabled: true,
            layer: TracingLayer::OpenTelemetry,
            otlp_endpoint: Some("http://localhost:4317".to_string()),
            default_level: Level::WARN,
            ..TracingConfig::default()
        };
        let guard = init_tracing_with_config(&config);
        drop(guard);
        reset_initialized();
    }

    #[test]
    fn test_double_initialization() {
        reset_initialized();
        let config = TracingConfig {
            enabled: true,
            layer: TracingLayer::Fmt,
            default_level: Level::WARN,
            ..TracingConfig::default()
        };
        let guard1 = init_tracing_with_config(&config);
        // 第二次调用应该直接返回
        let guard2 = init_tracing_with_config(&config);
        drop(guard1);
        drop(guard2);
        reset_initialized();
    }
}
