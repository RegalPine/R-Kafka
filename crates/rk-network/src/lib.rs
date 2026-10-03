//! rk-network: R-Kafka 网络层
//!
//! TCP 监听、连接管理、帧解码、SASL 状态机、流控、HTTP 监控。
//! 通过 RequestHandler trait 与业务层（rk-broker）解耦。
//! Phase 1 使用 Tokio TCP，Phase 2 引入 io_uring 优化。

pub mod connection;
pub mod flow_control;
pub mod handler;
pub mod http_server;
pub mod server;
pub mod session;
pub mod zero_copy;

pub use connection::{handle_connection, Connection, ConnectionState};
pub use flow_control::FlowControlSnapshot;
pub use flow_control::FlowController;
pub use handler::{extract_api_key, is_pre_auth_api, RequestHandler};
pub use http_server::{HttpMetricsServer, MetricsProvider, MetricsSnapshot, PrometheusMetrics};
pub use server::{run_server, run_server_with_shutdown, start_listener};
pub use session::{AuthState, ConnectionSession};
pub use zero_copy::{
    is_zero_copy_supported, zero_copy_implementation, zero_copy_send, zero_copy_send_sync,
    ZeroCopyStats,
};
