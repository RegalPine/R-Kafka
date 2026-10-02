//! rk-network: R-Kafka 网络层
//!
//! TCP 监听、连接管理、帧解码、SASL 状态机、流控、HTTP 监控。
//! Phase 1 使用 Tokio TCP，Phase 2 引入 io_uring 优化。

pub mod server;
pub mod connection;
pub mod flow_control;
pub mod http_server;
pub mod zero_copy;

pub use connection::{Connection, ConnectionState, handle_connection};
pub use server::{start_listener, run_server, run_server_with_shutdown};
pub use flow_control::FlowController;
pub use flow_control::FlowControlSnapshot;
pub use http_server::HttpMetricsServer;
pub use zero_copy::{
    zero_copy_send, zero_copy_send_sync, ZeroCopyStats,
    is_zero_copy_supported, zero_copy_implementation,
};
