//! TCP Server
//!
//! 基于 Tokio 的 TCP 监听，支持连接管理和请求路由。
//! 支持纯 TCP 和 TLS 两种传输方式。
//! 支持优雅关闭 (Graceful Shutdown)。

use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio_rustls::TlsAcceptor;
use tracing::{error, info, warn};

use rk_broker::BrokerRouter;
use rk_core::BrokerConfig;

use crate::connection::{handle_connection, handle_tls_connection};
use crate::flow_control::FlowController;

/// 启动 TCP 监听
///
/// 使用 `socket2` 设置 SO_REUSEPORT，使内核将连接均匀分发到
/// 绑定同一端口的多个 socket (Thread-per-Core 模型)。
pub async fn start_listener(config: &BrokerConfig) -> rk_core::Result<TcpListener> {
    let addr: SocketAddr = format!("{}:{}", config.broker.host, config.broker.port)
        .parse()
        .map_err(|e| rk_core::RkError::Config(format!("Invalid listen address: {e}")))?;

    // 使用 socket2 设置 SO_REUSEPORT
    let socket = socket2::Socket::new(
        if addr.is_ipv4() {
            socket2::Domain::IPV4
        } else {
            socket2::Domain::IPV6
        },
        socket2::Type::STREAM,
        None,
    )
    .map_err(|e| rk_core::RkError::Io(e))?;

    // SO_REUSEADDR: 允许快速重启时重用地址
    socket
        .set_reuse_address(true)
        .map_err(|e| rk_core::RkError::Io(e))?;

    // SO_REUSEPORT: 将连接均匀分发到多个 socket
    #[cfg(unix)]
    socket
        .set_reuse_port(true)
        .map_err(|e| rk_core::RkError::Io(e))?;

    // 非阻塞模式 (Tokio 需要)
    socket
        .set_nonblocking(true)
        .map_err(|e| rk_core::RkError::Io(e))?;

    socket
        .bind(&socket2::SockAddr::from(addr))
        .map_err(|e| rk_core::RkError::Io(e))?;

    socket
        .listen(1024)
        .map_err(|e| rk_core::RkError::Io(e))?;

    let listener: std::net::TcpListener = socket.into();
    let tokio_listener = TcpListener::from_std(listener)?;

    info!("TCP listener bound on {} (SO_REUSEPORT enabled)", addr);
    Ok(tokio_listener)
}

/// 运行 Broker 服务
///
/// 接受 TCP 连接并为每个连接 spawn 一个异步任务。
/// 支持通过 shutdown_rx 信号优雅关闭。
pub async fn run_server(
    config: &BrokerConfig,
    router: Arc<BrokerRouter>,
) -> rk_core::Result<()> {
    let listener = start_listener(config).await?;
    let flow_controller = Arc::new(FlowController::new(config));

    info!("R-Kafka broker is ready");

    // 无关闭信号的兼容模式 (向后兼容)
    let (_tx, rx) = watch::channel(false);
    run_server_with_shutdown(listener, router, flow_controller, None, rx).await
}

/// 运行 Broker 服务 (带优雅关闭)
///
/// 收到关闭信号后停止接受新连接，等待现有连接处理完成。
/// 当 `tls_acceptor` 不为 None 时，所有连接先进行 TLS 握手再处理。
pub async fn run_server_with_shutdown(
    listener: TcpListener,
    router: Arc<BrokerRouter>,
    flow_controller: Arc<FlowController>,
    tls_acceptor: Option<TlsAcceptor>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> rk_core::Result<()> {
    loop {
        tokio::select! {
            accept_result = listener.accept() => {
                match accept_result {
                    Ok((stream, peer)) => {
                        // 流控检查
                        if let Err(e) = flow_controller.try_accept_connection() {
                            warn!(peer = %peer, error = %e, "Connection rejected by flow controller");
                            drop(stream);
                            continue;
                        }

                        let router = router.clone();
                        let flow_controller = flow_controller.clone();

                        if let Some(ref acceptor) = tls_acceptor {
                            // TLS 模式: 先进行 TLS 握手，再处理请求
                            let acceptor = acceptor.clone();
                            tokio::spawn(async move {
                                match acceptor.accept(stream).await {
                                    Ok(tls_stream) => {
                                        info!(peer = %peer, "TLS connection established");
                                        handle_tls_connection(tls_stream, peer, router).await;
                                    }
                                    Err(e) => {
                                        warn!(peer = %peer, error = %e, "TLS handshake failed");
                                    }
                                }
                                flow_controller.on_connection_close();
                            });
                        } else {
                            // 纯 TCP 模式
                            info!(peer = %peer, "Accepted new connection");
                            tokio::spawn(async move {
                                handle_connection(stream, router).await;
                                flow_controller.on_connection_close();
                            });
                        }
                    }
                    Err(e) => {
                        error!(error = %e, "Failed to accept connection");
                        // 短暂等待后重试，避免 accept 失败风暴
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    }
                }
            }
            _ = shutdown_rx.changed() => {
                info!("TCP server received shutdown signal, stopping accept loop");
                break;
            }
        }
    }

    info!("TCP server stopped accepting new connections");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_start_listener_invalid_address() {
        let config = BrokerConfig::from_toml(
            r#"[broker]
            host = "invalid_host_name_that_does_not_exist.example"
            port = 19999"#,
        ).unwrap();

        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(start_listener(&config));
        assert!(result.is_err());
    }
}
