//! HTTP Metrics Server
//!
//! 轻量级 HTTP 监控端点。
//! 使用原生 Tokio TCP 实现，无外部 HTTP 框架依赖。
//! 支持:
//! - GET /metrics — JSON 格式 Broker 指标
//! - GET /metrics/prometheus — Prometheus text exposition format
//! - GET /health — 健康检查
//! - GET /ready — 就绪检查

use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tracing::{debug, info, warn};

use rk_broker::{BrokerMetrics, MetricsSnapshot};
use crate::flow_control::FlowController;

/// HTTP 监控服务器状态
pub struct HttpMetricsServer {
    /// 共享的 Broker 指标
    metrics: Arc<BrokerMetrics>,
    /// 监听端口
    port: u16,
    /// 监听地址
    host: String,
    /// Broker ID (用于 Prometheus labels)
    broker_id: i32,
    /// 流控制器 (可选，用于 /status 端点)
    flow_controller: Option<Arc<FlowController>>,
}

impl HttpMetricsServer {
    /// 创建 HTTP 监控服务器
    pub fn new(host: String, port: u16, metrics: Arc<BrokerMetrics>, broker_id: i32) -> Self {
        Self { metrics, port, host, broker_id, flow_controller: None }
    }

    /// 设置流控制器 (用于 /status 端点)
    pub fn with_flow_controller(mut self, flow_controller: Arc<FlowController>) -> Self {
        self.flow_controller = Some(flow_controller);
        self
    }

    /// 运行 HTTP 服务器
    ///
    /// 接受 HTTP 连接并处理请求，直到收到关闭信号。
    pub async fn run(
        &self,
        mut shutdown_rx: watch::Receiver<bool>,
    ) -> rk_core::Result<()> {
        let addr = format!("{}:{}", self.host, self.port);
        let listener = TcpListener::bind(&addr).await.map_err(|e| {
            rk_core::RkError::Config(format!("HTTP metrics server bind failed: {e}"))
        })?;
        info!(addr = %addr, "HTTP metrics server listening");

        loop {
            tokio::select! {
                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((stream, _peer)) => {
                            let metrics = self.metrics.clone();
                            let broker_id = self.broker_id;
                            let flow_controller = self.flow_controller.clone();
                            tokio::spawn(async move {
                                if let Err(e) = handle_http_request(stream, metrics, broker_id, flow_controller).await {
                                    debug!(error = %e, "HTTP request handling error");
                                }
                            });
                        }
                        Err(e) => {
                            warn!(error = %e, "HTTP accept failed");
                        }
                    }
                }
                _ = shutdown_rx.changed() => {
                    info!("HTTP metrics server shutting down");
                    break;
                }
            }
        }

        Ok(())
    }
}

/// 处理单个 HTTP 请求
async fn handle_http_request(
    mut stream: tokio::net::TcpStream,
    metrics: Arc<BrokerMetrics>,
    broker_id: i32,
    flow_controller: Option<Arc<FlowController>>,
) -> std::io::Result<()> {
    let mut buf = vec![0u8; 4096];
    let n = stream.read(&mut buf).await?;
    if n == 0 {
        return Ok(());
    }

    let request = String::from_utf8_lossy(&buf[..n]);
    let first_line = request.lines().next().unwrap_or("");

    // 解析 HTTP 方法和路径
    let parts: Vec<&str> = first_line.split_whitespace().collect();
    if parts.len() < 2 {
        send_response(&mut stream, 400, "Bad Request", "text/plain", r#"{"error":"bad request"}"#).await?;
        return Ok(());
    }

    let method = parts[0];
    let path = parts[1];

    if method != "GET" {
        send_response(&mut stream, 405, "Method Not Allowed", "text/plain", r#"{"error":"method not allowed"}"#).await?;
        return Ok(());
    }

    match path {
        "/metrics" => {
            let snapshot = metrics.snapshot();
            let json = metrics_to_json(&snapshot);
            send_response(&mut stream, 200, "OK", "application/json", &json).await?;
        }
        "/metrics/prometheus" => {
            let snapshot = metrics.snapshot();
            let prom = snapshot.to_prometheus(broker_id);
            send_response(&mut stream, 200, "OK", "text/plain; version=0.0.4; charset=utf-8", &prom).await?;
        }
        "/health" => {
            let json = r#"{"status":"ok","service":"r-kafka"}"#;
            send_response(&mut stream, 200, "OK", "application/json", json).await?;
        }
        "/ready" => {
            let json = r#"{"ready":true,"service":"r-kafka"}"#;
            send_response(&mut stream, 200, "OK", "application/json", json).await?;
        }
        "/status" => {
            let snapshot = metrics.snapshot();
            let fc_json = if let Some(ref fc) = flow_controller {
                let fc_snap = fc.snapshot();
                format!(
                    r#","flow_control":{{"current_connections":{},"max_connections":{},"pending_bytes":{},"max_pending_bytes":{}}}"#,
                    fc_snap.current_connections,
                    fc_snap.max_connections,
                    fc_snap.current_pending_bytes,
                    fc_snap.max_pending_bytes,
                )
            } else {
                String::new()
            };
            let json = format!(
                r#"{{"status":"ok","service":"r-kafka","broker_id":{},"uptime_secs":{},"total_requests":{},"total_errors":{}{}}}"#,
                broker_id, snapshot.uptime_secs, snapshot.total_requests, snapshot.total_errors, fc_json,
            );
            send_response(&mut stream, 200, "OK", "application/json", &json).await?;
        }
        _ => {
            send_response(&mut stream, 404, "Not Found", "text/plain", r#"{"error":"not found"}"#).await?;
        }
    }

    Ok(())
}

/// 发送 HTTP 响应
async fn send_response(
    stream: &mut tokio::net::TcpStream,
    status: u16,
    status_text: &str,
    content_type: &str,
    body: &str,
) -> std::io::Result<()> {
    let response = format!(
        "HTTP/1.1 {} {}\r\n\
         Content-Type: {}\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n\
         {}",
        status,
        status_text,
        content_type,
        body.len(),
        body,
    );
    stream.write_all(response.as_bytes()).await?;
    stream.flush().await?;
    Ok(())
}

/// 将 MetricsSnapshot 转为 JSON 字符串
fn metrics_to_json(snapshot: &MetricsSnapshot) -> String {
    serde_json::json!({
        "uptime_secs": snapshot.uptime_secs,
        "total_requests": snapshot.total_requests,
        "total_responses": snapshot.total_responses,
        "total_errors": snapshot.total_errors,
        "produce_requests": snapshot.produce_requests,
        "fetch_requests": snapshot.fetch_requests,
        "total_messages_produced": snapshot.total_messages_produced,
        "total_bytes_produced": snapshot.total_bytes_produced,
        "total_bytes_fetched": snapshot.total_bytes_fetched,
        "active_connections": snapshot.active_connections,
        "total_connections": snapshot.total_connections,
    })
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rk_core::BrokerConfig;

    #[test]
    fn test_metrics_to_json() {
        let snapshot = MetricsSnapshot {
            uptime_secs: 100,
            total_requests: 50,
            total_responses: 48,
            total_errors: 2,
            produce_requests: 20,
            fetch_requests: 15,
            total_messages_produced: 1000,
            total_bytes_produced: 50000,
            total_bytes_fetched: 30000,
            active_connections: 5,
            total_connections: 25,
        };
        let json = metrics_to_json(&snapshot);
        assert!(json.contains("\"uptime_secs\":100"));
        assert!(json.contains("\"total_requests\":50"));
        assert!(json.contains("\"active_connections\":5"));
    }

    #[tokio::test]
    async fn test_handle_health_request() {
        let metrics = Arc::new(BrokerMetrics::new());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let metrics_clone = metrics.clone();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handle_http_request(stream, metrics_clone, 0, None).await.unwrap();
        });

        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream.write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n").await.unwrap();

        let mut buf = vec![0u8; 4096];
        let n = stream.read(&mut buf).await.unwrap();
        let response = String::from_utf8_lossy(&buf[..n]);
        assert!(response.contains("200 OK"));
        assert!(response.contains("\"status\":\"ok\""));
    }

    #[tokio::test]
    async fn test_handle_metrics_request() {
        let metrics = Arc::new(BrokerMetrics::new());
        metrics.record_request(0);
        metrics.record_response();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let metrics_clone = metrics.clone();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handle_http_request(stream, metrics_clone, 0, None).await.unwrap();
        });

        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream.write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n").await.unwrap();

        let mut buf = vec![0u8; 4096];
        let n = stream.read(&mut buf).await.unwrap();
        let response = String::from_utf8_lossy(&buf[..n]);
        assert!(response.contains("200 OK"));
        assert!(response.contains("total_requests"));
    }

    #[tokio::test]
    async fn test_handle_prometheus_metrics() {
        let metrics = Arc::new(BrokerMetrics::new());
        metrics.record_request(0);
        metrics.record_produce(5, 1024);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let metrics_clone = metrics.clone();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handle_http_request(stream, metrics_clone, 1, None).await.unwrap();
        });

        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream.write_all(b"GET /metrics/prometheus HTTP/1.1\r\nHost: localhost\r\n\r\n").await.unwrap();

        let mut buf = vec![0u8; 4096];
        let n = stream.read(&mut buf).await.unwrap();
        let response = String::from_utf8_lossy(&buf[..n]);
        assert!(response.contains("200 OK"));
        assert!(response.contains("text/plain; version=0.0.4"));
        assert!(response.contains("rk_broker_requests_total"));
        assert!(response.contains("rk_broker_messages_produced_total"));
    }

    #[tokio::test]
    async fn test_handle_404() {
        let metrics = Arc::new(BrokerMetrics::new());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let metrics_clone = metrics.clone();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handle_http_request(stream, metrics_clone, 0, None).await.unwrap();
        });

        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream.write_all(b"GET /unknown HTTP/1.1\r\nHost: localhost\r\n\r\n").await.unwrap();

        let mut buf = vec![0u8; 4096];
        let n = stream.read(&mut buf).await.unwrap();
        let response = String::from_utf8_lossy(&buf[..n]);
        assert!(response.contains("404"));
    }

    #[tokio::test]
    async fn test_handle_status_request() {
        let metrics = Arc::new(BrokerMetrics::new());
        metrics.record_request(0);
        let fc = Arc::new(FlowController::new(
            &BrokerConfig::from_toml("").unwrap(),
        ));
        fc.try_accept_connection().unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let metrics_clone = metrics.clone();
        let fc_clone = fc.clone();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handle_http_request(stream, metrics_clone, 5, Some(fc_clone)).await.unwrap();
        });

        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream.write_all(b"GET /status HTTP/1.1\r\nHost: localhost\r\n\r\n").await.unwrap();

        let mut buf = vec![0u8; 4096];
        let n = stream.read(&mut buf).await.unwrap();
        let response = String::from_utf8_lossy(&buf[..n]);
        assert!(response.contains("200 OK"));
        assert!(response.contains(r#""broker_id":5"#));
        assert!(response.contains(r#""current_connections":1"#));
        assert!(response.contains("flow_control"));
    }
}
