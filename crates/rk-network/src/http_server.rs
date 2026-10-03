//! HTTP Metrics Server
//!
//! 轻量级 HTTP 监控端点。
//! 使用原生 Tokio TCP 实现，无外部 HTTP 框架依赖。
//! 通过 MetricsProvider trait 与 rk-broker 解耦。
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

use crate::flow_control::FlowController;

// 从 rk-core 重新导出，方便上层使用
pub use rk_core::{MetricsProvider, MetricsSnapshot};

/// 将 MetricsSnapshot 输出为 Prometheus text exposition format
pub trait PrometheusMetrics {
    fn to_prometheus(&self, broker_id: i32) -> String;
}

impl PrometheusMetrics for MetricsSnapshot {
    fn to_prometheus(&self, broker_id: i32) -> String {
        let labels = format!("broker_id=\"{}\"", broker_id);
        let mut out = String::with_capacity(1024);

        macro_rules! metric {
            ($help:expr, $type:expr, $name:expr, $val:expr) => {
                out.push_str(&format!("# HELP {} {}\n", $name, $help));
                out.push_str(&format!("# TYPE {} {}\n", $name, $type));
                out.push_str(&format!("{}{{{}}} {}\n", $name, labels, $val));
            };
        }

        metric!(
            "Broker uptime in seconds",
            "gauge",
            "rk_broker_uptime_seconds",
            self.uptime_secs
        );
        metric!(
            "Total number of requests",
            "counter",
            "rk_broker_requests_total",
            self.total_requests
        );
        metric!(
            "Total number of responses",
            "counter",
            "rk_broker_responses_total",
            self.total_responses
        );
        metric!(
            "Total number of errors",
            "counter",
            "rk_broker_errors_total",
            self.total_errors
        );
        metric!(
            "Total Produce requests",
            "counter",
            "rk_broker_produce_requests_total",
            self.produce_requests
        );
        metric!(
            "Total Fetch requests",
            "counter",
            "rk_broker_fetch_requests_total",
            self.fetch_requests
        );
        metric!(
            "Total messages produced",
            "counter",
            "rk_broker_messages_produced_total",
            self.total_messages_produced
        );
        metric!(
            "Total bytes produced",
            "counter",
            "rk_broker_bytes_produced_total",
            self.total_bytes_produced
        );
        metric!(
            "Total bytes fetched",
            "counter",
            "rk_broker_bytes_fetched_total",
            self.total_bytes_fetched
        );
        metric!(
            "Current active connections",
            "gauge",
            "rk_broker_active_connections",
            self.active_connections
        );
        metric!(
            "Total connections accepted",
            "counter",
            "rk_broker_total_connections_total",
            self.total_connections
        );

        out
    }
}

/// HTTP 监控服务器
pub struct HttpMetricsServer<M: MetricsProvider> {
    /// 共享的指标提供者
    metrics: Arc<M>,
    /// 监听端口
    port: u16,
    /// 监听地址
    host: String,
    /// Broker ID (用于 Prometheus labels)
    broker_id: i32,
    /// 流控制器 (可选，用于 /status 端点)
    flow_controller: Option<Arc<FlowController>>,
}

impl<M: MetricsProvider> HttpMetricsServer<M> {
    /// 创建 HTTP 监控服务器
    pub fn new(host: String, port: u16, metrics: Arc<M>, broker_id: i32) -> Self {
        Self {
            metrics,
            port,
            host,
            broker_id,
            flow_controller: None,
        }
    }

    /// 设置流控制器 (用于 /status 端点)
    pub fn with_flow_controller(mut self, flow_controller: Arc<FlowController>) -> Self {
        self.flow_controller = Some(flow_controller);
        self
    }

    /// 运行 HTTP 服务器
    ///
    /// 接受 HTTP 连接并处理请求，直到收到关闭信号。
    pub async fn run(&self, mut shutdown_rx: watch::Receiver<bool>) -> rk_core::Result<()> {
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
async fn handle_http_request<M: MetricsProvider>(
    mut stream: tokio::net::TcpStream,
    metrics: Arc<M>,
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
        send_response(
            &mut stream,
            400,
            "Bad Request",
            "text/plain",
            r#"{"error":"bad request"}"#,
        )
        .await?;
        return Ok(());
    }

    let method = parts[0];
    let path = parts[1];

    if method != "GET" {
        send_response(
            &mut stream,
            405,
            "Method Not Allowed",
            "text/plain",
            r#"{"error":"method not allowed"}"#,
        )
        .await?;
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
            let prom = <MetricsSnapshot as PrometheusMetrics>::to_prometheus(&snapshot, broker_id);
            send_response(
                &mut stream,
                200,
                "OK",
                "text/plain; version=0.0.4; charset=utf-8",
                &prom,
            )
            .await?;
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
                broker_id,
                snapshot.uptime_secs,
                snapshot.total_requests,
                snapshot.total_errors,
                fc_json,
            );
            send_response(&mut stream, 200, "OK", "application/json", &json).await?;
        }
        _ => {
            send_response(
                &mut stream,
                404,
                "Not Found",
                "text/plain",
                r#"{"error":"not found"}"#,
            )
            .await?;
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
    use std::sync::atomic::{AtomicU64, Ordering};

    /// 测试用的最小指标提供者
    struct TestMetrics {
        requests: AtomicU64,
    }

    impl TestMetrics {
        fn new() -> Self {
            Self {
                requests: AtomicU64::new(0),
            }
        }

        fn record_request(&self) {
            self.requests.fetch_add(1, Ordering::Relaxed);
        }
    }

    impl MetricsProvider for TestMetrics {
        fn snapshot(&self) -> MetricsSnapshot {
            MetricsSnapshot {
                uptime_secs: 0,
                total_requests: self.requests.load(Ordering::Relaxed),
                total_responses: 0,
                total_errors: 0,
                produce_requests: 0,
                fetch_requests: 0,
                total_messages_produced: 0,
                total_bytes_produced: 0,
                total_bytes_fetched: 0,
                active_connections: 0,
                total_connections: 0,
            }
        }
    }

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

    #[test]
    fn test_prometheus_format() {
        use super::PrometheusMetrics;
        let snapshot = MetricsSnapshot {
            uptime_secs: 10,
            total_requests: 3,
            total_responses: 3,
            total_errors: 0,
            produce_requests: 2,
            fetch_requests: 1,
            total_messages_produced: 10,
            total_bytes_produced: 2048,
            total_bytes_fetched: 0,
            active_connections: 1,
            total_connections: 1,
        };
        let prom = snapshot.to_prometheus(1);
        assert!(prom.contains("# HELP rk_broker_uptime_seconds"));
        assert!(prom.contains("# TYPE rk_broker_requests_total counter"));
        assert!(prom.contains("rk_broker_requests_total{broker_id=\"1\"}"));
        assert!(prom.contains("rk_broker_messages_produced_total{broker_id=\"1\"}"));
    }

    #[tokio::test]
    async fn test_handle_health_request() {
        let metrics = Arc::new(TestMetrics::new());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let metrics_clone = metrics.clone();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handle_http_request(stream, metrics_clone, 0, None)
                .await
                .unwrap();
        });

        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();

        let mut buf = vec![0u8; 4096];
        let n = stream.read(&mut buf).await.unwrap();
        let response = String::from_utf8_lossy(&buf[..n]);
        assert!(response.contains("200 OK"));
        assert!(response.contains("\"status\":\"ok\""));
    }

    #[tokio::test]
    async fn test_handle_metrics_request() {
        let metrics = Arc::new(TestMetrics::new());
        metrics.record_request();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let metrics_clone = metrics.clone();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handle_http_request(stream, metrics_clone, 0, None)
                .await
                .unwrap();
        });

        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();

        let mut buf = vec![0u8; 4096];
        let n = stream.read(&mut buf).await.unwrap();
        let response = String::from_utf8_lossy(&buf[..n]);
        assert!(response.contains("200 OK"));
        assert!(response.contains("total_requests"));
    }

    #[tokio::test]
    async fn test_handle_404() {
        let metrics = Arc::new(TestMetrics::new());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let metrics_clone = metrics.clone();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handle_http_request(stream, metrics_clone, 0, None)
                .await
                .unwrap();
        });

        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET /unknown HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();

        let mut buf = vec![0u8; 4096];
        let n = stream.read(&mut buf).await.unwrap();
        let response = String::from_utf8_lossy(&buf[..n]);
        assert!(response.contains("404"));
    }

    #[tokio::test]
    async fn test_handle_status_request() {
        let metrics = Arc::new(TestMetrics::new());
        metrics.record_request();
        let fc = Arc::new(FlowController::new(&BrokerConfig::from_toml("").unwrap()));
        fc.try_accept_connection().unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let metrics_clone = metrics.clone();
        let fc_clone = fc.clone();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handle_http_request(stream, metrics_clone, 5, Some(fc_clone))
                .await
                .unwrap();
        });

        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET /status HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();

        let mut buf = vec![0u8; 4096];
        let n = stream.read(&mut buf).await.unwrap();
        let response = String::from_utf8_lossy(&buf[..n]);
        assert!(response.contains("200 OK"));
        assert!(response.contains(r#""broker_id":5"#));
        assert!(response.contains("flow_control"));
    }
}
