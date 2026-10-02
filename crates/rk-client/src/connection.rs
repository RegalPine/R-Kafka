//! Broker Connection — Broker 连接管理
//!
//! 管理与单个 Broker 的 TCP 连接，处理请求/响应帧的编解码。
//!
//! ```text
//! ┌──────────────┐     Request Frame      ┌──────────────┐
//! │  Client       │ ──────────────────────→ │  Broker       │
//! │  Connection   │                        │              │
//! │               │ ←────────────────────── │              │
//! └──────────────┘     Response Frame      └──────────────┘
//! ```

use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tracing::{debug, info};

use crate::config::ClientConfig;
use crate::error::{ClientError, ClientResult};

// ─── BrokerConnection ────────────────────────────────────────────────

/// Broker 连接
///
/// 封装到单个 Broker 的 TCP 连接，提供请求发送和响应接收。
pub struct BrokerConnection {
    /// Broker 地址 (host:port)
    address: String,
    /// Broker ID (连接后获取)
    broker_id: AtomicI32,
    /// TCP 流
    stream: Mutex<Option<TcpStream>>,
    /// 关联 ID 计数器
    correlation_id: AtomicI32,
    /// 客户端 ID
    client_id: String,
    /// 请求超时
    _request_timeout: Duration,
    /// 连接超时
    connection_timeout: Duration,
}

impl BrokerConnection {
    /// 创建新连接 (不立即连接)
    pub fn new(address: &str, config: &ClientConfig) -> Self {
        Self {
            address: address.to_string(),
            broker_id: AtomicI32::new(-1),
            stream: Mutex::new(None),
            correlation_id: AtomicI32::new(0),
            client_id: config.client_id.clone(),
            _request_timeout: config.request_timeout,
            connection_timeout: config.connection_timeout,
        }
    }

    /// 建立 TCP 连接
    pub async fn connect(&self) -> ClientResult<()> {
        let stream = tokio::time::timeout(
            self.connection_timeout,
            TcpStream::connect(&self.address),
        )
        .await
        .map_err(|_| ClientError::Timeout(format!("Connect to {} timed out", self.address)))?
        .map_err(|e| ClientError::Connection(format!("Failed to connect to {}: {}", self.address, e)))?;

        stream.set_nodelay(true).ok();

        let mut guard = self.stream.lock().await;
        *guard = Some(stream);

        info!(address = %self.address, "Connected to broker");
        Ok(())
    }

    /// 是否已连接
    pub async fn is_connected(&self) -> bool {
        self.stream.lock().await.is_some()
    }

    /// 断开连接
    pub async fn disconnect(&self) {
        let mut guard = self.stream.lock().await;
        if let Some(mut stream) = guard.take() {
            stream.shutdown().await.ok();
            debug!(address = %self.address, "Disconnected from broker");
        }
    }

    /// 获取下一个关联 ID
    pub fn next_correlation_id(&self) -> i32 {
        self.correlation_id.fetch_add(1, Ordering::Relaxed)
    }

    /// 发送原始请求帧并接收响应
    pub async fn send_and_receive(&self, request_bytes: &[u8]) -> ClientResult<Vec<u8>> {
        let mut guard = self.stream.lock().await;
        let stream = guard.as_mut().ok_or_else(|| {
            ClientError::Connection("Not connected".to_string())
        })?;

        // 发送: 4 字节长度前缀 + 请求体
        let len = request_bytes.len() as u32;
        stream.write_all(&len.to_be_bytes()).await?;
        stream.write_all(request_bytes).await?;
        stream.flush().await?;

        // 接收: 4 字节长度前缀 + 响应体
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await?;
        let resp_len = u32::from_be_bytes(len_buf) as usize;

        let mut resp_buf = vec![0u8; resp_len];
        stream.read_exact(&mut resp_buf).await?;

        Ok(resp_buf)
    }

    /// 获取 Broker 地址
    pub fn address(&self) -> &str {
        &self.address
    }

    /// 设置 Broker ID
    pub fn set_broker_id(&self, id: i32) {
        self.broker_id.store(id, Ordering::Relaxed);
    }

    /// 获取 Broker ID
    pub fn broker_id(&self) -> i32 {
        self.broker_id.load(Ordering::Relaxed)
    }

    /// 获取客户端 ID
    pub fn client_id(&self) -> &str {
        &self.client_id
    }
}

// ─── ConnectionPool ──────────────────────────────────────────────────

/// 连接池 (简单的每 Broker 单连接)
///
/// 管理到多个 Broker 的连接。
pub struct ConnectionPool {
    /// 连接: address → BrokerConnection
    connections: dashmap::DashMap<String, Arc<BrokerConnection>>,
    /// 客户端配置
    config: ClientConfig,
}

impl ConnectionPool {
    /// 创建连接池
    pub fn new(config: ClientConfig) -> Self {
        Self {
            connections: dashmap::DashMap::new(),
            config,
        }
    }

    /// 获取或创建到指定 Broker 的连接
    pub async fn get_or_connect(&self, address: &str) -> ClientResult<Arc<BrokerConnection>> {
        if let Some(conn) = self.connections.get(address) {
            if conn.is_connected().await {
                return Ok(conn.clone());
            }
        }

        let conn = Arc::new(BrokerConnection::new(address, &self.config));
        conn.connect().await?;
        self.connections.insert(address.to_string(), conn.clone());
        Ok(conn)
    }

    /// 移除连接
    pub async fn remove(&self, address: &str) {
        if let Some((_, conn)) = self.connections.remove(address) {
            conn.disconnect().await;
        }
    }

    /// 关闭所有连接
    pub async fn close_all(&self) {
        for entry in self.connections.iter() {
            entry.value().disconnect().await;
        }
        self.connections.clear();
        info!("All connections closed");
    }

    /// 活跃连接数
    pub fn active_count(&self) -> usize {
        self.connections.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_broker_connection_new() {
        let config = ClientConfig::new()
            .client_id("test-client")
            .build()
            .unwrap();
        let conn = BrokerConnection::new("localhost:9092", &config);
        assert_eq!(conn.address(), "localhost:9092");
        assert_eq!(conn.client_id(), "test-client");
        assert_eq!(conn.broker_id(), -1);
    }

    #[test]
    fn test_correlation_id_increment() {
        let config = ClientConfig::default();
        let conn = BrokerConnection::new("localhost:9092", &config);
        assert_eq!(conn.next_correlation_id(), 0);
        assert_eq!(conn.next_correlation_id(), 1);
        assert_eq!(conn.next_correlation_id(), 2);
    }

    #[test]
    fn test_set_broker_id() {
        let config = ClientConfig::default();
        let conn = BrokerConnection::new("localhost:9092", &config);
        conn.set_broker_id(42);
        assert_eq!(conn.broker_id(), 42);
    }

    #[tokio::test]
    async fn test_not_connected() {
        let config = ClientConfig::default();
        let conn = BrokerConnection::new("localhost:9092", &config);
        assert!(!conn.is_connected().await);
    }

    #[tokio::test]
    async fn test_send_without_connect_fails() {
        let config = ClientConfig::default();
        let conn = BrokerConnection::new("localhost:9092", &config);
        let result = conn.send_and_receive(b"test").await;
        assert!(result.is_err());
    }

    #[test]
    fn test_connection_pool_new() {
        let config = ClientConfig::default();
        let pool = ConnectionPool::new(config);
        assert_eq!(pool.active_count(), 0);
    }

    #[tokio::test]
    async fn test_pool_close_all_empty() {
        let config = ClientConfig::default();
        let pool = ConnectionPool::new(config);
        pool.close_all().await;
        assert_eq!(pool.active_count(), 0);
    }
}
