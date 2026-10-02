//! Client Config — 客户端配置
//!
//! 提供 `ClientConfig` 构建器模式配置客户端参数。

use std::collections::HashMap;
use std::time::Duration;

use crate::error::{ClientError, ClientResult};

/// 客户端配置
///
/// 使用构建器模式设置参数:
/// ```ignore
/// let config = ClientConfig::new()
///     .bootstrap_servers("localhost:9092")
///     .client_id("my-app")
///     .request_timeout(Duration::from_secs(30))
///     .build()?;
/// ```
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// Bootstrap 服务器列表 (逗号分隔)
    pub bootstrap_servers: Vec<String>,
    /// 客户端标识
    pub client_id: String,
    /// 请求超时
    pub request_timeout: Duration,
    /// 连接超时
    pub connection_timeout: Duration,
    /// 重试次数
    pub retries: u32,
    /// 重试退避时间
    pub retry_backoff: Duration,
    /// acks 模式 (0, 1, -1/all)
    pub acks: i16,
    /// 额外属性
    pub properties: HashMap<String, String>,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: vec!["localhost:9092".to_string()],
            client_id: "rk-client".to_string(),
            request_timeout: Duration::from_secs(30),
            connection_timeout: Duration::from_secs(10),
            retries: 3,
            retry_backoff: Duration::from_millis(100),
            acks: 1,
            properties: HashMap::new(),
        }
    }
}

impl ClientConfig {
    /// 创建默认配置
    pub fn new() -> Self {
        Self::default()
    }

    /// 设置 Bootstrap 服务器
    pub fn bootstrap_servers(mut self, servers: &str) -> Self {
        self.bootstrap_servers = servers
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        self
    }

    /// 设置客户端 ID
    pub fn client_id(mut self, id: &str) -> Self {
        self.client_id = id.to_string();
        self
    }

    /// 设置请求超时
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// 设置连接超时
    pub fn connection_timeout(mut self, timeout: Duration) -> Self {
        self.connection_timeout = timeout;
        self
    }

    /// 设置重试次数
    pub fn retries(mut self, retries: u32) -> Self {
        self.retries = retries;
        self
    }

    /// 设置 acks 模式
    pub fn acks(mut self, acks: i16) -> Self {
        self.acks = acks;
        self
    }

    /// 设置额外属性
    pub fn set(mut self, key: &str, value: &str) -> Self {
        self.properties.insert(key.to_string(), value.to_string());
        self
    }

    /// 获取额外属性
    pub fn get(&self, key: &str) -> Option<&str> {
        self.properties.get(key).map(|s| s.as_str())
    }

    /// 构建并验证配置
    pub fn build(self) -> ClientResult<Self> {
        if self.bootstrap_servers.is_empty() {
            return Err(ClientError::Config(
                "bootstrap_servers cannot be empty".to_string(),
            ));
        }
        if self.client_id.is_empty() {
            return Err(ClientError::Config("client_id cannot be empty".to_string()));
        }
        if self.acks != 0 && self.acks != 1 && self.acks != -1 {
            return Err(ClientError::Config("acks must be 0, 1, or -1".to_string()));
        }
        Ok(self)
    }
}

// ─── ProducerConfig ──────────────────────────────────────────────────

/// Producer 配置
#[derive(Debug, Clone)]
pub struct ProducerConfig {
    /// 基础客户端配置
    pub client: ClientConfig,
    /// 批量大小 (字节)
    pub batch_size: usize,
    /// 延迟时间 (攒批最大等待)
    pub linger_ms: u64,
    /// 压缩类型
    pub compression: CompressionType,
}

/// 压缩类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionType {
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

impl Default for ProducerConfig {
    fn default() -> Self {
        Self {
            client: ClientConfig::default(),
            batch_size: 16384,
            linger_ms: 5,
            compression: CompressionType::None,
        }
    }
}

// ─── ConsumerConfig ──────────────────────────────────────────────────

/// Consumer 配置
#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    /// 基础客户端配置
    pub client: ClientConfig,
    /// 消费组 ID
    pub group_id: String,
    /// 每次 poll 最大返回消息数
    pub max_poll_records: usize,
    /// 自动提交偏移量
    pub enable_auto_commit: bool,
    /// 自动提交间隔
    pub auto_commit_interval_ms: u64,
    /// 偏移量重置策略
    pub auto_offset_reset: OffsetReset,
}

/// 偏移量重置策略
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OffsetReset {
    Earliest,
    Latest,
    None,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            client: ClientConfig::default(),
            group_id: "default-group".to_string(),
            max_poll_records: 500,
            enable_auto_commit: true,
            auto_commit_interval_ms: 5000,
            auto_offset_reset: OffsetReset::Latest,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ClientConfig::default();
        assert_eq!(config.bootstrap_servers, vec!["localhost:9092"]);
        assert_eq!(config.client_id, "rk-client");
        assert_eq!(config.acks, 1);
        assert_eq!(config.retries, 3);
    }

    #[test]
    fn test_builder_pattern() {
        let config = ClientConfig::new()
            .bootstrap_servers("host1:9092,host2:9092")
            .client_id("test-app")
            .request_timeout(Duration::from_secs(60))
            .acks(-1)
            .set("custom.key", "custom.value")
            .build()
            .unwrap();

        assert_eq!(config.bootstrap_servers.len(), 2);
        assert_eq!(config.client_id, "test-app");
        assert_eq!(config.acks, -1);
        assert_eq!(config.get("custom.key"), Some("custom.value"));
    }

    #[test]
    fn test_empty_bootstrap_fails() {
        let result = ClientConfig::new().bootstrap_servers("").build();
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_acks_fails() {
        let result = ClientConfig::new().acks(5).build();
        assert!(result.is_err());
    }

    #[test]
    fn test_producer_config_default() {
        let config = ProducerConfig::default();
        assert_eq!(config.batch_size, 16384);
        assert_eq!(config.linger_ms, 5);
        assert_eq!(config.compression, CompressionType::None);
    }

    #[test]
    fn test_consumer_config_default() {
        let config = ConsumerConfig::default();
        assert_eq!(config.group_id, "default-group");
        assert_eq!(config.max_poll_records, 500);
        assert!(config.enable_auto_commit);
        assert_eq!(config.auto_offset_reset, OffsetReset::Latest);
    }

    #[test]
    fn test_offset_reset_variants() {
        assert_ne!(OffsetReset::Earliest, OffsetReset::Latest);
        assert_ne!(OffsetReset::Latest, OffsetReset::None);
    }

    #[test]
    fn test_compression_variants() {
        assert_ne!(CompressionType::None, CompressionType::Gzip);
        assert_ne!(CompressionType::Snappy, CompressionType::Zstd);
    }
}
