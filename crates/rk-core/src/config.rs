//! R-Kafka 全局配置
//!
//! 从 TOML 文件加载，结构与设计文档 10.3 配置示例一致。

use serde::Deserialize;

/// Broker 顶层配置
#[derive(Debug, Clone, Deserialize)]
pub struct BrokerConfig {
    #[serde(default)]
    pub broker: BrokerSection,
    #[serde(default)]
    pub storage: StorageSection,
    #[serde(default)]
    pub retention: RetentionSection,
    #[serde(default)]
    pub replication: ReplicationSection,
    #[serde(default)]
    pub network: NetworkSection,
    #[serde(default)]
    pub security: SecuritySection,
    #[serde(default)]
    pub controller: ControllerSection,
    #[serde(default)]
    pub observability: ObservabilitySection,
    #[serde(default)]
    pub producer: ProducerSection,
}

impl BrokerConfig {
    /// 从 TOML 字符串解析配置
    pub fn from_toml(s: &str) -> crate::error::Result<Self> {
        toml::from_str(s).map_err(|e| crate::error::RkError::ConfigParse(e.to_string()))
    }

    /// 从文件路径加载配置
    pub fn from_file(path: &str) -> crate::error::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        Self::from_toml(&content)
    }
}

/// [broker] 段
#[derive(Debug, Clone, Deserialize)]
pub struct BrokerSection {
    /// Broker ID
    #[serde(default = "default_broker_id")]
    pub id: i32,
    /// 监听地址
    #[serde(default = "default_host")]
    pub host: String,
    /// 监听端口
    #[serde(default = "default_port")]
    pub port: u16,
    /// Rack ID (用于 Rack Awareness)
    #[serde(default)]
    pub rack: Option<String>,
}

impl Default for BrokerSection {
    fn default() -> Self {
        Self {
            id: default_broker_id(),
            host: default_host(),
            port: default_port(),
            rack: None,
        }
    }
}

/// [storage] 段
#[derive(Debug, Clone, Deserialize)]
pub struct StorageSection {
    /// 数据根目录
    #[serde(default = "default_data_dir")]
    pub data_dir: String,
    /// Segment 最大字节数 (默认 1GB)
    #[serde(default = "default_segment_max_size")]
    pub segment_max_size: u64,
    /// Segment 最大存活时间 (毫秒，默认 30min)
    #[serde(default = "default_segment_max_time_ms")]
    pub segment_max_time_ms: u64,
    /// Flush 间隔 (毫秒)
    #[serde(default = "default_flush_interval_ms")]
    pub flush_interval_ms: u64,
    /// Flush 模式: async | sync | hybrid
    #[serde(default = "default_flush_mode")]
    pub flush_mode: String,
    /// I/O 引擎: stdio | io_uring (Linux only)
    #[serde(default = "default_io_engine")]
    pub io_engine: String,
}

impl Default for StorageSection {
    fn default() -> Self {
        Self {
            data_dir: default_data_dir(),
            segment_max_size: default_segment_max_size(),
            segment_max_time_ms: default_segment_max_time_ms(),
            flush_interval_ms: default_flush_interval_ms(),
            flush_mode: default_flush_mode(),
            io_engine: default_io_engine(),
        }
    }
}

/// [retention] 段
#[derive(Debug, Clone, Deserialize)]
pub struct RetentionSection {
    /// 最大保留字节数 (默认 1TB)
    #[serde(default = "default_retention_max_bytes")]
    pub max_bytes: u64,
    /// 最大保留时间 (毫秒，默认 7 天)
    #[serde(default = "default_retention_max_ms")]
    pub max_ms: u64,
    /// 是否启用 Log Compaction
    #[serde(default)]
    pub compaction_enabled: bool,
}

impl Default for RetentionSection {
    fn default() -> Self {
        Self {
            max_bytes: default_retention_max_bytes(),
            max_ms: default_retention_max_ms(),
            compaction_enabled: false,
        }
    }
}

/// [replication] 段
#[derive(Debug, Clone, Deserialize)]
pub struct ReplicationSection {
    /// 默认副本因子
    #[serde(default = "default_replication_factor")]
    pub default_replication_factor: i32,
    /// 最小 ISR 大小
    #[serde(default = "default_min_isr")]
    pub min_isr_size: i32,
    /// 副本最大滞后时间 (毫秒)
    #[serde(default = "default_replica_lag_ms")]
    pub replica_lag_time_max_ms: u64,
    /// 是否允许 Unclean Leader Election
    #[serde(default)]
    pub unclean_leader_election: bool,
}

impl Default for ReplicationSection {
    fn default() -> Self {
        Self {
            default_replication_factor: default_replication_factor(),
            min_isr_size: default_min_isr(),
            replica_lag_time_max_ms: default_replica_lag_ms(),
            unclean_leader_election: false,
        }
    }
}

/// [network] 段
#[derive(Debug, Clone, Deserialize)]
pub struct NetworkSection {
    /// 最大连接数
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,
    /// 最大请求大小 (字节)
    #[serde(default = "default_max_request_size")]
    pub max_request_size: usize,
    /// 最大待处理字节数
    #[serde(default = "default_max_pending_bytes")]
    pub max_pending_bytes: usize,
}

impl Default for NetworkSection {
    fn default() -> Self {
        Self {
            max_connections: default_max_connections(),
            max_request_size: default_max_request_size(),
            max_pending_bytes: default_max_pending_bytes(),
        }
    }
}

/// [security] 段
#[derive(Debug, Clone, Deserialize)]
pub struct SecuritySection {
    #[serde(default)]
    pub tls_enabled: bool,
    /// TLS 模式: one-way | mutual
    #[serde(default = "default_tls_mode")]
    pub tls_mode: String,
    #[serde(default)]
    pub cert_path: Option<String>,
    #[serde(default)]
    pub key_path: Option<String>,
    #[serde(default)]
    pub ca_path: Option<String>,
    #[serde(default)]
    pub sasl_enabled: bool,
    #[serde(default)]
    pub sasl_mechanisms: Vec<String>,
    /// SASL/PLAIN 用户列表: [[security.sasl_users]] username = "admin", password = "secret"
    #[serde(default)]
    pub sasl_users: Vec<SaslUserConfig>,
}

/// SASL 用户凭证配置
#[derive(Debug, Clone, Deserialize)]
pub struct SaslUserConfig {
    pub username: String,
    pub password: String,
}

impl Default for SecuritySection {
    fn default() -> Self {
        Self {
            tls_enabled: false,
            tls_mode: default_tls_mode(),
            cert_path: None,
            key_path: None,
            ca_path: None,
            sasl_enabled: false,
            sasl_mechanisms: Vec::new(),
            sasl_users: Vec::new(),
        }
    }
}

/// [controller] 段
#[derive(Debug, Clone, Deserialize)]
pub struct ControllerSection {
    #[serde(default)]
    pub quorum_peers: Vec<String>,
    #[serde(default = "default_election_timeout_ms")]
    pub election_timeout_ms: u64,
}

impl Default for ControllerSection {
    fn default() -> Self {
        Self {
            quorum_peers: Vec::new(),
            election_timeout_ms: default_election_timeout_ms(),
        }
    }
}

/// [observability] 段
#[derive(Debug, Clone, Deserialize)]
pub struct ObservabilitySection {
    #[serde(default)]
    pub metrics_enabled: bool,
    #[serde(default = "default_metrics_port")]
    pub metrics_port: u16,
    #[serde(default)]
    pub tracing_enabled: bool,
    #[serde(default = "default_tracing_layer")]
    pub tracing_layer: String,
    #[serde(default)]
    pub tracing_endpoint: Option<String>,
}

impl Default for ObservabilitySection {
    fn default() -> Self {
        Self {
            metrics_enabled: false,
            metrics_port: default_metrics_port(),
            tracing_enabled: false,
            tracing_layer: default_tracing_layer(),
            tracing_endpoint: None,
        }
    }
}

// ─── 默认值函数 ──────────────────────────────────────────────────────

/// [producer] 段 — 攒批优化配置
#[derive(Debug, Clone, Deserialize)]
pub struct ProducerSection {
    /// 单批次最大字节数 (默认 1MB)
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// 最大等待时间 (毫秒，默认 5ms)
    #[serde(default = "default_linger_ms")]
    pub linger_ms: u64,
}

impl Default for ProducerSection {
    fn default() -> Self {
        Self {
            batch_size: default_batch_size(),
            linger_ms: default_linger_ms(),
        }
    }
}

fn default_broker_id() -> i32 { 1 }
fn default_host() -> String { "0.0.0.0".to_string() }
fn default_port() -> u16 { 9092 }
fn default_data_dir() -> String { "/data/r-kafka".to_string() }
fn default_segment_max_size() -> u64 { 1_073_741_824 } // 1GB
fn default_segment_max_time_ms() -> u64 { 1_800_000 } // 30min
fn default_flush_interval_ms() -> u64 { 5 }
fn default_flush_mode() -> String { "hybrid".to_string() }
fn default_io_engine() -> String { "stdio".to_string() }
fn default_retention_max_bytes() -> u64 { 1_099_511_627_776 } // 1TB
fn default_retention_max_ms() -> u64 { 604_800_000 } // 7 days
fn default_replication_factor() -> i32 { 3 }
fn default_min_isr() -> i32 { 2 }
fn default_replica_lag_ms() -> u64 { 10_000 }
fn default_max_connections() -> usize { 100_000 }
fn default_max_request_size() -> usize { 104_857_600 } // 100MB
fn default_max_pending_bytes() -> usize { 536_870_912 } // 512MB
fn default_tls_mode() -> String { "one-way".to_string() }
fn default_election_timeout_ms() -> u64 { 3000 }
fn default_metrics_port() -> u16 { 9090 }
fn default_tracing_layer() -> String { "fmt".to_string() }
fn default_batch_size() -> usize { 1_048_576 } // 1MB
fn default_linger_ms() -> u64 { 5 }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = BrokerConfig::from_toml("").unwrap();
        assert_eq!(config.broker.id, 1);
        assert_eq!(config.broker.port, 9092);
        assert_eq!(config.storage.segment_max_size, 1_073_741_824);
        assert_eq!(config.network.max_connections, 100_000);
    }

    #[test]
    fn test_custom_config() {
        let toml = r#"
            [broker]
            id = 5
            host = "127.0.0.1"
            port = 19092

            [storage]
            data_dir = "/tmp/r-kafka"
        "#;
        let config = BrokerConfig::from_toml(toml).unwrap();
        assert_eq!(config.broker.id, 5);
        assert_eq!(config.broker.host, "127.0.0.1");
        assert_eq!(config.broker.port, 19092);
        assert_eq!(config.storage.data_dir, "/tmp/r-kafka");
        // 其他字段使用默认值
        assert_eq!(config.storage.segment_max_size, 1_073_741_824);
    }
}
