//! KafkaAdmin — 管理操作客户端
//!
//! 提供 Topic 管理 API:
//! - 创建 Topic
//! - 删除 Topic
//! - 列出 Topic
//! - 查看 Topic 详情
//!
//! ```ignore
//! let admin = KafkaAdmin::new(config);
//! admin.create_topic("my-topic", 6, 3).await?;
//! let topics = admin.list_topics().await?;
//! admin.delete_topic("my-topic").await?;
//! ```

use std::time::Duration;

use tracing::info;

use crate::config::ClientConfig;
use crate::connection::ConnectionPool;
use crate::error::{ClientError, ClientResult};
use crate::metadata::MetadataCache;

// ─── Topic 信息 ──────────────────────────────────────────────────────

/// Topic 详情
#[derive(Debug, Clone)]
pub struct TopicInfo {
    /// Topic 名称
    pub name: String,
    /// Partition 数量
    pub partitions: i32,
    /// 副本因子
    pub replication_factor: i16,
    /// Topic 配置
    pub configs: Vec<(String, String)>,
}

/// Partition 信息
#[derive(Debug, Clone)]
pub struct PartitionInfo {
    /// Topic 名称
    pub topic: String,
    /// Partition ID
    pub partition_id: i32,
    /// Leader Broker ID
    pub leader: i32,
    /// ISR 列表
    pub isr: Vec<i32>,
    /// 副本列表
    pub replicas: Vec<i32>,
}

/// Topic 创建选项
#[derive(Debug, Clone)]
pub struct NewTopic {
    /// Topic 名称
    pub name: String,
    /// Partition 数量
    pub partitions: i32,
    /// 副本因子
    pub replication_factor: i16,
    /// Topic 配置
    pub configs: Vec<(String, String)>,
}

impl NewTopic {
    pub fn new(name: &str, partitions: i32, replication_factor: i16) -> Self {
        Self {
            name: name.to_string(),
            partitions,
            replication_factor,
            configs: vec![],
        }
    }

    pub fn with_config(mut self, key: &str, value: &str) -> Self {
        self.configs.push((key.to_string(), value.to_string()));
        self
    }
}

// ─── KafkaAdmin ──────────────────────────────────────────────────────

/// Kafka Admin Client
///
/// 高层 API，用于 Topic 管理操作。
pub struct KafkaAdmin {
    _config: ClientConfig,
    connection_pool: ConnectionPool,
    metadata_cache: MetadataCache,
}

impl KafkaAdmin {
    /// 创建新的 Admin 客户端
    pub fn new(config: ClientConfig) -> Self {
        let pool = ConnectionPool::new(config.clone());
        let cache = MetadataCache::new(Duration::from_secs(300));
        Self {
            _config: config,
            connection_pool: pool,
            metadata_cache: cache,
        }
    }

    /// 创建 Topic
    pub async fn create_topic(&self, name: &str, partitions: i32, replication_factor: i16) -> ClientResult<()> {
        if name.is_empty() {
            return Err(ClientError::Config("Topic name cannot be empty".to_string()));
        }
        if partitions <= 0 {
            return Err(ClientError::Config("Partitions must be > 0".to_string()));
        }
        if replication_factor <= 0 {
            return Err(ClientError::Config("Replication factor must be > 0".to_string()));
        }

        // Phase 3: 实际需要通过 CreateTopics API 发送请求
        info!(topic = name, partitions = partitions, rf = replication_factor, "Topic created (simulated)");
        Ok(())
    }

    /// 创建 Topic (带选项)
    pub async fn create_topic_with_options(&self, new_topic: &NewTopic) -> ClientResult<()> {
        self.create_topic(&new_topic.name, new_topic.partitions, new_topic.replication_factor).await
    }

    /// 删除 Topic
    pub async fn delete_topic(&self, name: &str) -> ClientResult<()> {
        if name.is_empty() {
            return Err(ClientError::Config("Topic name cannot be empty".to_string()));
        }
        // Phase 3: 实际需要通过 DeleteTopics API 发送请求
        info!(topic = name, "Topic deleted (simulated)");
        Ok(())
    }

    /// 列出 Topic
    pub async fn list_topics(&self) -> ClientResult<Vec<String>> {
        // Phase 3: 实际需要通过 Metadata API 获取
        let topics = self.metadata_cache.all_topics().iter().map(|s| s.to_string()).collect();
        Ok(topics)
    }

    /// 获取 Topic 详情
    pub async fn describe_topic(&self, name: &str) -> ClientResult<TopicInfo> {
        // Phase 3: 模拟返回
        Ok(TopicInfo {
            name: name.to_string(),
            partitions: 1,
            replication_factor: 1,
            configs: vec![],
        })
    }

    /// 获取 Partition 信息
    pub async fn describe_partitions(&self, topic: &str) -> ClientResult<Vec<PartitionInfo>> {
        // Phase 3: 模拟返回
        Ok(vec![PartitionInfo {
            topic: topic.to_string(),
            partition_id: 0,
            leader: 1,
            isr: vec![1],
            replicas: vec![1],
        }])
    }

    /// 获取集群 Broker 列表
    pub async fn list_brokers(&self) -> ClientResult<Vec<(i32, String)>> {
        let brokers = self.metadata_cache.all_brokers()
            .iter()
            .map(|b| (b.broker_id, b.address()))
            .collect();
        Ok(brokers)
    }

    /// 关闭 Admin 客户端
    pub async fn close(&self) -> ClientResult<()> {
        self.connection_pool.close_all().await;
        info!("Admin client closed");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_admin() -> KafkaAdmin {
        KafkaAdmin::new(ClientConfig::default())
    }

    #[test]
    fn test_new_topic() {
        let topic = NewTopic::new("test", 6, 3);
        assert_eq!(topic.name, "test");
        assert_eq!(topic.partitions, 6);
        assert_eq!(topic.replication_factor, 3);
        assert!(topic.configs.is_empty());
    }

    #[test]
    fn test_new_topic_with_config() {
        let topic = NewTopic::new("test", 6, 3)
            .with_config("retention.ms", "86400000")
            .with_config("cleanup.policy", "compact");
        assert_eq!(topic.configs.len(), 2);
    }

    #[tokio::test]
    async fn test_create_topic_empty_name_fails() {
        let admin = test_admin();
        assert!(admin.create_topic("", 6, 3).await.is_err());
    }

    #[tokio::test]
    async fn test_create_topic_zero_partitions_fails() {
        let admin = test_admin();
        assert!(admin.create_topic("test", 0, 3).await.is_err());
    }

    #[tokio::test]
    async fn test_create_topic_zero_rf_fails() {
        let admin = test_admin();
        assert!(admin.create_topic("test", 6, 0).await.is_err());
    }

    #[tokio::test]
    async fn test_create_topic_success() {
        let admin = test_admin();
        assert!(admin.create_topic("test", 6, 3).await.is_ok());
    }

    #[tokio::test]
    async fn test_create_topic_with_options() {
        let admin = test_admin();
        let topic = NewTopic::new("test", 6, 3).with_config("retention.ms", "86400000");
        assert!(admin.create_topic_with_options(&topic).await.is_ok());
    }

    #[tokio::test]
    async fn test_delete_topic_empty_name_fails() {
        let admin = test_admin();
        assert!(admin.delete_topic("").await.is_err());
    }

    #[tokio::test]
    async fn test_delete_topic_success() {
        let admin = test_admin();
        assert!(admin.delete_topic("test").await.is_ok());
    }

    #[tokio::test]
    async fn test_list_topics() {
        let admin = test_admin();
        let topics = admin.list_topics().await.unwrap();
        assert!(topics.is_empty()); // 缓存为空
    }

    #[tokio::test]
    async fn test_describe_topic() {
        let admin = test_admin();
        let info = admin.describe_topic("test").await.unwrap();
        assert_eq!(info.name, "test");
    }

    #[tokio::test]
    async fn test_describe_partitions() {
        let admin = test_admin();
        let parts = admin.describe_partitions("test").await.unwrap();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].topic, "test");
    }

    #[tokio::test]
    async fn test_list_brokers() {
        let admin = test_admin();
        let brokers = admin.list_brokers().await.unwrap();
        assert!(brokers.is_empty());
    }

    #[tokio::test]
    async fn test_admin_close() {
        let admin = test_admin();
        assert!(admin.close().await.is_ok());
    }
}
