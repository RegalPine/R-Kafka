//! Metadata Cache — 集群 Metadata 缓存
//!
//! 缓存 Broker 列表、Topic/Partition 映射和 Leader 信息。

use std::collections::HashMap;
use std::time::{Duration, Instant};

use tracing::debug;

/// Topic-Partition 键
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct TopicPartition {
    pub topic: String,
    pub partition: i32,
}

impl TopicPartition {
    pub fn new(topic: &str, partition: i32) -> Self {
        Self {
            topic: topic.to_string(),
            partition,
        }
    }
}

impl std::fmt::Display for TopicPartition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.topic, self.partition)
    }
}

/// Partition Leader 信息
#[derive(Debug, Clone)]
pub struct PartitionLeader {
    pub topic: String,
    pub partition: i32,
    pub leader_id: i32,
    pub leader_host: String,
    pub leader_port: u16,
}

/// Broker 端点
#[derive(Debug, Clone)]
pub struct BrokerEndpoint {
    pub broker_id: i32,
    pub host: String,
    pub port: u16,
    pub rack: Option<String>,
}

impl BrokerEndpoint {
    pub fn address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

/// Metadata 缓存
///
/// 缓存集群 Metadata，支持 TTL 过期刷新。
pub struct MetadataCache {
    /// Broker 列表: broker_id → endpoint
    brokers: HashMap<i32, BrokerEndpoint>,
    /// Partition Leader: (topic, partition) → leader
    leaders: HashMap<TopicPartition, PartitionLeader>,
    /// Topic → Partition 数量
    topic_partitions: HashMap<String, i32>,
    /// 上次刷新时间
    last_refresh: Option<Instant>,
    /// 缓存 TTL
    ttl: Duration,
}

impl MetadataCache {
    /// 创建空缓存
    pub fn new(ttl: Duration) -> Self {
        Self {
            brokers: HashMap::new(),
            leaders: HashMap::new(),
            topic_partitions: HashMap::new(),
            last_refresh: None,
            ttl,
        }
    }

    /// 更新 Broker 列表
    pub fn update_brokers(&mut self, brokers: Vec<BrokerEndpoint>) {
        self.brokers.clear();
        for b in brokers {
            self.brokers.insert(b.broker_id, b);
        }
        debug!(
            broker_count = self.brokers.len(),
            "Metadata cache: brokers updated"
        );
    }

    /// 更新 Partition Leader
    pub fn update_leader(&mut self, leader: PartitionLeader) {
        let tp = TopicPartition::new(&leader.topic, leader.partition);
        self.leaders.insert(tp, leader);
    }

    /// 更新 Topic Partition 数量
    pub fn update_topic_partitions(&mut self, topic: &str, count: i32) {
        self.topic_partitions.insert(topic.to_string(), count);
    }

    /// 标记已刷新
    pub fn mark_refreshed(&mut self) {
        self.last_refresh = Some(Instant::now());
    }

    /// 是否需要刷新
    pub fn needs_refresh(&self) -> bool {
        match self.last_refresh {
            Some(t) => t.elapsed() > self.ttl,
            None => true,
        }
    }

    /// 获取 Partition Leader
    pub fn get_leader(&self, topic: &str, partition: i32) -> Option<&PartitionLeader> {
        self.leaders.get(&TopicPartition::new(topic, partition))
    }

    /// 获取 Broker 端点
    pub fn get_broker(&self, broker_id: i32) -> Option<&BrokerEndpoint> {
        self.brokers.get(&broker_id)
    }

    /// 获取 Topic 的 Partition 数量
    pub fn get_partition_count(&self, topic: &str) -> Option<i32> {
        self.topic_partitions.get(topic).copied()
    }

    /// 获取所有 Broker
    pub fn all_brokers(&self) -> Vec<&BrokerEndpoint> {
        self.brokers.values().collect()
    }

    /// 获取所有 Topic
    pub fn all_topics(&self) -> Vec<&str> {
        self.topic_partitions.keys().map(|s| s.as_str()).collect()
    }

    /// Broker 数量
    pub fn broker_count(&self) -> usize {
        self.brokers.len()
    }

    /// Topic 数量
    pub fn topic_count(&self) -> usize {
        self.topic_partitions.len()
    }

    /// 清空缓存
    pub fn clear(&mut self) {
        self.brokers.clear();
        self.leaders.clear();
        self.topic_partitions.clear();
        self.last_refresh = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topic_partition() {
        let tp = TopicPartition::new("test", 0);
        assert_eq!(tp.to_string(), "test-0");
        assert_eq!(tp.topic, "test");
        assert_eq!(tp.partition, 0);
    }

    #[test]
    fn test_broker_endpoint() {
        let ep = BrokerEndpoint {
            broker_id: 1,
            host: "10.0.0.1".to_string(),
            port: 9092,
            rack: Some("rack-a".to_string()),
        };
        assert_eq!(ep.address(), "10.0.0.1:9092");
    }

    #[test]
    fn test_metadata_cache_new() {
        let cache = MetadataCache::new(Duration::from_secs(60));
        assert!(cache.needs_refresh());
        assert_eq!(cache.broker_count(), 0);
        assert_eq!(cache.topic_count(), 0);
    }

    #[test]
    fn test_update_brokers() {
        let mut cache = MetadataCache::new(Duration::from_secs(60));
        cache.update_brokers(vec![
            BrokerEndpoint {
                broker_id: 1,
                host: "h1".to_string(),
                port: 9092,
                rack: None,
            },
            BrokerEndpoint {
                broker_id: 2,
                host: "h2".to_string(),
                port: 9092,
                rack: None,
            },
        ]);
        assert_eq!(cache.broker_count(), 2);
        assert!(cache.get_broker(1).is_some());
        assert!(cache.get_broker(3).is_none());
    }

    #[test]
    fn test_update_leader() {
        let mut cache = MetadataCache::new(Duration::from_secs(60));
        cache.update_leader(PartitionLeader {
            topic: "test".to_string(),
            partition: 0,
            leader_id: 1,
            leader_host: "h1".to_string(),
            leader_port: 9092,
        });
        assert!(cache.get_leader("test", 0).is_some());
        assert!(cache.get_leader("test", 1).is_none());
    }

    #[test]
    fn test_topic_partitions() {
        let mut cache = MetadataCache::new(Duration::from_secs(60));
        cache.update_topic_partitions("test", 6);
        assert_eq!(cache.get_partition_count("test"), Some(6));
        assert_eq!(cache.get_partition_count("unknown"), None);
    }

    #[test]
    fn test_mark_refreshed() {
        let mut cache = MetadataCache::new(Duration::from_secs(60));
        assert!(cache.needs_refresh());
        cache.mark_refreshed();
        assert!(!cache.needs_refresh());
    }

    #[test]
    fn test_clear() {
        let mut cache = MetadataCache::new(Duration::from_secs(60));
        cache.update_brokers(vec![BrokerEndpoint {
            broker_id: 1,
            host: "h1".to_string(),
            port: 9092,
            rack: None,
        }]);
        cache.update_topic_partitions("test", 3);
        assert_eq!(cache.broker_count(), 1);
        assert_eq!(cache.topic_count(), 1);

        cache.clear();
        assert_eq!(cache.broker_count(), 0);
        assert_eq!(cache.topic_count(), 0);
        assert!(cache.needs_refresh());
    }

    #[test]
    fn test_all_topics() {
        let mut cache = MetadataCache::new(Duration::from_secs(60));
        cache.update_topic_partitions("t1", 1);
        cache.update_topic_partitions("t2", 2);
        let topics = cache.all_topics();
        assert_eq!(topics.len(), 2);
    }
}
