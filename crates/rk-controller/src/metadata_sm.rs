//! Metadata 状态机 — KRaft Metadata 管理
//!
//! 应用 Raft 日志中的 `LogEntry` 到内存 Metadata 状态。
//! 状态机是确定性的: 相同日志序列产生相同状态。
//!
//! 管理的 Metadata:
//! - Topics: topic_name → TopicMetadata (partitions, replication_factor, configs)
//! - Partitions: (topic, partition) → PartitionMetadata (leader, isr, replicas)
//! - Brokers: broker_id → BrokerMetadata (host, port, rack)
//! - Configs: resource_type:resource_name → Map<key, value>

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::raft_node::LogEntry;

// ─── Metadata 数据结构 ───────────────────────────────────────────────

/// Topic 元数据
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicMetadata {
    /// Topic 名称
    pub name: String,
    /// Topic UUID
    pub topic_id: u128,
    /// Partition 数量
    pub partition_count: u32,
    /// 副本因子
    pub replication_factor: u16,
    /// Topic 级别配置
    pub configs: HashMap<String, String>,
    /// 是否被标记为删除
    pub marked_for_deletion: bool,
}

/// Partition 元数据
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionMetadata {
    /// Topic 名称
    pub topic_name: String,
    /// Partition 编号
    pub partition_id: i32,
    /// 当前 Leader Broker ID
    pub leader: Option<i32>,
    /// Leader Epoch
    pub leader_epoch: i32,
    /// ISR (In-Sync Replicas) 列表
    pub isr: Vec<i32>,
    /// 所有副本 Broker ID 列表
    pub replicas: Vec<i32>,
}

/// Broker 元数据
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerMetadata {
    /// Broker ID
    pub broker_id: i32,
    /// 机架标识 (可选)
    pub rack: Option<String>,
    /// 主机地址
    pub host: String,
    /// 端口
    pub port: u16,
    /// 是否存活
    pub is_alive: bool,
}

/// 集群 Metadata 快照
///
/// 可用于持久化或恢复状态机状态。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataSnapshot {
    /// 已应用的日志条目数
    pub last_applied_log: u64,
    /// Topics
    pub topics: HashMap<String, TopicMetadata>,
    /// Partitions: key = "topic:partition_id"
    pub partitions: HashMap<String, PartitionMetadata>,
    /// Brokers
    pub brokers: HashMap<i32, BrokerMetadata>,
    /// 配置: key = "resource_type:resource_name:key"
    pub configs: HashMap<String, String>,
}

// ─── Metadata 状态机 ─────────────────────────────────────────────────

/// Metadata 状态机
///
/// 应用 Raft 日志条目到内存 Metadata，提供只读查询接口。
pub struct MetadataStateMachine {
    /// 已应用的最后一条日志索引
    last_applied: AtomicU64,
    /// Topics: topic_name → TopicMetadata
    topics: dashmap::DashMap<String, TopicMetadata>,
    /// Partitions: "topic:partition_id" → PartitionMetadata
    partitions: dashmap::DashMap<String, PartitionMetadata>,
    /// Brokers: broker_id → BrokerMetadata
    brokers: dashmap::DashMap<i32, BrokerMetadata>,
    /// 配置: "resource_type:resource_name:key" → value
    configs: dashmap::DashMap<String, String>,
}

impl MetadataStateMachine {
    /// 创建新的空状态机
    pub fn new() -> Self {
        Self {
            last_applied: AtomicU64::new(0),
            topics: dashmap::DashMap::new(),
            partitions: dashmap::DashMap::new(),
            brokers: dashmap::DashMap::new(),
            configs: dashmap::DashMap::new(),
        }
    }

    /// 从快照恢复状态机
    pub fn from_snapshot(snapshot: MetadataSnapshot) -> Self {
        let sm = Self::new();
        sm.last_applied
            .store(snapshot.last_applied_log, Ordering::SeqCst);

        for (name, meta) in snapshot.topics {
            sm.topics.insert(name, meta);
        }
        for (key, meta) in snapshot.partitions {
            sm.partitions.insert(key, meta);
        }
        for (id, meta) in snapshot.brokers {
            sm.brokers.insert(id, meta);
        }
        for (key, value) in snapshot.configs {
            sm.configs.insert(key, value);
        }

        info!(
            last_applied = snapshot.last_applied_log,
            topics = sm.topics.len(),
            partitions = sm.partitions.len(),
            brokers = sm.brokers.len(),
            "MetadataStateMachine restored from snapshot"
        );

        sm
    }

    /// 应用一条 Raft 日志条目到状态机
    ///
    /// 这是状态机的核心入口，由 Raft 框架在日志提交后调用。
    pub fn apply(&self, log_index: u64, entry: &LogEntry) {
        match entry {
            LogEntry::CreateTopic {
                topic_name,
                topic_id,
                partitions,
                replication_factor,
                configs,
            } => {
                self.apply_create_topic(
                    topic_name,
                    *topic_id,
                    *partitions,
                    *replication_factor,
                    configs,
                );
            }
            LogEntry::DeleteTopic { topic_name } => {
                self.apply_delete_topic(topic_name);
            }
            LogEntry::AssignPartition {
                topic_name,
                partition_id,
                replicas,
            } => {
                self.apply_assign_partition(topic_name, *partition_id, replicas);
            }
            LogEntry::UpdateLeaderAndIsr {
                topic_name,
                partition_id,
                leader,
                epoch,
                isr,
            } => {
                self.apply_update_leader_and_isr(topic_name, *partition_id, *leader, *epoch, isr);
            }
            LogEntry::RegisterBroker {
                broker_id,
                rack,
                host,
                port,
            } => {
                self.apply_register_broker(*broker_id, rack.clone(), host.clone(), *port);
            }
            LogEntry::UnregisterBroker { broker_id } => {
                self.apply_unregister_broker(*broker_id);
            }
            LogEntry::SetConfig {
                resource_type,
                resource_name,
                key,
                value,
            } => {
                self.apply_set_config(resource_type, resource_name, key, value);
            }
            LogEntry::UpdateFeature { name, version } => {
                self.apply_set_config("feature", name, "version", &version.to_string());
            }
            LogEntry::Noop => {
                // No-op: 仅推进 last_applied
            }
        }

        self.last_applied.store(log_index, Ordering::SeqCst);
    }

    /// 创建快照
    pub fn snapshot(&self) -> MetadataSnapshot {
        let topics: HashMap<String, TopicMetadata> = self
            .topics
            .iter()
            .map(|r| (r.key().clone(), r.value().clone()))
            .collect();

        let partitions: HashMap<String, PartitionMetadata> = self
            .partitions
            .iter()
            .map(|r| (r.key().clone(), r.value().clone()))
            .collect();

        let brokers: HashMap<i32, BrokerMetadata> = self
            .brokers
            .iter()
            .map(|r| (*r.key(), r.value().clone()))
            .collect();

        let configs: HashMap<String, String> = self
            .configs
            .iter()
            .map(|r| (r.key().clone(), r.value().clone()))
            .collect();

        MetadataSnapshot {
            last_applied_log: self.last_applied.load(Ordering::SeqCst),
            topics,
            partitions,
            brokers,
            configs,
        }
    }

    // ─── 内部 apply 方法 ─────────────────────────────────────────────

    fn apply_create_topic(
        &self,
        topic_name: &str,
        topic_id: u128,
        partitions: u32,
        replication_factor: u16,
        configs: &[(String, String)],
    ) {
        if self.topics.contains_key(topic_name) {
            warn!(topic = topic_name, "Topic already exists, skipping");
            return;
        }

        let config_map: HashMap<String, String> = configs.iter().cloned().collect();

        let meta = TopicMetadata {
            name: topic_name.to_string(),
            topic_id,
            partition_count: partitions,
            replication_factor,
            configs: config_map,
            marked_for_deletion: false,
        };

        self.topics.insert(topic_name.to_string(), meta);

        info!(
            topic = topic_name,
            partitions = partitions,
            replication_factor = replication_factor,
            "Topic created"
        );
    }

    fn apply_delete_topic(&self, topic_name: &str) {
        // 标记为删除 (实际删除在后续清理流程)
        if let Some(mut meta) = self.topics.get_mut(topic_name) {
            meta.marked_for_deletion = true;
            info!(topic = topic_name, "Topic marked for deletion");
        }

        // 删除关联的 partition 元数据
        let prefix = format!("{}:", topic_name);
        let keys_to_remove: Vec<String> = self
            .partitions
            .iter()
            .filter(|r| r.key().starts_with(&prefix))
            .map(|r| r.key().clone())
            .collect();

        for key in keys_to_remove {
            self.partitions.remove(&key);
        }
    }

    fn apply_assign_partition(&self, topic_name: &str, partition_id: i32, replicas: &[i32]) {
        let key = format!("{}:{}", topic_name, partition_id);

        let meta = if let Some(existing) = self.partitions.get(&key) {
            let mut m = existing.clone();
            m.replicas = replicas.to_vec();
            m
        } else {
            PartitionMetadata {
                topic_name: topic_name.to_string(),
                partition_id,
                leader: replicas.first().copied(),
                leader_epoch: 0,
                isr: replicas.to_vec(),
                replicas: replicas.to_vec(),
            }
        };

        self.partitions.insert(key, meta);
    }

    fn apply_update_leader_and_isr(
        &self,
        topic_name: &str,
        partition_id: i32,
        leader: i32,
        epoch: i32,
        isr: &[i32],
    ) {
        let key = format!("{}:{}", topic_name, partition_id);

        let meta = if let Some(existing) = self.partitions.get(&key) {
            let mut m = existing.clone();
            m.leader = Some(leader);
            m.leader_epoch = epoch;
            m.isr = isr.to_vec();
            m
        } else {
            PartitionMetadata {
                topic_name: topic_name.to_string(),
                partition_id,
                leader: Some(leader),
                leader_epoch: epoch,
                isr: isr.to_vec(),
                replicas: vec![],
            }
        };

        self.partitions.insert(key, meta);
    }

    fn apply_register_broker(&self, broker_id: i32, rack: Option<String>, host: String, port: u16) {
        info!(broker_id = broker_id, host = %host, port = port, "Broker registered");

        let meta = BrokerMetadata {
            broker_id,
            rack,
            host,
            port,
            is_alive: true,
        };

        self.brokers.insert(broker_id, meta);
    }

    fn apply_unregister_broker(&self, broker_id: i32) {
        if let Some(mut meta) = self.brokers.get_mut(&broker_id) {
            meta.is_alive = false;
            info!(
                broker_id = broker_id,
                "Broker unregistered (marked offline)"
            );
        }
    }

    fn apply_set_config(&self, resource_type: &str, resource_name: &str, key: &str, value: &str) {
        let config_key = format!("{}:{}:{}", resource_type, resource_name, key);
        self.configs.insert(config_key, value.to_string());
    }

    // ─── 只读查询 ────────────────────────────────────────────────────

    /// 获取最后应用的日志索引
    pub fn last_applied(&self) -> u64 {
        self.last_applied.load(Ordering::SeqCst)
    }

    /// 获取 Topic 元数据
    pub fn get_topic(&self, topic_name: &str) -> Option<TopicMetadata> {
        self.topics.get(topic_name).map(|r| r.value().clone())
    }

    /// 列出所有 Topic
    pub fn list_topics(&self) -> Vec<TopicMetadata> {
        self.topics.iter().map(|r| r.value().clone()).collect()
    }

    /// 获取 Topic 数量
    pub fn topic_count(&self) -> usize {
        self.topics.len()
    }

    /// 获取 Partition 元数据
    pub fn get_partition(&self, topic_name: &str, partition_id: i32) -> Option<PartitionMetadata> {
        let key = format!("{}:{}", topic_name, partition_id);
        self.partitions.get(&key).map(|r| r.value().clone())
    }

    /// 获取 Topic 的所有 Partition
    pub fn get_topic_partitions(&self, topic_name: &str) -> Vec<PartitionMetadata> {
        let prefix = format!("{}:", topic_name);
        self.partitions
            .iter()
            .filter(|r| r.key().starts_with(&prefix))
            .map(|r| r.value().clone())
            .collect()
    }

    /// 获取 Broker 元数据
    pub fn get_broker(&self, broker_id: i32) -> Option<BrokerMetadata> {
        self.brokers.get(&broker_id).map(|r| r.value().clone())
    }

    /// 列出所有存活的 Broker
    pub fn list_alive_brokers(&self) -> Vec<BrokerMetadata> {
        self.brokers
            .iter()
            .filter(|r| r.value().is_alive)
            .map(|r| r.value().clone())
            .collect()
    }

    /// 获取 Broker 数量 (含离线)
    pub fn broker_count(&self) -> usize {
        self.brokers.len()
    }

    /// 获取配置值
    pub fn get_config(
        &self,
        resource_type: &str,
        resource_name: &str,
        key: &str,
    ) -> Option<String> {
        let config_key = format!("{}:{}:{}", resource_type, resource_name, key);
        self.configs.get(&config_key).map(|r| r.value().clone())
    }
}

impl Default for MetadataStateMachine {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for MetadataStateMachine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetadataStateMachine")
            .field("last_applied", &self.last_applied.load(Ordering::SeqCst))
            .field("topics", &self.topics.len())
            .field("partitions", &self.partitions.len())
            .field("brokers", &self.brokers.len())
            .field("configs", &self.configs.len())
            .finish()
    }
}

// ─── 单元测试 ────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sm_new() {
        let sm = MetadataStateMachine::new();
        assert_eq!(sm.last_applied(), 0);
        assert_eq!(sm.topic_count(), 0);
        assert_eq!(sm.broker_count(), 0);
    }

    #[test]
    fn test_sm_apply_create_topic() {
        let sm = MetadataStateMachine::new();

        sm.apply(
            1,
            &LogEntry::CreateTopic {
                topic_name: "test-topic".to_string(),
                topic_id: 42,
                partitions: 3,
                replication_factor: 2,
                configs: vec![("retention.ms".to_string(), "86400000".to_string())],
            },
        );

        assert_eq!(sm.last_applied(), 1);
        assert_eq!(sm.topic_count(), 1);

        let topic = sm.get_topic("test-topic").unwrap();
        assert_eq!(topic.name, "test-topic");
        assert_eq!(topic.topic_id, 42);
        assert_eq!(topic.partition_count, 3);
        assert_eq!(topic.replication_factor, 2);
        assert_eq!(topic.configs.get("retention.ms").unwrap(), "86400000");
        assert!(!topic.marked_for_deletion);
    }

    #[test]
    fn test_sm_apply_delete_topic() {
        let sm = MetadataStateMachine::new();

        sm.apply(
            1,
            &LogEntry::CreateTopic {
                topic_name: "to-delete".to_string(),
                topic_id: 1,
                partitions: 1,
                replication_factor: 1,
                configs: vec![],
            },
        );

        sm.apply(
            2,
            &LogEntry::AssignPartition {
                topic_name: "to-delete".to_string(),
                partition_id: 0,
                replicas: vec![1],
            },
        );

        assert_eq!(sm.get_topic_partitions("to-delete").len(), 1);

        sm.apply(
            3,
            &LogEntry::DeleteTopic {
                topic_name: "to-delete".to_string(),
            },
        );

        let topic = sm.get_topic("to-delete").unwrap();
        assert!(topic.marked_for_deletion);
        assert_eq!(sm.get_topic_partitions("to-delete").len(), 0);
    }

    #[test]
    fn test_sm_apply_assign_partition() {
        let sm = MetadataStateMachine::new();

        sm.apply(
            1,
            &LogEntry::CreateTopic {
                topic_name: "test".to_string(),
                topic_id: 1,
                partitions: 3,
                replication_factor: 2,
                configs: vec![],
            },
        );

        sm.apply(
            2,
            &LogEntry::AssignPartition {
                topic_name: "test".to_string(),
                partition_id: 0,
                replicas: vec![1, 2],
            },
        );

        sm.apply(
            3,
            &LogEntry::AssignPartition {
                topic_name: "test".to_string(),
                partition_id: 1,
                replicas: vec![2, 3],
            },
        );

        let p0 = sm.get_partition("test", 0).unwrap();
        assert_eq!(p0.replicas, vec![1, 2]);
        assert_eq!(p0.leader, Some(1)); // 第一个 replica 是 leader

        let p1 = sm.get_partition("test", 1).unwrap();
        assert_eq!(p1.replicas, vec![2, 3]);
        assert_eq!(p1.leader, Some(2));

        let partitions = sm.get_topic_partitions("test");
        assert_eq!(partitions.len(), 2);
    }

    #[test]
    fn test_sm_apply_update_leader_and_isr() {
        let sm = MetadataStateMachine::new();

        sm.apply(
            1,
            &LogEntry::AssignPartition {
                topic_name: "test".to_string(),
                partition_id: 0,
                replicas: vec![1, 2, 3],
            },
        );

        sm.apply(
            2,
            &LogEntry::UpdateLeaderAndIsr {
                topic_name: "test".to_string(),
                partition_id: 0,
                leader: 2,
                epoch: 5,
                isr: vec![2, 3],
            },
        );

        let p = sm.get_partition("test", 0).unwrap();
        assert_eq!(p.leader, Some(2));
        assert_eq!(p.leader_epoch, 5);
        assert_eq!(p.isr, vec![2, 3]);
    }

    #[test]
    fn test_sm_apply_register_broker() {
        let sm = MetadataStateMachine::new();

        sm.apply(
            1,
            &LogEntry::RegisterBroker {
                broker_id: 1,
                rack: Some("rack-a".to_string()),
                host: "192.168.1.1".to_string(),
                port: 9092,
            },
        );

        sm.apply(
            2,
            &LogEntry::RegisterBroker {
                broker_id: 2,
                rack: Some("rack-b".to_string()),
                host: "192.168.1.2".to_string(),
                port: 9092,
            },
        );

        assert_eq!(sm.broker_count(), 2);

        let broker = sm.get_broker(1).unwrap();
        assert_eq!(broker.broker_id, 1);
        assert_eq!(broker.rack, Some("rack-a".to_string()));
        assert!(broker.is_alive);

        let alive = sm.list_alive_brokers();
        assert_eq!(alive.len(), 2);
    }

    #[test]
    fn test_sm_apply_unregister_broker() {
        let sm = MetadataStateMachine::new();

        sm.apply(
            1,
            &LogEntry::RegisterBroker {
                broker_id: 1,
                rack: None,
                host: "127.0.0.1".to_string(),
                port: 9092,
            },
        );

        sm.apply(2, &LogEntry::UnregisterBroker { broker_id: 1 });

        let broker = sm.get_broker(1).unwrap();
        assert!(!broker.is_alive);

        let alive = sm.list_alive_brokers();
        assert_eq!(alive.len(), 0);
    }

    #[test]
    fn test_sm_apply_set_config() {
        let sm = MetadataStateMachine::new();

        sm.apply(
            1,
            &LogEntry::SetConfig {
                resource_type: "topic".to_string(),
                resource_name: "my-topic".to_string(),
                key: "retention.ms".to_string(),
                value: "604800000".to_string(),
            },
        );

        let value = sm.get_config("topic", "my-topic", "retention.ms");
        assert_eq!(value, Some("604800000".to_string()));
    }

    #[test]
    fn test_sm_apply_update_feature() {
        let sm = MetadataStateMachine::new();

        sm.apply(
            1,
            &LogEntry::UpdateFeature {
                name: "metadata.version".to_string(),
                version: 17,
            },
        );

        let value = sm.get_config("feature", "metadata.version", "version");
        assert_eq!(value, Some("17".to_string()));
    }

    #[test]
    fn test_sm_apply_noop() {
        let sm = MetadataStateMachine::new();
        sm.apply(1, &LogEntry::Noop);
        assert_eq!(sm.last_applied(), 1);
        assert_eq!(sm.topic_count(), 0);
    }

    #[test]
    fn test_sm_snapshot_and_restore() {
        let sm = MetadataStateMachine::new();

        sm.apply(
            1,
            &LogEntry::RegisterBroker {
                broker_id: 1,
                rack: None,
                host: "127.0.0.1".to_string(),
                port: 9092,
            },
        );

        sm.apply(
            2,
            &LogEntry::CreateTopic {
                topic_name: "test".to_string(),
                topic_id: 42,
                partitions: 3,
                replication_factor: 2,
                configs: vec![],
            },
        );

        sm.apply(
            3,
            &LogEntry::AssignPartition {
                topic_name: "test".to_string(),
                partition_id: 0,
                replicas: vec![1, 2],
            },
        );

        let snapshot = sm.snapshot();
        assert_eq!(snapshot.last_applied_log, 3);
        assert_eq!(snapshot.topics.len(), 1);
        assert_eq!(snapshot.partitions.len(), 1);
        assert_eq!(snapshot.brokers.len(), 1);

        // 从快照恢复
        let sm2 = MetadataStateMachine::from_snapshot(snapshot);
        assert_eq!(sm2.last_applied(), 3);
        assert_eq!(sm2.topic_count(), 1);
        assert_eq!(sm2.broker_count(), 1);

        let topic = sm2.get_topic("test").unwrap();
        assert_eq!(topic.topic_id, 42);
    }

    #[test]
    fn test_sm_duplicate_create_topic() {
        let sm = MetadataStateMachine::new();

        sm.apply(
            1,
            &LogEntry::CreateTopic {
                topic_name: "dup".to_string(),
                topic_id: 1,
                partitions: 1,
                replication_factor: 1,
                configs: vec![],
            },
        );

        // 重复创建应该被忽略
        sm.apply(
            2,
            &LogEntry::CreateTopic {
                topic_name: "dup".to_string(),
                topic_id: 2,
                partitions: 3,
                replication_factor: 2,
                configs: vec![],
            },
        );

        assert_eq!(sm.topic_count(), 1);
        let topic = sm.get_topic("dup").unwrap();
        assert_eq!(topic.topic_id, 1); // 保留第一次创建的
    }

    #[test]
    fn test_sm_get_nonexistent() {
        let sm = MetadataStateMachine::new();
        assert!(sm.get_topic("nonexistent").is_none());
        assert!(sm.get_partition("nonexistent", 0).is_none());
        assert!(sm.get_broker(999).is_none());
        assert!(sm.get_config("a", "b", "c").is_none());
    }

    #[test]
    fn test_metadata_snapshot_serde() {
        let snapshot = MetadataSnapshot {
            last_applied_log: 42,
            topics: HashMap::new(),
            partitions: HashMap::new(),
            brokers: HashMap::new(),
            configs: HashMap::new(),
        };

        let json = serde_json::to_string(&snapshot).unwrap();
        let decoded: MetadataSnapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.last_applied_log, 42);
    }

    #[test]
    fn test_sm_debug() {
        let sm = MetadataStateMachine::new();
        let debug_str = format!("{:?}", sm);
        assert!(debug_str.contains("MetadataStateMachine"));
    }
}
