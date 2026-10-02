//! Raft 节点封装 — KRaft 共识协议
//!
//! 基于 openraft 实现 Raft 共识，定义 R-Kafka 集群的类型配置。
//! 每个 Controller 节点运行一个 RaftNode 实例，参与 Leader 选举和日志复制。
//!
//! 架构:
//! - `TypeConfig`: openraft 类型配置 (NodeId, Node, Entry 等)
//! - `RaftNode`: 封装 openraft::Raft，提供高层 API
//! - `LocalNode`: 节点信息 (地址、角色)

use std::fmt;

use openraft::{BasicNode, Config, SnapshotPolicy};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing::info;

use rk_core::types::BrokerId;

// ─── openraft 类型配置 ────────────────────────────────────────────────

// openraft 类型配置:
// - `NodeId = u64`: 由 BrokerId 转换
// - `Node = BasicNode`: openraft 内置节点类型 (address)
// - `D = LogEntry`: 应用层日志条目
// - `SnapshotData = Cursor<Vec<u8>>`: 快照数据流
openraft::declare_raft_types!(
    /// R-Kafka Raft 类型配置
    pub TypeConfig:
        D = LogEntry,
        R = (),
        NodeId = u64,
        Node = BasicNode,
        Entry = openraft::Entry<TypeConfig>,
        SnapshotData = std::io::Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);

/// 节点信息
///
/// 描述集群中一个 Controller/Broker 节点的元数据。
/// 比 openraft::BasicNode 多包含角色信息。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LocalNode {
    /// 节点 ID (与 BrokerId 对应)
    pub node_id: u64,
    /// RPC 地址 (host:port)
    pub address: String,
    /// 节点角色
    pub role: NodeRole,
}

impl LocalNode {
    /// 转换为 openraft BasicNode
    pub fn to_basic_node(&self) -> BasicNode {
        BasicNode::new(&self.address)
    }
}

/// 节点角色
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum NodeRole {
    /// 同时担任 Controller 和 Broker
    ControllerBroker,
    /// 仅 Controller
    Controller,
    /// 仅 Broker
    Broker,
}

impl fmt::Display for NodeRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NodeRole::ControllerBroker => write!(f, "controller+broker"),
            NodeRole::Controller => write!(f, "controller"),
            NodeRole::Broker => write!(f, "broker"),
        }
    }
}

/// 日志条目
///
/// Raft 日志中复制的命令，对应 KRaft Metadata Record 的变更操作。
/// `declare_raft_types!` 宏会自动为 `D` 类型实现 `AppData` trait。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum LogEntry {
    /// 创建 Topic
    CreateTopic {
        topic_name: String,
        topic_id: u128,
        partitions: u32,
        replication_factor: u16,
        configs: Vec<(String, String)>,
    },
    /// 删除 Topic
    DeleteTopic { topic_name: String },
    /// 更新 Partition 分配
    AssignPartition {
        topic_name: String,
        partition_id: i32,
        replicas: Vec<i32>,
    },
    /// 更新 Leader & ISR
    UpdateLeaderAndIsr {
        topic_name: String,
        partition_id: i32,
        leader: i32,
        epoch: i32,
        isr: Vec<i32>,
    },
    /// Broker 注册
    RegisterBroker {
        broker_id: i32,
        rack: Option<String>,
        host: String,
        port: u16,
    },
    /// Broker 注销
    UnregisterBroker { broker_id: i32 },
    /// 配置变更
    SetConfig {
        resource_type: String,
        resource_name: String,
        key: String,
        value: String,
    },
    /// Feature 版本更新
    UpdateFeature { name: String, version: u16 },
    /// 空条目 (用于 No-op 提交)
    Noop,
}

// ─── Raft 节点 ────────────────────────────────────────────────────────

/// Raft 节点封装
///
/// 管理 Raft 共识实例的生命周期，提供高层 API 用于:
/// - 初始化节点并加入集群
/// - 提交 Metadata 变更请求
/// - 查询当前 Leader 和集群状态
pub struct RaftNode {
    /// 本节点 ID
    node_id: u64,
    /// Raft 配置
    config: std::sync::Arc<Config>,
    /// 节点信息
    local_node: LocalNode,
    /// 集群成员: node_id -> LocalNode
    members: dashmap::DashMap<u64, LocalNode>,
    /// 是否已启动
    started: Mutex<bool>,
}

impl RaftNode {
    /// 创建新的 Raft 节点
    ///
    /// # Arguments
    /// * `node_id` - 本节点 ID (与 BrokerId 对应)
    /// * `address` - RPC 监听地址 (host:port)
    /// * `role` - 节点角色
    pub fn new(node_id: u64, address: String, role: NodeRole) -> Self {
        let config = Config {
            // 快照策略: 每 10000 条日志触发一次快照
            snapshot_policy: SnapshotPolicy::LogsSinceLast(10_000),
            // 选举超时: 1s
            election_timeout_min: 1000,
            election_timeout_max: 1500,
            // 心跳间隔: 300ms
            heartbeat_interval: 300,
            ..Default::default()
        };

        let local_node = LocalNode {
            node_id,
            address,
            role,
        };

        let members = dashmap::DashMap::new();
        members.insert(node_id, local_node.clone());

        info!(
            node_id = node_id,
            address = %local_node.address,
            role = %local_node.role,
            "RaftNode created"
        );

        Self {
            node_id,
            config: std::sync::Arc::new(config),
            local_node,
            members,
            started: Mutex::new(false),
        }
    }

    /// 获取本节点 ID
    pub fn node_id(&self) -> u64 {
        self.node_id
    }

    /// 获取本节点信息
    pub fn local_node(&self) -> &LocalNode {
        &self.local_node
    }

    /// 获取 Raft 配置
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// 添加集群成员
    pub fn add_member(&self, node: LocalNode) {
        info!(
            node_id = node.node_id,
            address = %node.address,
            "Adding cluster member"
        );
        self.members.insert(node.node_id, node);
    }

    /// 移除集群成员
    pub fn remove_member(&self, node_id: u64) -> Option<LocalNode> {
        info!(node_id = node_id, "Removing cluster member");
        self.members.remove(&node_id).map(|(_, v)| v)
    }

    /// 获取所有集群成员
    pub fn members(&self) -> Vec<LocalNode> {
        self.members.iter().map(|r| r.value().clone()).collect()
    }

    /// 获取成员数量
    pub fn member_count(&self) -> usize {
        self.members.len()
    }

    /// 将 BrokerId 转换为 Raft NodeId (u64)
    pub fn broker_id_to_node_id(broker_id: BrokerId) -> u64 {
        broker_id.0 as u64
    }

    /// 将 Raft NodeId (u64) 转换为 BrokerId
    pub fn node_id_to_broker_id(node_id: u64) -> BrokerId {
        BrokerId(node_id as i32)
    }

    /// 标记节点已启动
    pub async fn mark_started(&self) {
        let mut started = self.started.lock().await;
        *started = true;
        info!(node_id = self.node_id, "RaftNode marked as started");
    }

    /// 检查节点是否已启动
    pub async fn is_started(&self) -> bool {
        *self.started.lock().await
    }

    /// 获取当前集群成员列表 (用于初始化 Raft membership)
    pub fn initial_members(&self) -> std::collections::BTreeMap<u64, LocalNode> {
        self.members
            .iter()
            .map(|r| (*r.key(), r.value().clone()))
            .collect()
    }

    /// 获取 BasicNode 映射 (用于 openraft API)
    pub fn basic_node_map(&self) -> std::collections::BTreeMap<u64, BasicNode> {
        self.members
            .iter()
            .map(|r| (*r.key(), r.value().to_basic_node()))
            .collect()
    }
}

impl fmt::Debug for RaftNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RaftNode")
            .field("node_id", &self.node_id)
            .field("local_node", &self.local_node)
            .field("member_count", &self.members.len())
            .finish()
    }
}

// ─── 单元测试 ────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_local_node_creation() {
        let node = LocalNode {
            node_id: 1,
            address: "127.0.0.1:9092".to_string(),
            role: NodeRole::ControllerBroker,
        };
        assert_eq!(node.node_id, 1);
        assert_eq!(node.address, "127.0.0.1:9092");
        assert_eq!(node.role, NodeRole::ControllerBroker);
    }

    #[test]
    fn test_local_node_to_basic_node() {
        let node = LocalNode {
            node_id: 1,
            address: "127.0.0.1:9092".to_string(),
            role: NodeRole::Broker,
        };
        let basic = node.to_basic_node();
        // BasicNode 包含 address
        let json = serde_json::to_string(&basic).unwrap();
        assert!(json.contains("127.0.0.1:9092"));
    }

    #[test]
    fn test_node_role_display() {
        assert_eq!(
            format!("{}", NodeRole::ControllerBroker),
            "controller+broker"
        );
        assert_eq!(format!("{}", NodeRole::Controller), "controller");
        assert_eq!(format!("{}", NodeRole::Broker), "broker");
    }

    #[test]
    fn test_log_entry_variants() {
        let entry = LogEntry::CreateTopic {
            topic_name: "test".to_string(),
            topic_id: 42,
            partitions: 3,
            replication_factor: 2,
            configs: vec![("retention.ms".to_string(), "86400000".to_string())],
        };
        assert!(matches!(entry, LogEntry::CreateTopic { .. }));

        let noop = LogEntry::Noop;
        assert_eq!(noop, LogEntry::Noop);
    }

    #[test]
    fn test_log_entry_serde_roundtrip() {
        let entry = LogEntry::CreateTopic {
            topic_name: "my-topic".to_string(),
            topic_id: 123,
            partitions: 6,
            replication_factor: 3,
            configs: vec![],
        };

        let json = serde_json::to_string(&entry).unwrap();
        let decoded: LogEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(entry, decoded);
    }

    #[test]
    fn test_log_entry_delete_topic_roundtrip() {
        let entry = LogEntry::DeleteTopic {
            topic_name: "old-topic".to_string(),
        };
        let json = serde_json::to_string(&entry).unwrap();
        let decoded: LogEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(entry, decoded);
    }

    #[test]
    fn test_log_entry_update_leader_and_isr() {
        let entry = LogEntry::UpdateLeaderAndIsr {
            topic_name: "test".to_string(),
            partition_id: 0,
            leader: 1,
            epoch: 5,
            isr: vec![1, 2, 3],
        };
        let json = serde_json::to_string(&entry).unwrap();
        let decoded: LogEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(entry, decoded);
    }

    #[test]
    fn test_raft_node_creation() {
        let node = RaftNode::new(1, "127.0.0.1:9092".to_string(), NodeRole::ControllerBroker);
        assert_eq!(node.node_id(), 1);
        assert_eq!(node.local_node().node_id, 1);
        assert_eq!(node.member_count(), 1); // 自身
    }

    #[test]
    fn test_raft_node_add_remove_member() {
        let node = RaftNode::new(1, "127.0.0.1:9092".to_string(), NodeRole::ControllerBroker);

        let member2 = LocalNode {
            node_id: 2,
            address: "127.0.0.1:9093".to_string(),
            role: NodeRole::Broker,
        };
        let member3 = LocalNode {
            node_id: 3,
            address: "127.0.0.1:9094".to_string(),
            role: NodeRole::Controller,
        };

        node.add_member(member2.clone());
        node.add_member(member3.clone());
        assert_eq!(node.member_count(), 3);

        let members = node.members();
        assert_eq!(members.len(), 3);

        let removed = node.remove_member(2);
        assert!(removed.is_some());
        assert_eq!(node.member_count(), 2);
    }

    #[test]
    fn test_raft_node_initial_members() {
        let node = RaftNode::new(1, "127.0.0.1:9092".to_string(), NodeRole::ControllerBroker);
        node.add_member(LocalNode {
            node_id: 2,
            address: "127.0.0.1:9093".to_string(),
            role: NodeRole::Broker,
        });

        let initial = node.initial_members();
        assert_eq!(initial.len(), 2);
        assert!(initial.contains_key(&1));
        assert!(initial.contains_key(&2));
    }

    #[test]
    fn test_raft_node_basic_node_map() {
        let node = RaftNode::new(1, "127.0.0.1:9092".to_string(), NodeRole::ControllerBroker);
        node.add_member(LocalNode {
            node_id: 2,
            address: "127.0.0.1:9093".to_string(),
            role: NodeRole::Broker,
        });

        let basic_map = node.basic_node_map();
        assert_eq!(basic_map.len(), 2);
        assert!(basic_map.contains_key(&1));
        assert!(basic_map.contains_key(&2));
    }

    #[test]
    fn test_broker_id_conversion() {
        let broker_id = BrokerId(42);
        let raft_id = RaftNode::broker_id_to_node_id(broker_id);
        assert_eq!(raft_id, 42);

        let back = RaftNode::node_id_to_broker_id(raft_id);
        assert_eq!(back, broker_id);
    }

    #[tokio::test]
    async fn test_raft_node_started_flag() {
        let node = RaftNode::new(1, "127.0.0.1:9092".to_string(), NodeRole::ControllerBroker);
        assert!(!node.is_started().await);

        node.mark_started().await;
        assert!(node.is_started().await);
    }

    #[test]
    fn test_raft_node_debug() {
        let node = RaftNode::new(1, "127.0.0.1:9092".to_string(), NodeRole::ControllerBroker);
        let debug_str = format!("{:?}", node);
        assert!(debug_str.contains("RaftNode"));
        assert!(debug_str.contains("node_id: 1"));
    }

    #[test]
    fn test_local_node_serde_roundtrip() {
        let node = LocalNode {
            node_id: 1,
            address: "127.0.0.1:9092".to_string(),
            role: NodeRole::ControllerBroker,
        };
        let json = serde_json::to_string(&node).unwrap();
        let decoded: LocalNode = serde_json::from_str(&json).unwrap();
        assert_eq!(node, decoded);
    }

    #[test]
    fn test_log_entry_register_broker() {
        let entry = LogEntry::RegisterBroker {
            broker_id: 1,
            rack: Some("rack-a".to_string()),
            host: "192.168.1.1".to_string(),
            port: 9092,
        };
        let json = serde_json::to_string(&entry).unwrap();
        let decoded: LogEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(entry, decoded);
    }

    #[test]
    fn test_log_entry_set_config() {
        let entry = LogEntry::SetConfig {
            resource_type: "topic".to_string(),
            resource_name: "my-topic".to_string(),
            key: "retention.ms".to_string(),
            value: "604800000".to_string(),
        };
        let json = serde_json::to_string(&entry).unwrap();
        let decoded: LogEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(entry, decoded);
    }
}
