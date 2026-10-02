//! rk-controller: R-Kafka 控制器 Crate
//!
//! 实现 KRaft 模式的集群控制层:
//! - **raft_node**: openraft 类型配置和 Raft 节点封装
//! - **metadata_sm**: Metadata 状态机 (应用日志条目到内存 Metadata)
//! - **broker_reg**: Broker 注册表和心跳追踪
//! - **partition_alloc**: Partition 副本分配策略 (轮询/机架感知)
//!
//! 架构:
//! ```text
//! ┌─────────────────────────────────────────────────┐
//! │              rk-controller                       │
//! │                                                  │
//! │  ┌──────────┐  ┌──────────────┐  ┌───────────┐  │
//! │  │ RaftNode │  │ Metadata SM  │  │ Broker    │  │
//! │  │(openraft)│  │ (apply log)  │  │ Registry  │  │
//! │  └────┬─────┘  └──────┬───────┘  └─────┬─────┘  │
//! │       │               │                │         │
//! │       └───────────────┼────────────────┘         │
//! │                       │                          │
//! │              ┌────────┴────────┐                 │
//! │              │ Partition Alloc │                 │
//! │              │ (round-robin /  │                 │
//! │              │  rack-aware)    │                 │
//! │              └─────────────────┘                 │
//! └─────────────────────────────────────────────────┘
//! ```

pub mod broker_reg;
pub mod cluster_bootstrap;
pub mod feature_manager;
pub mod metadata_record;
pub mod metadata_sm;
pub mod partition_alloc;
pub mod rack_awareness;
pub mod raft_node;
pub mod replica_placement;

// Re-exports
pub use broker_reg::{BrokerHeartbeatState, BrokerRegistration, BrokerRegistry};
pub use cluster_bootstrap::{
    initialize_cluster_metadata, ClusterBootstrap, ClusterBootstrapConfig, ClusterBootstrapSummary,
    ClusterState,
};
pub use feature_manager::{BrokerFeature, FeatureManager, FinalizedFeature};
pub use metadata_record::{MetadataLog, MetadataLogEntry, MetadataRecord, RecordType};
pub use metadata_sm::{
    BrokerMetadata, MetadataSnapshot, MetadataStateMachine, PartitionMetadata, TopicMetadata,
};
pub use partition_alloc::{
    AllocationError, AllocationStrategy, BrokerInfo, PartitionAllocator, PartitionAssignment,
};
pub use rack_awareness::{
    BrokerRackInfo, LeaderDistribution, RackAwareConfig, RackId, RackTopology, RackTopologySummary,
    RackViolation,
};
pub use raft_node::{LocalNode, LogEntry, NodeRole, RaftNode, TypeConfig};
pub use replica_placement::{
    compute_reassignment, PlacementError, PlacementPlan, PlacementStats, PlacementStrategy,
    ReassignmentPlan, ReplicaPlacer,
};
