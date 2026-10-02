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

pub mod raft_node;
pub mod metadata_sm;
pub mod metadata_record;
pub mod broker_reg;
pub mod partition_alloc;
pub mod cluster_bootstrap;
pub mod rack_awareness;
pub mod replica_placement;
pub mod feature_manager;

// Re-exports
pub use raft_node::{TypeConfig, RaftNode, LocalNode, NodeRole, LogEntry};
pub use metadata_sm::{
    MetadataStateMachine, MetadataSnapshot, TopicMetadata, PartitionMetadata, BrokerMetadata,
};
pub use metadata_record::{
    MetadataRecord, RecordType, MetadataLog, MetadataLogEntry,
};
pub use broker_reg::{BrokerRegistry, BrokerRegistration, BrokerHeartbeatState};
pub use partition_alloc::{
    PartitionAllocator, PartitionAssignment, AllocationStrategy, AllocationError, BrokerInfo,
};
pub use cluster_bootstrap::{
    ClusterState, ClusterBootstrapConfig, ClusterBootstrap, ClusterBootstrapSummary,
    initialize_cluster_metadata,
};
pub use rack_awareness::{
    RackId, BrokerRackInfo, RackTopology, RackTopologySummary,
    RackViolation, RackAwareConfig, LeaderDistribution,
};
pub use replica_placement::{
    PlacementStrategy, PlacementPlan, PlacementStats, PlacementError,
    ReplicaPlacer, ReassignmentPlan, compute_reassignment,
};
pub use feature_manager::{FeatureManager, BrokerFeature, FinalizedFeature};
