//! rk-replication: R-Kafka 副本复制引擎
//!
//! 实现 Kafka 副本协议的核心组件:
//! - `replica`: 副本数据结构 (Replica, ReplicaRole, ReplicaState, PartitionReplicaSet)
//! - `isr`: ISR 追踪器 (ISRTracker, ISR 扩缩容, Lag 检测)
//! - `hw_manager`: HW/LEO 管理器 (HighWatermarkManager, HW 单调递增)
//! - `replica_manager`: 副本管理器 (ReplicaManager, 统一管理接口)
//!
//! Phase 1: 单 Broker 模式，所有 Partition 的 Leader 都是本地 Broker。
//! Phase 3: 多 Broker 模式，支持 Follower Replica 管理和跨 Broker 复制。
//!
//! 架构:
//! ```text
//! ReplicaManager
//!     │
//!     ├── PartitionReplicaSet (per partition)
//!     │       ├── Leader Replica
//!     │       ├── Follower Replica(s)
//!     │       └── ISR Set
//!     │
//!     ├── HighWatermarkManager (HW/LEO 计算)
//!     └── ISRTracker (ISR 扩缩容)
//! ```

pub mod replica;
pub mod isr;
pub mod hw_manager;
pub mod replica_manager;
pub mod replica_fetcher;
pub mod election;
pub mod leader;

// Re-exports
pub use replica::{
    ReplicaRole, ReplicaState, ReplicaInfo, PartitionReplicaSet,
};
pub use isr::{
    ISRConfig, ISREvent, ShrinkReason, ISRTracker,
};
pub use hw_manager::{
    HighWatermarkManager, HWUpdate, LagStats,
};
pub use replica_manager::{
    ReplicaManager, ReplicaPartitionKey, ReplicaManagerSummary,
};
pub use replica_fetcher::{
    ReplicaFetcherConfig, ReplicaFetcher, ReplicationFetchRequest,
    ReplicationFetchResponse, ReplicatedBatch, PartitionFetcherState,
    FetchCycleResult, ReplicaFetcherSummary,
};
pub use election::{
    ElectionConfig, ElectionStrategy, ElectionResult, ElectionFailure,
    LeaderElector,
};
pub use leader::{
    FollowerProgress, LeaderReplica, LeaderReplicaSummary,
};
