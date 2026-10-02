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

pub mod election;
pub mod hw_manager;
pub mod isr;
pub mod leader;
pub mod replica;
pub mod replica_fetcher;
pub mod replica_manager;

// Re-exports
pub use election::{
    ElectionConfig, ElectionFailure, ElectionResult, ElectionStrategy, LeaderElector,
};
pub use hw_manager::{HWUpdate, HighWatermarkManager, LagStats};
pub use isr::{ISRConfig, ISREvent, ISRTracker, ShrinkReason};
pub use leader::{FollowerProgress, LeaderReplica, LeaderReplicaSummary};
pub use replica::{PartitionReplicaSet, ReplicaInfo, ReplicaRole, ReplicaState};
pub use replica_fetcher::{
    FetchCycleResult, PartitionFetcherState, ReplicaFetcher, ReplicaFetcherConfig,
    ReplicaFetcherSummary, ReplicatedBatch, ReplicationFetchRequest, ReplicationFetchResponse,
};
pub use replica_manager::{ReplicaManager, ReplicaManagerSummary, ReplicaPartitionKey};
