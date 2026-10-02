//! Leader Election — Leader 选举引擎
//!
//! 实现 Kafka 风格的 Leader 选举策略:
//!
//! 1. **Preferred Leader Election**: 优先选择 replica_assignment 中的第一个 Broker
//! 2. **ISR-based Election**: 从 ISR 集合中选择 Leader (首选 preferred leader)
//! 3. **Unclean Election**: ISR 为空时，从非 ISR 副本中选择 (允许数据丢失)
//!
//! ```text
//! Leader 选举流程:
//!
//! ISR 非空? ──Yes──→ ISR 中包含 preferred leader?
//!     │                       │
//!     │                    Yes ─→ 选举 preferred leader
//!     │                    No  ─→ 选举 ISR 中第一个 (按 assignment 顺序)
//!     │
//!    No ──→ unclean_leader_election_enabled?
//!                 │
//!              Yes ─→ 从 replica_assignment 中选择第一个存活的非 ISR 副本
//!              No  ─→ 选举失败 (Partition 不可用)
//! ```

use std::collections::HashSet;

use rk_core::error::{RkError, Result};
use rk_core::types::BrokerId;
use tracing::{info, warn};

use crate::replica::PartitionReplicaSet;

// ─── 选举配置 ────────────────────────────────────────────────────────

/// Leader 选举配置
#[derive(Debug, Clone)]
pub struct ElectionConfig {
    /// 是否允许 unclean leader election (ISR 为空时从非 ISR 中选)
    pub unclean_leader_election_enabled: bool,
}

impl Default for ElectionConfig {
    fn default() -> Self {
        Self {
            unclean_leader_election_enabled: false,
        }
    }
}

// ─── 选举结果 ────────────────────────────────────────────────────────

/// Leader 选举策略
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ElectionStrategy {
    /// Preferred leader 当选 (最佳情况)
    Preferred,
    /// ISR 中非 preferred 的副本当选 (ISR 非空但 preferred 不在)
    IsrMember,
    /// Unclean election (ISR 为空，从非 ISR 中选)
    Unclean,
}

impl std::fmt::Display for ElectionStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ElectionStrategy::Preferred => write!(f, "Preferred"),
            ElectionStrategy::IsrMember => write!(f, "ISRMember"),
            ElectionStrategy::Unclean => write!(f, "Unclean"),
        }
    }
}

/// Leader 选举结果
#[derive(Debug, Clone)]
pub struct ElectionResult {
    /// 新 Leader 的 Broker ID
    pub new_leader: BrokerId,
    /// 选举策略
    pub strategy: ElectionStrategy,
    /// 新 Leader Epoch (旧 epoch + 1)
    pub new_leader_epoch: i32,
    /// 新的 ISR 集合 (election 后可能缩减)
    pub new_isr: HashSet<BrokerId>,
    /// 是否有数据丢失风险 (unclean election)
    pub data_loss_risk: bool,
}

/// Leader 选举失败原因
#[derive(Debug, Clone)]
pub enum ElectionFailure {
    /// ISR 为空且 unclean election 被禁用
    IsrEmptyUncleanDisabled,
    /// 没有可用的副本
    NoReplicasAvailable,
    /// 所有副本都已失败
    AllReplicasFailed,
}

impl std::fmt::Display for ElectionFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ElectionFailure::IsrEmptyUncleanDisabled => {
                write!(f, "ISR is empty and unclean leader election is disabled")
            }
            ElectionFailure::NoReplicasAvailable => write!(f, "No replicas available"),
            ElectionFailure::AllReplicasFailed => write!(f, "All replicas have failed"),
        }
    }
}

// ─── LeaderElector ───────────────────────────────────────────────────

/// Leader 选举器
///
/// 根据当前副本集状态和配置执行 Leader 选举。
pub struct LeaderElector {
    config: ElectionConfig,
}

impl LeaderElector {
    /// 创建选举器
    pub fn new(config: ElectionConfig) -> Self {
        Self { config }
    }

    /// 创建默认配置的选举器
    pub fn with_defaults() -> Self {
        Self::new(ElectionConfig::default())
    }

    /// 获取配置
    pub fn config(&self) -> &ElectionConfig {
        &self.config
    }

    /// 执行 Leader 选举
    ///
    /// 当当前 Leader 失效时调用此方法。
    pub fn elect_leader(
        &self,
        replica_set: &PartitionReplicaSet,
        current_leader_epoch: i32,
        alive_brokers: &HashSet<BrokerId>,
    ) -> std::result::Result<ElectionResult, ElectionFailure> {
        let isr = &replica_set.isr;
        let assignment = &replica_set.replica_assignment;
        let preferred = replica_set.preferred_leader();

        // 1. ISR 非空: 优先选择 preferred leader
        if !isr.is_empty() {
            // 过滤 ISR 中存活的 Broker
            let alive_isr: Vec<BrokerId> = assignment
                .iter()
                .filter(|b| isr.contains(b) && alive_brokers.contains(b))
                .copied()
                .collect();

            if !alive_isr.is_empty() {
                let new_leader = alive_isr[0];
                let strategy = if Some(new_leader) == preferred && isr.contains(&new_leader) {
                    ElectionStrategy::Preferred
                } else {
                    ElectionStrategy::IsrMember
                };

                if strategy == ElectionStrategy::Preferred {
                    info!(
                        topic = %replica_set.topic.0,
                        partition = replica_set.partition.0,
                        new_leader = new_leader.0,
                        epoch = current_leader_epoch + 1,
                        "Preferred leader elected"
                    );
                } else {
                    info!(
                        topic = %replica_set.topic.0,
                        partition = replica_set.partition.0,
                        new_leader = new_leader.0,
                        epoch = current_leader_epoch + 1,
                        "ISR member elected as leader (not preferred)"
                    );
                }

                return Ok(ElectionResult {
                    new_leader,
                    strategy,
                    new_leader_epoch: current_leader_epoch + 1,
                    new_isr: alive_isr.iter().copied().collect(),
                    data_loss_risk: false,
                });
            }
        }

        // 2. ISR 为空或 ISR 中无存活 Broker
        if !self.config.unclean_leader_election_enabled {
            warn!(
                topic = %replica_set.topic.0,
                partition = replica_set.partition.0,
                "ISR empty, unclean election disabled — partition unavailable"
            );
            return Err(ElectionFailure::IsrEmptyUncleanDisabled);
        }

        // 3. Unclean election: 从 replica_assignment 中选择第一个存活的非 ISR 副本
        warn!(
            topic = %replica_set.topic.0,
            partition = replica_set.partition.0,
            "Performing UNCLEAN leader election — data loss possible!"
        );

        for &broker_id in assignment {
            if alive_brokers.contains(&broker_id) {
                info!(
                    topic = %replica_set.topic.0,
                    partition = replica_set.partition.0,
                    new_leader = broker_id.0,
                    epoch = current_leader_epoch + 1,
                    "Unclean leader elected"
                );

                return Ok(ElectionResult {
                    new_leader: broker_id,
                    strategy: ElectionStrategy::Unclean,
                    new_leader_epoch: current_leader_epoch + 1,
                    new_isr: HashSet::new(), // unclean election → 空 ISR
                    data_loss_risk: true,
                });
            }
        }

        Err(ElectionFailure::NoReplicasAvailable)
    }

    /// 检查是否应该执行 preferred leader election
    ///
    /// 条件: preferred leader 在 ISR 中，且当前 leader 不是 preferred
    pub fn should_do_preferred_election(
        &self,
        replica_set: &PartitionReplicaSet,
    ) -> bool {
        if let Some(preferred) = replica_set.preferred_leader() {
            replica_set.is_in_isr(preferred) && !replica_set.is_preferred_leader()
        } else {
            false
        }
    }

    /// 执行 preferred leader election (主动切换回 preferred)
    pub fn preferred_election(
        &self,
        replica_set: &PartitionReplicaSet,
        current_leader_epoch: i32,
    ) -> Result<ElectionResult> {
        let preferred = replica_set
            .preferred_leader()
            .ok_or_else(|| RkError::Protocol("No preferred leader in assignment".to_string()))?;

        if !replica_set.is_in_isr(preferred) {
            return Err(RkError::Protocol(format!(
                "Preferred leader {} is not in ISR",
                preferred.0
            )));
        }

        Ok(ElectionResult {
            new_leader: preferred,
            strategy: ElectionStrategy::Preferred,
            new_leader_epoch: current_leader_epoch + 1,
            new_isr: replica_set.isr.clone(),
            data_loss_risk: false,
        })
    }
}

// ─── Tests ───────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replica::PartitionReplicaSet;
    use rk_core::types::{BrokerId, PartitionId, TopicName};

    fn broker(id: i32) -> BrokerId {
        BrokerId(id)
    }

    fn topic(name: &str) -> TopicName {
        TopicName(name.to_string())
    }

    fn partition(id: i32) -> PartitionId {
        PartitionId(id)
    }

    fn alive_set(ids: Vec<i32>) -> HashSet<BrokerId> {
        ids.into_iter().map(BrokerId).collect()
    }

    fn make_replica_set() -> PartitionReplicaSet {
        PartitionReplicaSet::new(
            topic("test"),
            partition(0),
            broker(1),
            vec![broker(1), broker(2), broker(3)],
        )
    }

    #[test]
    fn test_election_config_default() {
        let config = ElectionConfig::default();
        assert!(!config.unclean_leader_election_enabled);
    }

    #[test]
    fn test_election_strategy_display() {
        assert_eq!(format!("{}", ElectionStrategy::Preferred), "Preferred");
        assert_eq!(format!("{}", ElectionStrategy::IsrMember), "ISRMember");
        assert_eq!(format!("{}", ElectionStrategy::Unclean), "Unclean");
    }

    #[test]
    fn test_election_failure_display() {
        assert!(format!("{}", ElectionFailure::IsrEmptyUncleanDisabled).contains("ISR is empty"));
        assert!(format!("{}", ElectionFailure::NoReplicasAvailable).contains("No replicas"));
    }

    #[test]
    fn test_preferred_leader_election() {
        let elector = LeaderElector::with_defaults();
        let prs = make_replica_set();
        let alive = alive_set(vec![1, 2, 3]);

        let result = elector.elect_leader(&prs, 0, &alive).unwrap();
        assert_eq!(result.new_leader, broker(1)); // preferred = broker(1)
        assert_eq!(result.strategy, ElectionStrategy::Preferred);
        assert_eq!(result.new_leader_epoch, 1);
        assert!(!result.data_loss_risk);
        assert_eq!(result.new_isr.len(), 3);
    }

    #[test]
    fn test_isr_member_election_preferred_down() {
        let elector = LeaderElector::with_defaults();
        let prs = make_replica_set();
        // broker(1) 宕机，不在 alive 中
        let alive = alive_set(vec![2, 3]);

        let result = elector.elect_leader(&prs, 5, &alive).unwrap();
        assert_eq!(result.new_leader, broker(2)); // 按 assignment 顺序，broker(2) 是 ISR 中第一个存活的
        assert_eq!(result.strategy, ElectionStrategy::IsrMember);
        assert_eq!(result.new_leader_epoch, 6);
        assert!(!result.data_loss_risk);
    }

    #[test]
    fn test_isr_empty_unclean_disabled() {
        let elector = LeaderElector::with_defaults();
        let mut prs = make_replica_set();
        // 清空 ISR
        prs.isr.clear();
        let alive = alive_set(vec![1, 2, 3]);

        let result = elector.elect_leader(&prs, 0, &alive);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ElectionFailure::IsrEmptyUncleanDisabled));
    }

    #[test]
    fn test_unclean_leader_election() {
        let config = ElectionConfig {
            unclean_leader_election_enabled: true,
        };
        let elector = LeaderElector::new(config);

        let mut prs = make_replica_set();
        // 清空 ISR (所有 ISR 成员数据不一致)
        prs.isr.clear();
        let alive = alive_set(vec![1, 2, 3]);

        let result = elector.elect_leader(&prs, 10, &alive).unwrap();
        assert_eq!(result.new_leader, broker(1)); // assignment 中第一个存活的
        assert_eq!(result.strategy, ElectionStrategy::Unclean);
        assert_eq!(result.new_leader_epoch, 11);
        assert!(result.data_loss_risk);
        assert!(result.new_isr.is_empty()); // unclean → 空 ISR
    }

    #[test]
    fn test_unclean_election_no_alive() {
        let config = ElectionConfig {
            unclean_leader_election_enabled: true,
        };
        let elector = LeaderElector::new(config);
        let mut prs = make_replica_set();
        prs.isr.clear();
        let alive = alive_set(vec![]); // 全部宕机

        let result = elector.elect_leader(&prs, 0, &alive);
        assert!(result.is_err());
    }

    #[test]
    fn test_should_do_preferred_election() {
        let elector = LeaderElector::with_defaults();
        let mut prs = make_replica_set();

        // 当前 leader = broker(1) = preferred → 不需要切换
        assert!(!elector.should_do_preferred_election(&prs));

        // 模拟 leader 切换到 broker(2)
        prs.leader_id = broker(2);
        assert!(elector.should_do_preferred_election(&prs));

        // preferred 不在 ISR → 不需要
        prs.isr.remove(&broker(1));
        assert!(!elector.should_do_preferred_election(&prs));
    }

    #[test]
    fn test_preferred_election_manual() {
        let elector = LeaderElector::with_defaults();
        let mut prs = make_replica_set();
        prs.leader_id = broker(2); // 当前 leader 不是 preferred

        let result = elector.preferred_election(&prs, 5).unwrap();
        assert_eq!(result.new_leader, broker(1));
        assert_eq!(result.strategy, ElectionStrategy::Preferred);
        assert_eq!(result.new_leader_epoch, 6);
    }

    #[test]
    fn test_preferred_election_not_in_isr() {
        let elector = LeaderElector::with_defaults();
        let mut prs = make_replica_set();
        prs.leader_id = broker(2);
        prs.isr.remove(&broker(1)); // preferred 不在 ISR

        assert!(elector.preferred_election(&prs, 5).is_err());
    }

    #[test]
    fn test_election_with_partial_alive() {
        let elector = LeaderElector::with_defaults();
        let prs = make_replica_set();
        // 只有 broker(3) 存活
        let alive = alive_set(vec![3]);

        let result = elector.elect_leader(&prs, 0, &alive).unwrap();
        assert_eq!(result.new_leader, broker(3));
        assert_eq!(result.strategy, ElectionStrategy::IsrMember);
        assert_eq!(result.new_isr.len(), 1);
        assert!(result.new_isr.contains(&broker(3)));
    }
}
