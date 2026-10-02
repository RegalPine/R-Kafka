//! Replica Placement — 高级副本放置策略
//!
//! 基于机架拓扑信息，实现智能副本放置:
//!
//! ```text
//! 放置流程:
//!
//! 1. 获取集群机架拓扑 (RackTopology)
//! 2. 选择放置策略 (PlacementStrategy)
//! 3. 生成放置计划 (PlacementPlan)
//! 4. 验证约束 (跨机架 + 负载均衡)
//! 5. 输出副本分配 (PartitionAssignment)
//!
//! 策略:
//! - RackAwareSpread: 跨机架均匀分布
//! - LeaderBalanced: 首选 Leader 跨机架均衡
//! - LoadAwareed: 考虑 Broker 当前负载
//! ```

use std::collections::{BTreeMap, HashMap, HashSet};

use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::rack_awareness::{RackId, RackTopology};

// ─── 放置策略 ────────────────────────────────────────────────────────

/// 副本放置策略
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum PlacementStrategy {
    /// 跨机架均匀分布 (默认)
    #[default]
    RackAwareSpread,
    /// 首选 Leader 跨机架均衡
    LeaderBalanced,
    /// 考虑 Broker 当前负载
    LoadAwareed,
}

// ─── PlacementPlan ───────────────────────────────────────────────────

/// 副本放置计划
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlacementPlan {
    /// Topic 名称
    pub topic_name: String,
    /// Partition 数量
    pub partition_count: u32,
    /// 副本因子
    pub replication_factor: u16,
    /// 使用的放置策略
    pub strategy: PlacementStrategy,
    /// Partition 编号 → 副本列表 (第一个是首选 Leader)
    pub assignments: HashMap<i32, Vec<i32>>,
    /// 放置统计
    pub stats: PlacementStats,
}

/// 放置统计
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PlacementStats {
    /// 使用的机架数量
    pub racks_used: usize,
    /// 是否满足跨机架约束
    pub rack_constraint_satisfied: bool,
    /// 每个 Broker 的 partition 数量
    pub broker_load: BTreeMap<i32, u32>,
    /// 负载标准差 (越小越均衡)
    pub load_stddev: f64,
    /// 首选 Leader 分布
    pub leader_distribution: BTreeMap<i32, u32>,
    /// 是否发生回退 (机架不足回退到轮询)
    pub fell_back: bool,
}

impl PlacementPlan {
    /// 获取指定 partition 的副本列表
    pub fn replicas_for(&self, partition_id: i32) -> Option<&Vec<i32>> {
        self.assignments.get(&partition_id)
    }

    /// 获取指定 partition 的首选 Leader
    pub fn preferred_leader(&self, partition_id: i32) -> Option<i32> {
        self.assignments
            .get(&partition_id)
            .and_then(|r| r.first().copied())
    }

    /// 获取所有涉及的 Broker ID
    pub fn involved_brokers(&self) -> Vec<i32> {
        let mut brokers = HashSet::new();
        for replicas in self.assignments.values() {
            for &bid in replicas {
                brokers.insert(bid);
            }
        }
        let mut v: Vec<i32> = brokers.into_iter().collect();
        v.sort();
        v
    }

    /// 获取所有涉及的机架
    pub fn involved_racks(&self, topology: &RackTopology) -> Vec<RackId> {
        let mut racks = HashSet::new();
        for replicas in self.assignments.values() {
            for &bid in replicas {
                if let Some(rack) = topology.rack_for_broker(bid) {
                    racks.insert(rack.clone());
                }
            }
        }
        let mut v: Vec<RackId> = racks.into_iter().collect();
        v.sort();
        v
    }
}

// ─── ReplicaPlacer ───────────────────────────────────────────────────

/// 副本放置器
pub struct ReplicaPlacer;

impl ReplicaPlacer {
    /// 执行副本放置
    ///
    /// # Arguments
    /// * `topic_name` - Topic 名称
    /// * `partition_count` - Partition 数量
    /// * `replication_factor` - 副本因子
    /// * `topology` - 集群机架拓扑
    /// * `strategy` - 放置策略
    pub fn place(
        topic_name: &str,
        partition_count: u32,
        replication_factor: u16,
        topology: &RackTopology,
        strategy: PlacementStrategy,
    ) -> Result<PlacementPlan, PlacementError> {
        if topology.total_broker_count() == 0 {
            return Err(PlacementError::NoBrokersAvailable);
        }

        let rf = replication_factor as usize;
        if rf > topology.total_broker_count() {
            return Err(PlacementError::InsufficientBrokers {
                required: rf,
                available: topology.total_broker_count(),
            });
        }

        if partition_count == 0 {
            return Err(PlacementError::InvalidPartitions(0));
        }

        match strategy {
            PlacementStrategy::RackAwareSpread => {
                Self::place_rack_aware_spread(topic_name, partition_count, rf, topology)
            }
            PlacementStrategy::LeaderBalanced => {
                Self::place_leader_balanced(topic_name, partition_count, rf, topology)
            }
            PlacementStrategy::LoadAwareed => {
                Self::place_load_awareed(topic_name, partition_count, rf, topology)
            }
        }
    }

    /// 跨机架均匀分布
    fn place_rack_aware_spread(
        topic_name: &str,
        partition_count: u32,
        replication_factor: usize,
        topology: &RackTopology,
    ) -> Result<PlacementPlan, PlacementError> {
        let racks = topology.rack_ids();
        let rack_count = racks.len();
        let mut fell_back = false;

        let mut assignments = HashMap::new();

        if rack_count < replication_factor {
            // 机架不足，回退到简单轮询
            warn!(
                rack_count = rack_count,
                replication_factor = replication_factor,
                "Not enough racks, falling back to round-robin"
            );
            fell_back = true;
            let all_brokers = Self::sorted_alive_brokers(topology);
            let broker_count = all_brokers.len();

            for pid in 0..partition_count {
                let mut replicas = Vec::with_capacity(replication_factor);
                let start = (pid as usize) % broker_count;
                for offset in 0..replication_factor {
                    let idx = (start + offset) % broker_count;
                    replicas.push(all_brokers[idx]);
                }
                assignments.insert(pid as i32, replicas);
            }
        } else {
            // 跨机架分布
            for pid in 0..partition_count {
                let mut replicas = Vec::with_capacity(replication_factor);
                let rack_start = (pid as usize) % rack_count;

                for offset in 0..replication_factor {
                    let rack_idx = (rack_start + offset) % rack_count;
                    let rack = &racks[rack_idx];
                    let alive = topology.alive_brokers_in_rack(rack);

                    if let Some(broker) = alive.get((pid as usize) % alive.len()) {
                        replicas.push(broker.broker_id);
                    }
                }

                assignments.insert(pid as i32, replicas);
            }
        }

        let stats = Self::compute_stats(topology, &assignments, fell_back);

        info!(
            topic = topic_name,
            partitions = partition_count,
            replication_factor = replication_factor,
            racks_used = stats.racks_used,
            rack_satisfied = stats.rack_constraint_satisfied,
            "Replica placement (rack-aware-spread) completed"
        );

        Ok(PlacementPlan {
            topic_name: topic_name.to_string(),
            partition_count,
            replication_factor: replication_factor as u16,
            strategy: PlacementStrategy::RackAwareSpread,
            assignments,
            stats,
        })
    }

    /// 首选 Leader 跨机架均衡
    fn place_leader_balanced(
        topic_name: &str,
        partition_count: u32,
        replication_factor: usize,
        topology: &RackTopology,
    ) -> Result<PlacementPlan, PlacementError> {
        // 先做基本的 rack-aware 分配
        let mut plan = Self::place_rack_aware_spread(
            topic_name,
            partition_count,
            replication_factor,
            topology,
        )?;

        // 然后均衡首选 Leader 分布
        let racks = topology.rack_ids();
        let rack_count = racks.len();

        if rack_count >= replication_factor {
            // 统计当前 leader 分布
            let mut leader_counts: HashMap<i32, u32> = HashMap::new();
            for replicas in plan.assignments.values() {
                if let Some(&leader) = replicas.first() {
                    *leader_counts.entry(leader).or_insert(0) += 1;
                }
            }

            // 尝试通过旋转副本列表来均衡 leader
            // 对于每个 partition，尝试将 leader 移到负载最低的 broker
            for pid in 0..partition_count {
                if let Some(replicas) = plan.assignments.get_mut(&(pid as i32)) {
                    if replicas.len() <= 1 {
                        continue;
                    }

                    // 找到当前 replicas 中负载最低的 broker
                    let min_load_broker = replicas
                        .iter()
                        .min_by_key(|&&bid| leader_counts.get(&bid).copied().unwrap_or(0))
                        .copied();

                    if let Some(target) = min_load_broker {
                        // 将 target 旋转到首位
                        if let Some(pos) = replicas.iter().position(|&b| b == target) {
                            if pos > 0 {
                                // 更新 leader counts
                                if let Some(&old_leader) = replicas.first() {
                                    if let Some(count) = leader_counts.get_mut(&old_leader) {
                                        *count = count.saturating_sub(1);
                                    }
                                }
                                replicas.rotate_right(pos);
                                *leader_counts.entry(target).or_insert(0) += 1;
                            }
                        }
                    }
                }
            }

            // 重新计算统计
            plan.stats = Self::compute_stats(topology, &plan.assignments, plan.stats.fell_back);
        }

        plan.strategy = PlacementStrategy::LeaderBalanced;

        info!(
            topic = topic_name,
            leaders = plan.stats.leader_distribution.len(),
            "Replica placement (leader-balanced) completed"
        );

        Ok(plan)
    }

    /// 负载感知放置
    fn place_load_awareed(
        topic_name: &str,
        partition_count: u32,
        replication_factor: usize,
        topology: &RackTopology,
    ) -> Result<PlacementPlan, PlacementError> {
        let racks = topology.rack_ids();
        let rack_count = racks.len();
        let mut fell_back = false;

        // 跟踪每个 broker 的负载
        let mut broker_loads: HashMap<i32, u32> = HashMap::new();
        for rack_id in &racks {
            for broker in topology.alive_brokers_in_rack(rack_id) {
                broker_loads.insert(broker.broker_id, broker.partition_load);
            }
        }

        let mut assignments = HashMap::new();

        if rack_count < replication_factor {
            fell_back = true;
            // 回退: 按负载排序选择
            let mut sorted: Vec<(i32, u32)> = broker_loads.iter().map(|(&k, &v)| (k, v)).collect();
            sorted.sort_by_key(|a| a.1);

            for pid in 0..partition_count {
                let mut replicas = Vec::with_capacity(replication_factor);
                let broker_count = sorted.len();
                let start = (pid as usize) % broker_count;
                for offset in 0..replication_factor {
                    let idx = (start + offset) % broker_count;
                    replicas.push(sorted[idx].0);
                }
                assignments.insert(pid as i32, replicas);
            }
        } else {
            // 跨机架 + 负载感知
            for pid in 0..partition_count {
                let mut replicas = Vec::with_capacity(replication_factor);
                let rack_start = (pid as usize) % rack_count;

                for offset in 0..replication_factor {
                    let rack_idx = (rack_start + offset) % rack_count;
                    let rack = &racks[rack_idx];
                    let alive = topology.alive_brokers_in_rack(rack);

                    // 在该机架中选择负载最低的 broker
                    let best = alive
                        .iter()
                        .min_by_key(|b| broker_loads.get(&b.broker_id).copied().unwrap_or(0));

                    if let Some(broker) = best {
                        replicas.push(broker.broker_id);
                        // 增加负载计数
                        *broker_loads.entry(broker.broker_id).or_insert(0) += 1;
                    }
                }

                assignments.insert(pid as i32, replicas);
            }
        }

        let stats = Self::compute_stats(topology, &assignments, fell_back);

        info!(
            topic = topic_name,
            partitions = partition_count,
            replication_factor = replication_factor,
            "Replica placement (load-aware) completed"
        );

        Ok(PlacementPlan {
            topic_name: topic_name.to_string(),
            partition_count,
            replication_factor: replication_factor as u16,
            strategy: PlacementStrategy::LoadAwareed,
            assignments,
            stats,
        })
    }

    // ─── 辅助方法 ────────────────────────────────────────────────────

    /// 获取存活的 Broker ID 列表 (排序)
    fn sorted_alive_brokers(topology: &RackTopology) -> Vec<i32> {
        let mut brokers: Vec<i32> = Vec::new();
        for rack_id in topology.rack_ids() {
            for broker in topology.alive_brokers_in_rack(&rack_id) {
                brokers.push(broker.broker_id);
            }
        }
        brokers.sort();
        brokers
    }

    /// 计算放置统计
    fn compute_stats(
        topology: &RackTopology,
        assignments: &HashMap<i32, Vec<i32>>,
        fell_back: bool,
    ) -> PlacementStats {
        // Broker 负载
        let mut broker_load: BTreeMap<i32, u32> = BTreeMap::new();
        for replicas in assignments.values() {
            for &bid in replicas {
                *broker_load.entry(bid).or_insert(0) += 1;
            }
        }

        // 负载标准差
        let loads: Vec<f64> = broker_load.values().map(|&c| c as f64).collect();
        let n = loads.len() as f64;
        let stddev = if n > 0.0 {
            let mean = loads.iter().sum::<f64>() / n;
            let variance = loads.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / n;
            variance.sqrt()
        } else {
            0.0
        };

        // Leader 分布
        let mut leader_dist: BTreeMap<i32, u32> = BTreeMap::new();
        for replicas in assignments.values() {
            if let Some(&leader) = replicas.first() {
                *leader_dist.entry(leader).or_insert(0) += 1;
            }
        }

        // 机架约束验证
        let violations = topology.validate_replica_spread(assignments);
        let rack_satisfied = violations.is_empty();

        // 使用的机架数
        let mut racks_used = HashSet::new();
        for replicas in assignments.values() {
            for &bid in replicas {
                if let Some(rack) = topology.rack_for_broker(bid) {
                    racks_used.insert(rack.clone());
                }
            }
        }

        PlacementStats {
            racks_used: racks_used.len(),
            rack_constraint_satisfied: rack_satisfied,
            broker_load,
            load_stddev: stddev,
            leader_distribution: leader_dist,
            fell_back,
        }
    }
}

// ─── Reassignment ────────────────────────────────────────────────────

/// 副本重分配计划
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReassignmentPlan {
    /// Topic 名称
    pub topic_name: String,
    /// 变更列表: partition_id → (旧副本, 新副本)
    pub changes: HashMap<i32, (Vec<i32>, Vec<i32>)>,
    /// 是否需要数据迁移
    pub requires_data_migration: bool,
}

impl ReassignmentPlan {
    /// 计算从旧分配到新分配需要的数据迁移
    pub fn compute_migration_count(&self) -> usize {
        self.changes
            .values()
            .filter(|(old, new)| {
                // 如果有新 broker 不在旧列表中，需要迁移
                let old_set: HashSet<i32> = old.iter().copied().collect();
                new.iter().any(|b| !old_set.contains(b))
            })
            .count()
    }
}

/// 计算副本重分配计划
pub fn compute_reassignment(
    topic_name: &str,
    current: &HashMap<i32, Vec<i32>>,
    target: &HashMap<i32, Vec<i32>>,
) -> ReassignmentPlan {
    let mut changes = HashMap::new();
    let mut requires_migration = false;

    for (pid, target_replicas) in target {
        let current_replicas = current.get(pid).cloned().unwrap_or_default();
        if current_replicas != *target_replicas {
            changes.insert(*pid, (current_replicas.clone(), target_replicas.clone()));
            // 检查是否需要数据迁移
            let current_set: HashSet<i32> = current_replicas.iter().copied().collect();
            if target_replicas.iter().any(|b| !current_set.contains(b)) {
                requires_migration = true;
            }
        }
    }

    ReassignmentPlan {
        topic_name: topic_name.to_string(),
        changes,
        requires_data_migration: requires_migration,
    }
}

// ─── 错误类型 ────────────────────────────────────────────────────────

/// 副本放置错误
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlacementError {
    /// 没有可用 Broker
    NoBrokersAvailable,
    /// Broker 数量不足
    InsufficientBrokers { required: usize, available: usize },
    /// 无效的 Partition 数量
    InvalidPartitions(u32),
}

impl std::fmt::Display for PlacementError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PlacementError::NoBrokersAvailable => {
                write!(f, "No brokers available for replica placement")
            }
            PlacementError::InsufficientBrokers {
                required,
                available,
            } => {
                write!(
                    f,
                    "Need {} brokers but only {} available",
                    required, available
                )
            }
            PlacementError::InvalidPartitions(n) => {
                write!(f, "Invalid partition count: {}", n)
            }
        }
    }
}

impl std::error::Error for PlacementError {}

// ─── 单元测试 ────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rack_awareness::BrokerRackInfo;

    fn make_topology_3rack() -> RackTopology {
        let brokers = vec![
            BrokerRackInfo {
                broker_id: 1,
                rack_id: RackId::new("rack-a"),
                host: "192.168.1.1".into(),
                is_alive: true,
                partition_load: 0,
            },
            BrokerRackInfo {
                broker_id: 2,
                rack_id: RackId::new("rack-a"),
                host: "192.168.1.2".into(),
                is_alive: true,
                partition_load: 0,
            },
            BrokerRackInfo {
                broker_id: 3,
                rack_id: RackId::new("rack-b"),
                host: "192.168.1.3".into(),
                is_alive: true,
                partition_load: 0,
            },
            BrokerRackInfo {
                broker_id: 4,
                rack_id: RackId::new("rack-b"),
                host: "192.168.1.4".into(),
                is_alive: true,
                partition_load: 0,
            },
            BrokerRackInfo {
                broker_id: 5,
                rack_id: RackId::new("rack-c"),
                host: "192.168.1.5".into(),
                is_alive: true,
                partition_load: 0,
            },
            BrokerRackInfo {
                broker_id: 6,
                rack_id: RackId::new("rack-c"),
                host: "192.168.1.6".into(),
                is_alive: true,
                partition_load: 0,
            },
        ];
        RackTopology::from_brokers(&brokers)
    }

    // ─── RackAwareSpread 测试 ───────────────────────────────────────

    #[test]
    fn test_rack_aware_spread_3rack_3replica() {
        let topo = make_topology_3rack();
        let plan = ReplicaPlacer::place("orders", 6, 3, &topo, PlacementStrategy::RackAwareSpread)
            .unwrap();

        assert_eq!(plan.assignments.len(), 6);
        assert!(
            plan.stats.rack_constraint_satisfied,
            "Rack constraint should be satisfied"
        );
        assert_eq!(plan.stats.racks_used, 3);

        // 每个 partition 的 3 个副本应该在不同机架上
        for (pid, replicas) in &plan.assignments {
            assert_eq!(
                replicas.len(),
                3,
                "Partition {} should have 3 replicas",
                pid
            );
            assert!(
                topo.all_on_different_racks(replicas),
                "Partition {} replicas {:?} should be on different racks",
                pid,
                replicas
            );
        }
    }

    #[test]
    fn test_rack_aware_spread_rf2() {
        let topo = make_topology_3rack();
        let plan = ReplicaPlacer::place("events", 12, 2, &topo, PlacementStrategy::RackAwareSpread)
            .unwrap();

        assert_eq!(plan.assignments.len(), 12);
        assert!(plan.stats.rack_constraint_satisfied);

        for (_pid, replicas) in &plan.assignments {
            assert_eq!(replicas.len(), 2);
            assert!(topo.all_on_different_racks(replicas));
        }
    }

    #[test]
    fn test_rack_aware_spread_rf1() {
        let topo = make_topology_3rack();
        let plan = ReplicaPlacer::place("simple", 6, 1, &topo, PlacementStrategy::RackAwareSpread)
            .unwrap();

        assert_eq!(plan.assignments.len(), 6);
        for (_, replicas) in &plan.assignments {
            assert_eq!(replicas.len(), 1);
        }
    }

    // ─── LeaderBalanced 测试 ────────────────────────────────────────

    #[test]
    fn test_leader_balanced() {
        let topo = make_topology_3rack();
        let plan = ReplicaPlacer::place("balanced", 6, 3, &topo, PlacementStrategy::LeaderBalanced)
            .unwrap();

        assert_eq!(plan.assignments.len(), 6);
        // Leader 应该分布在多个 broker 上
        assert!(
            plan.stats.leader_distribution.len() > 1,
            "Leaders should be distributed"
        );
    }

    #[test]
    fn test_leader_balanced_even_distribution() {
        let topo = make_topology_3rack();
        let plan =
            ReplicaPlacer::place("even", 6, 1, &topo, PlacementStrategy::LeaderBalanced).unwrap();

        // RF=1 时，每个 broker 应该有 1 个 leader (6 brokers, 6 partitions)
        let max_leaders = plan
            .stats
            .leader_distribution
            .values()
            .max()
            .copied()
            .unwrap_or(0);
        let min_leaders = plan
            .stats
            .leader_distribution
            .values()
            .min()
            .copied()
            .unwrap_or(0);
        assert!(
            max_leaders - min_leaders <= 1,
            "Leader distribution should be nearly even: max={}, min={}",
            max_leaders,
            min_leaders
        );
    }

    // ─── LoadAwareed 测试 ───────────────────────────────────────────

    #[test]
    fn test_load_awareed_basic() {
        let topo = make_topology_3rack();
        let plan =
            ReplicaPlacer::place("loaded", 6, 3, &topo, PlacementStrategy::LoadAwareed).unwrap();

        assert_eq!(plan.assignments.len(), 6);
        assert!(plan.stats.rack_constraint_satisfied);
    }

    #[test]
    fn test_load_awareed_prefers_low_load() {
        // 设置不同负载
        let brokers = vec![
            BrokerRackInfo {
                broker_id: 1,
                rack_id: RackId::new("rack-a"),
                host: "192.168.1.1".into(),
                is_alive: true,
                partition_load: 100,
            },
            BrokerRackInfo {
                broker_id: 2,
                rack_id: RackId::new("rack-a"),
                host: "192.168.1.2".into(),
                is_alive: true,
                partition_load: 0,
            },
            BrokerRackInfo {
                broker_id: 3,
                rack_id: RackId::new("rack-b"),
                host: "192.168.1.3".into(),
                is_alive: true,
                partition_load: 50,
            },
            BrokerRackInfo {
                broker_id: 4,
                rack_id: RackId::new("rack-b"),
                host: "192.168.1.4".into(),
                is_alive: true,
                partition_load: 0,
            },
            BrokerRackInfo {
                broker_id: 5,
                rack_id: RackId::new("rack-c"),
                host: "192.168.1.5".into(),
                is_alive: true,
                partition_load: 80,
            },
            BrokerRackInfo {
                broker_id: 6,
                rack_id: RackId::new("rack-c"),
                host: "192.168.1.6".into(),
                is_alive: true,
                partition_load: 0,
            },
        ];
        let topo = RackTopology::from_brokers(&brokers);
        let plan =
            ReplicaPlacer::place("test", 3, 3, &topo, PlacementStrategy::LoadAwareed).unwrap();

        // 低负载 broker (2, 4, 6) 应该获得更多 partition
        let low_load_total: u32 = plan.stats.broker_load.get(&2).copied().unwrap_or(0)
            + plan.stats.broker_load.get(&4).copied().unwrap_or(0)
            + plan.stats.broker_load.get(&6).copied().unwrap_or(0);
        let high_load_total: u32 = plan.stats.broker_load.get(&1).copied().unwrap_or(0)
            + plan.stats.broker_load.get(&3).copied().unwrap_or(0)
            + plan.stats.broker_load.get(&5).copied().unwrap_or(0);

        assert!(
            low_load_total >= high_load_total,
            "Low-load brokers should get more partitions: low={}, high={}",
            low_load_total,
            high_load_total
        );
    }

    // ─── 错误处理测试 ────────────────────────────────────────────────

    #[test]
    fn test_no_brokers() {
        let topo = RackTopology::new();
        let result = ReplicaPlacer::place("test", 3, 1, &topo, PlacementStrategy::RackAwareSpread);
        assert_eq!(result, Err(PlacementError::NoBrokersAvailable));
    }

    #[test]
    fn test_insufficient_brokers() {
        let brokers = vec![BrokerRackInfo {
            broker_id: 1,
            rack_id: RackId::new("rack-a"),
            host: "192.168.1.1".into(),
            is_alive: true,
            partition_load: 0,
        }];
        let topo = RackTopology::from_brokers(&brokers);
        let result = ReplicaPlacer::place("test", 3, 3, &topo, PlacementStrategy::RackAwareSpread);
        assert_eq!(
            result,
            Err(PlacementError::InsufficientBrokers {
                required: 3,
                available: 1
            })
        );
    }

    #[test]
    fn test_zero_partitions() {
        let topo = make_topology_3rack();
        let result = ReplicaPlacer::place("test", 0, 3, &topo, PlacementStrategy::RackAwareSpread);
        assert_eq!(result, Err(PlacementError::InvalidPartitions(0)));
    }

    // ─── PlacementPlan 测试 ─────────────────────────────────────────

    #[test]
    fn test_plan_replicas_for() {
        let topo = make_topology_3rack();
        let plan =
            ReplicaPlacer::place("test", 3, 2, &topo, PlacementStrategy::RackAwareSpread).unwrap();

        let replicas = plan.replicas_for(0).unwrap();
        assert_eq!(replicas.len(), 2);
        assert!(plan.replicas_for(999).is_none());
    }

    #[test]
    fn test_plan_preferred_leader() {
        let topo = make_topology_3rack();
        let plan =
            ReplicaPlacer::place("test", 3, 3, &topo, PlacementStrategy::RackAwareSpread).unwrap();

        let leader = plan.preferred_leader(0).unwrap();
        assert!(leader > 0);
        assert!(plan.preferred_leader(999).is_none());
    }

    #[test]
    fn test_plan_involved_brokers() {
        let topo = make_topology_3rack();
        let plan =
            ReplicaPlacer::place("test", 6, 3, &topo, PlacementStrategy::RackAwareSpread).unwrap();

        let brokers = plan.involved_brokers();
        assert!(brokers.len() >= 3, "Should involve at least 3 brokers");
    }

    #[test]
    fn test_plan_involved_racks() {
        let topo = make_topology_3rack();
        let plan =
            ReplicaPlacer::place("test", 6, 3, &topo, PlacementStrategy::RackAwareSpread).unwrap();

        let racks = plan.involved_racks(&topo);
        assert_eq!(racks.len(), 3, "Should involve all 3 racks");
    }

    // ─── Reassignment 测试 ──────────────────────────────────────────

    #[test]
    fn test_reassignment_no_changes() {
        let mut current = HashMap::new();
        current.insert(0, vec![1, 3, 5]);
        current.insert(1, vec![2, 4, 6]);

        let target = current.clone();
        let plan = compute_reassignment("test", &current, &target);
        assert!(plan.changes.is_empty());
        assert!(!plan.requires_data_migration);
    }

    #[test]
    fn test_reassignment_with_changes() {
        let mut current = HashMap::new();
        current.insert(0, vec![1, 3, 5]);
        current.insert(1, vec![2, 4, 6]);

        let mut target = HashMap::new();
        target.insert(0, vec![1, 3, 5]); // 不变
        target.insert(1, vec![1, 4, 6]); // broker 2 → 1 (需要迁移)

        let plan = compute_reassignment("test", &current, &target);
        assert_eq!(plan.changes.len(), 1);
        assert!(plan.changes.contains_key(&1));
        assert!(plan.requires_data_migration);
    }

    #[test]
    fn test_reassignment_leader_change_only() {
        let mut current = HashMap::new();
        current.insert(0, vec![1, 3, 5]);

        let mut target = HashMap::new();
        target.insert(0, vec![3, 1, 5]); // 只是 leader 变更，无新 broker

        let plan = compute_reassignment("test", &current, &target);
        assert_eq!(plan.changes.len(), 1);
        assert!(!plan.requires_data_migration); // 无新 broker，不需要数据迁移
    }

    #[test]
    fn test_reassignment_migration_count() {
        let mut current = HashMap::new();
        current.insert(0, vec![1, 3, 5]);
        current.insert(1, vec![2, 4, 6]);

        let mut target = HashMap::new();
        target.insert(0, vec![1, 3, 5]); // 不变
        target.insert(1, vec![1, 2, 4]); // 6→1, 需要迁移

        let plan = compute_reassignment("test", &current, &target);
        assert_eq!(plan.compute_migration_count(), 1);
    }

    // ─── 统计测试 ───────────────────────────────────────────────────

    #[test]
    fn test_stats_load_stddev_balanced() {
        let topo = make_topology_3rack();
        let plan =
            ReplicaPlacer::place("test", 6, 1, &topo, PlacementStrategy::RackAwareSpread).unwrap();

        // RF=1, 6 partitions, 6 brokers → 完全均衡
        assert!(
            plan.stats.load_stddev < 0.5,
            "Stddev should be low for balanced: {}",
            plan.stats.load_stddev
        );
    }

    #[test]
    fn test_stats_fell_back() {
        // 只有 2 个机架但需要 3 副本
        let brokers = vec![
            BrokerRackInfo {
                broker_id: 1,
                rack_id: RackId::new("rack-a"),
                host: "192.168.1.1".into(),
                is_alive: true,
                partition_load: 0,
            },
            BrokerRackInfo {
                broker_id: 2,
                rack_id: RackId::new("rack-a"),
                host: "192.168.1.2".into(),
                is_alive: true,
                partition_load: 0,
            },
            BrokerRackInfo {
                broker_id: 3,
                rack_id: RackId::new("rack-b"),
                host: "192.168.1.3".into(),
                is_alive: true,
                partition_load: 0,
            },
        ];
        let topo = RackTopology::from_brokers(&brokers);
        let plan =
            ReplicaPlacer::place("test", 3, 3, &topo, PlacementStrategy::RackAwareSpread).unwrap();

        assert!(plan.stats.fell_back, "Should fall back when racks < RF");
        assert!(
            !plan.stats.rack_constraint_satisfied,
            "Rack constraint should NOT be satisfied"
        );
    }

    // ─── 序列化测试 ─────────────────────────────────────────────────

    #[test]
    fn test_placement_strategy_serde() {
        let s = PlacementStrategy::LeaderBalanced;
        let json = serde_json::to_string(&s).unwrap();
        let decoded: PlacementStrategy = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, PlacementStrategy::LeaderBalanced);
    }

    #[test]
    fn test_placement_error_display() {
        let err = PlacementError::InsufficientBrokers {
            required: 3,
            available: 1,
        };
        let msg = format!("{}", err);
        assert!(msg.contains("3"));
        assert!(msg.contains("1"));
    }

    // ─── 综合场景测试 ────────────────────────────────────────────────

    #[test]
    fn test_full_placement_workflow() {
        let topo = make_topology_3rack();

        // 1. 创建放置计划
        let plan = ReplicaPlacer::place("orders", 12, 3, &topo, PlacementStrategy::RackAwareSpread)
            .unwrap();

        // 2. 验证约束
        assert!(plan.stats.rack_constraint_satisfied);
        assert_eq!(plan.stats.racks_used, 3);

        // 3. 验证所有 partition 有正确副本数
        for (pid, replicas) in &plan.assignments {
            assert_eq!(
                replicas.len(),
                3,
                "Partition {} should have 3 replicas",
                pid
            );
            let mut unique = replicas.clone();
            unique.sort();
            unique.dedup();
            assert_eq!(unique.len(), 3, "Partition {} has duplicate replicas", pid);
        }

        // 4. 计算重分配到不同策略
        let plan2 = ReplicaPlacer::place("orders", 12, 3, &topo, PlacementStrategy::LeaderBalanced)
            .unwrap();

        let reassignment = compute_reassignment("orders", &plan.assignments, &plan2.assignments);
        // 两种策略可能产生不同的分配
        let _ = reassignment.changes.len();
    }
}
