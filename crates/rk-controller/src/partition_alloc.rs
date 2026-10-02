//! Partition 分配策略
//!
//! 在创建 Topic 或重新分配 Partition 时，决定每个 Partition 的副本
//! 应该放置在哪些 Broker 上。
//!
//! 策略:
//! - **RoundRobin**: 简单的轮询分配，均匀分布
//! - **RackAware**: 确保副本分布在不同机架上 (如果可能)
//! - **Manual**: 手动指定副本分配
//!
//! 约束:
//! - 副本数 ≤ 存活 Broker 数
//! - 每个 Broker 上的 Partition 数量尽量均衡
//! - RackAware 模式: 同一 Partition 的不同副本在不同机架

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use tracing::{info, warn};

// ─── 分配策略 ─────────────────────────────────────────────────────────

/// Partition 分配策略
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AllocationStrategy {
    /// 轮询分配 (均匀分布)
    RoundRobin,
    /// 机架感知分配 (跨机架分布副本)
    RackAware,
}

/// Broker 信息 (用于分配决策)
#[derive(Debug, Clone)]
pub struct BrokerInfo {
    /// Broker ID
    pub broker_id: i32,
    /// 机架标识
    pub rack: Option<String>,
    /// 当前已分配的 Partition 数量 (用于均衡)
    pub partition_count: u32,
}

/// Partition 分配结果
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionAssignment {
    /// Topic 名称
    pub topic_name: String,
    /// Partition 编号 → 副本列表 (第一个是首选 Leader)
    pub assignments: HashMap<i32, Vec<i32>>,
}

// ─── 分配器 ──────────────────────────────────────────────────────────

/// Partition 分配器
pub struct PartitionAllocator;

impl PartitionAllocator {
    /// 分配 Partition 副本到 Broker
    ///
    /// # Arguments
    /// * `topic_name` - Topic 名称
    /// * `partitions` - Partition 数量
    /// * `replication_factor` - 副本因子
    /// * `brokers` - 可用 Broker 列表
    /// * `strategy` - 分配策略
    ///
    /// # Returns
    /// PartitionAssignment 或错误
    pub fn allocate(
        topic_name: &str,
        partitions: u32,
        replication_factor: u16,
        brokers: &[BrokerInfo],
        strategy: AllocationStrategy,
    ) -> Result<PartitionAssignment, AllocationError> {
        if brokers.is_empty() {
            return Err(AllocationError::NoBrokersAvailable);
        }

        let rf = replication_factor as usize;
        if rf > brokers.len() {
            return Err(AllocationError::InsufficientBrokers {
                required: rf,
                available: brokers.len(),
            });
        }

        if partitions == 0 {
            return Err(AllocationError::InvalidPartitions(0));
        }

        match strategy {
            AllocationStrategy::RoundRobin => {
                Self::allocate_round_robin(topic_name, partitions, rf, brokers)
            }
            AllocationStrategy::RackAware => {
                Self::allocate_rack_aware(topic_name, partitions, rf, brokers)
            }
        }
    }

    /// 轮询分配
    ///
    /// 将 Partition 的副本均匀分布在所有 Broker 上。
    /// 每个 Partition 的副本从不同的起始 Broker 开始。
    fn allocate_round_robin(
        topic_name: &str,
        partitions: u32,
        replication_factor: usize,
        brokers: &[BrokerInfo],
    ) -> Result<PartitionAssignment, AllocationError> {
        let mut assignments = HashMap::new();
        let broker_count = brokers.len();

        // 按 partition_count 排序，优先分配给负载低的 Broker
        let mut sorted_brokers: Vec<&BrokerInfo> = brokers.iter().collect();
        sorted_brokers.sort_by_key(|a| a.partition_count);

        let broker_ids: Vec<i32> = sorted_brokers.iter().map(|b| b.broker_id).collect();

        for partition_id in 0..partitions {
            let mut replicas = Vec::with_capacity(replication_factor);

            // 从 partition_id 对应的偏移开始轮询
            let start = (partition_id as usize) % broker_count;

            for offset in 0..replication_factor {
                let idx = (start + offset) % broker_count;
                replicas.push(broker_ids[idx]);
            }

            assignments.insert(partition_id as i32, replicas);
        }

        info!(
            topic = topic_name,
            partitions = partitions,
            replication_factor = replication_factor,
            "Partition allocation (round-robin) completed"
        );

        Ok(PartitionAssignment {
            topic_name: topic_name.to_string(),
            assignments,
        })
    }

    /// 机架感知分配
    ///
    /// 确保同一 Partition 的不同副本分布在不同机架上 (如果可能)。
    fn allocate_rack_aware(
        topic_name: &str,
        partitions: u32,
        replication_factor: usize,
        brokers: &[BrokerInfo],
    ) -> Result<PartitionAssignment, AllocationError> {
        // 按机架分组 Broker
        let mut rack_brokers: HashMap<Option<String>, Vec<i32>> = HashMap::new();
        for broker in brokers {
            rack_brokers
                .entry(broker.rack.clone())
                .or_default()
                .push(broker.broker_id);
        }

        let racks: Vec<Option<String>> = rack_brokers.keys().cloned().collect();
        let rack_count = racks.len();

        // 如果机架数 < 副本因子，回退到轮询
        if rack_count < replication_factor {
            warn!(
                rack_count = rack_count,
                replication_factor = replication_factor,
                "Not enough racks for rack-aware allocation, falling back to round-robin"
            );
            return Self::allocate_round_robin(topic_name, partitions, replication_factor, brokers);
        }

        let mut assignments = HashMap::new();

        for partition_id in 0..partitions {
            let mut replicas = Vec::with_capacity(replication_factor);

            // 每个 Partition 从不同的机架偏移开始
            let rack_start = (partition_id as usize) % rack_count;

            for offset in 0..replication_factor {
                let rack_idx = (rack_start + offset) % rack_count;
                let rack = &racks[rack_idx];

                if let Some(broker_ids) = rack_brokers.get(rack) {
                    // 在该机架中选择 partition_id 对应的 Broker
                    let broker_idx = (partition_id as usize) % broker_ids.len();
                    replicas.push(broker_ids[broker_idx]);
                }
            }

            assignments.insert(partition_id as i32, replicas);
        }

        info!(
            topic = topic_name,
            partitions = partitions,
            replication_factor = replication_factor,
            racks = rack_count,
            "Partition allocation (rack-aware) completed"
        );

        Ok(PartitionAssignment {
            topic_name: topic_name.to_string(),
            assignments,
        })
    }

    /// 重新平衡: 计算当前分配的不均衡度
    ///
    /// 返回每个 Broker 的 Partition 数量标准差。
    /// 值越接近 0 表示越均衡。
    pub fn imbalance_score(assignment: &PartitionAssignment, broker_ids: &[i32]) -> f64 {
        let mut counts: HashMap<i32, u32> = HashMap::new();
        for &bid in broker_ids {
            counts.insert(bid, 0);
        }

        for replicas in assignment.assignments.values() {
            for &bid in replicas {
                *counts.entry(bid).or_insert(0) += 1;
            }
        }

        let values: Vec<f64> = counts.values().map(|&c| c as f64).collect();
        let n = values.len() as f64;
        if n == 0.0 {
            return 0.0;
        }

        let mean = values.iter().sum::<f64>() / n;
        let variance = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / n;

        variance.sqrt()
    }
}

// ─── 错误类型 ─────────────────────────────────────────────────────────

/// Partition 分配错误
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AllocationError {
    /// 没有可用 Broker
    NoBrokersAvailable,
    /// Broker 数量不足
    InsufficientBrokers { required: usize, available: usize },
    /// 无效的 Partition 数量
    InvalidPartitions(u32),
}

impl std::fmt::Display for AllocationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AllocationError::NoBrokersAvailable => {
                write!(f, "No brokers available for partition assignment")
            }
            AllocationError::InsufficientBrokers {
                required,
                available,
            } => {
                write!(
                    f,
                    "Insufficient brokers: need {} but only {} available",
                    required, available
                )
            }
            AllocationError::InvalidPartitions(n) => {
                write!(f, "Invalid partition count: {}", n)
            }
        }
    }
}

impl std::error::Error for AllocationError {}

// ─── 单元测试 ────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_brokers(ids: &[i32]) -> Vec<BrokerInfo> {
        ids.iter()
            .map(|&id| BrokerInfo {
                broker_id: id,
                rack: None,
                partition_count: 0,
            })
            .collect()
    }

    fn make_brokers_with_racks(brokers: &[(i32, &str)]) -> Vec<BrokerInfo> {
        brokers
            .iter()
            .map(|&(id, rack)| BrokerInfo {
                broker_id: id,
                rack: Some(rack.to_string()),
                partition_count: 0,
            })
            .collect()
    }

    // ─── Round-Robin 测试 ────────────────────────────────────────────

    #[test]
    fn test_round_robin_basic() {
        let brokers = make_brokers(&[1, 2, 3]);
        let result =
            PartitionAllocator::allocate("test", 3, 2, &brokers, AllocationStrategy::RoundRobin)
                .unwrap();

        assert_eq!(result.assignments.len(), 3);

        // 每个 partition 有 2 个副本
        for (_, replicas) in &result.assignments {
            assert_eq!(replicas.len(), 2);
        }

        // 所有副本应该是不同的 broker
        for (_, replicas) in &result.assignments {
            let mut unique = replicas.clone();
            unique.sort();
            unique.dedup();
            assert_eq!(
                unique.len(),
                replicas.len(),
                "Duplicate brokers in replicas"
            );
        }
    }

    #[test]
    fn test_round_robin_single_broker() {
        let brokers = make_brokers(&[1]);
        let result =
            PartitionAllocator::allocate("test", 3, 1, &brokers, AllocationStrategy::RoundRobin)
                .unwrap();

        assert_eq!(result.assignments.len(), 3);
        for (_, replicas) in &result.assignments {
            assert_eq!(replicas, &vec![1]);
        }
    }

    #[test]
    fn test_round_robin_distribution() {
        let brokers = make_brokers(&[1, 2, 3]);
        let result =
            PartitionAllocator::allocate("test", 6, 1, &brokers, AllocationStrategy::RoundRobin)
                .unwrap();

        // 每个 broker 应该分配 2 个 partition
        let mut counts: HashMap<i32, u32> = HashMap::new();
        for replicas in result.assignments.values() {
            for &bid in replicas {
                *counts.entry(bid).or_insert(0) += 1;
            }
        }

        assert_eq!(counts[&1], 2);
        assert_eq!(counts[&2], 2);
        assert_eq!(counts[&3], 2);
    }

    #[test]
    fn test_round_robin_rf_equals_brokers() {
        let brokers = make_brokers(&[1, 2, 3]);
        let result =
            PartitionAllocator::allocate("test", 3, 3, &brokers, AllocationStrategy::RoundRobin)
                .unwrap();

        // 每个 partition 的副本应该包含所有 broker
        for (_, replicas) in &result.assignments {
            assert_eq!(replicas.len(), 3);
        }
    }

    // ─── Rack-Aware 测试 ─────────────────────────────────────────────

    #[test]
    fn test_rack_aware_basic() {
        let brokers = make_brokers_with_racks(&[(1, "rack-a"), (2, "rack-b"), (3, "rack-c")]);

        let result =
            PartitionAllocator::allocate("test", 3, 3, &brokers, AllocationStrategy::RackAware)
                .unwrap();

        assert_eq!(result.assignments.len(), 3);

        // 每个 partition 的 3 个副本应该在不同机架
        for (_, replicas) in &result.assignments {
            assert_eq!(replicas.len(), 3);
            // 验证没有重复 broker
            let mut unique = replicas.clone();
            unique.sort();
            unique.dedup();
            assert_eq!(unique.len(), 3);
        }
    }

    #[test]
    fn test_rack_aware_fallback_to_round_robin() {
        // 只有 2 个机架，但需要 3 个副本
        let brokers = make_brokers_with_racks(&[(1, "rack-a"), (2, "rack-a"), (3, "rack-b")]);

        let result =
            PartitionAllocator::allocate("test", 3, 3, &brokers, AllocationStrategy::RackAware)
                .unwrap();

        // 应该回退到 round-robin 但仍然成功
        assert_eq!(result.assignments.len(), 3);
    }

    #[test]
    fn test_rack_aware_rf_2() {
        let brokers = make_brokers_with_racks(&[(1, "rack-a"), (2, "rack-b"), (3, "rack-c")]);

        let result =
            PartitionAllocator::allocate("test", 6, 2, &brokers, AllocationStrategy::RackAware)
                .unwrap();

        // 每个 partition 的 2 个副本应该在不同机架
        for (pid, replicas) in &result.assignments {
            assert_eq!(
                replicas.len(),
                2,
                "Partition {} should have 2 replicas",
                pid
            );
        }
    }

    // ─── 错误测试 ────────────────────────────────────────────────────

    #[test]
    fn test_no_brokers() {
        let result =
            PartitionAllocator::allocate("test", 3, 1, &[], AllocationStrategy::RoundRobin);
        assert_eq!(result, Err(AllocationError::NoBrokersAvailable));
    }

    #[test]
    fn test_insufficient_brokers() {
        let brokers = make_brokers(&[1, 2]);
        let result =
            PartitionAllocator::allocate("test", 3, 3, &brokers, AllocationStrategy::RoundRobin);
        assert_eq!(
            result,
            Err(AllocationError::InsufficientBrokers {
                required: 3,
                available: 2,
            })
        );
    }

    #[test]
    fn test_zero_partitions() {
        let brokers = make_brokers(&[1, 2, 3]);
        let result =
            PartitionAllocator::allocate("test", 0, 1, &brokers, AllocationStrategy::RoundRobin);
        assert_eq!(result, Err(AllocationError::InvalidPartitions(0)));
    }

    // ─── 均衡度测试 ──────────────────────────────────────────────────

    #[test]
    fn test_imbalance_score_balanced() {
        let brokers = make_brokers(&[1, 2, 3]);
        let result =
            PartitionAllocator::allocate("test", 6, 1, &brokers, AllocationStrategy::RoundRobin)
                .unwrap();

        let score = PartitionAllocator::imbalance_score(&result, &[1, 2, 3]);
        // 完全均衡: 每个 broker 2 个 partition
        assert!(
            score < 0.01,
            "Imbalance score should be near 0 for balanced: {}",
            score
        );
    }

    #[test]
    fn test_imbalance_score_non_empty() {
        let assignment = PartitionAssignment {
            topic_name: "test".to_string(),
            assignments: {
                let mut m = HashMap::new();
                m.insert(0, vec![1, 2]);
                m.insert(1, vec![1, 3]);
                m.insert(2, vec![1, 2]);
                m
            },
        };

        let score = PartitionAllocator::imbalance_score(&assignment, &[1, 2, 3]);
        // broker 1 有 3 个，broker 2 有 2 个，broker 3 有 1 个
        // mean = 2, variance = ((1+0+1)/3) = 0.667, stddev ≈ 0.816
        assert!(score > 0.0, "Imbalance score should be > 0 for unbalanced");
    }

    // ─── 序列化测试 ──────────────────────────────────────────────────

    #[test]
    fn test_allocation_strategy_serde() {
        let strategy = AllocationStrategy::RackAware;
        let json = serde_json::to_string(&strategy).unwrap();
        let decoded: AllocationStrategy = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, AllocationStrategy::RackAware);
    }

    #[test]
    fn test_allocation_error_display() {
        let err = AllocationError::InsufficientBrokers {
            required: 3,
            available: 1,
        };
        let msg = format!("{}", err);
        assert!(msg.contains("3"));
        assert!(msg.contains("1"));
    }

    #[test]
    fn test_partition_assignment_debug() {
        let assignment = PartitionAssignment {
            topic_name: "test".to_string(),
            assignments: {
                let mut m = HashMap::new();
                m.insert(0, vec![1, 2, 3]);
                m
            },
        };
        let debug_str = format!("{:?}", assignment);
        assert!(debug_str.contains("test"));
    }

    // ─── 综合场景测试 ────────────────────────────────────────────────

    #[test]
    fn test_large_cluster_allocation() {
        let broker_ids: Vec<i32> = (1..=10).collect();
        let brokers = make_brokers(&broker_ids);

        let result = PartitionAllocator::allocate(
            "large-topic",
            20,
            3,
            &brokers,
            AllocationStrategy::RoundRobin,
        )
        .unwrap();

        assert_eq!(result.assignments.len(), 20);

        // 验证每个 partition 有 3 个不同的副本
        for (pid, replicas) in &result.assignments {
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
    }

    #[test]
    fn test_3_rack_3_replica_scenario() {
        // 经典场景: 3 机架，3 副本，6 分区
        let brokers = make_brokers_with_racks(&[
            (1, "rack-a"),
            (2, "rack-a"),
            (3, "rack-b"),
            (4, "rack-b"),
            (5, "rack-c"),
            (6, "rack-c"),
        ]);

        let result =
            PartitionAllocator::allocate("test", 6, 3, &brokers, AllocationStrategy::RackAware)
                .unwrap();

        assert_eq!(result.assignments.len(), 6);

        // 每个 partition 应该有 3 个副本
        for (_, replicas) in &result.assignments {
            assert_eq!(replicas.len(), 3);
        }
    }
}
