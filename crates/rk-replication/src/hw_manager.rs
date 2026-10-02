//! High Watermark / Log End Offset Manager
//!
//! 管理 HW 和 LEO 的计算与更新:
//! - HW (High Watermark): ISR 中所有副本 LEO 的最小值
//! - LEO (Log End Offset): 副本已写入的最新位置
//! - 消费者只能读取 offset < HW 的数据
//!
//! HW 只能单调递增，不会回退。

use rk_core::types::{BrokerId, Offset};
use tracing::{debug, trace};

use crate::replica::PartitionReplicaSet;

// ─── HW 更新结果 ─────────────────────────────────────────────────────

/// HW 更新结果
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HWUpdate {
    /// 旧 HW
    pub old_hw: Offset,
    /// 新 HW
    pub new_hw: Offset,
    /// 是否发生了变化
    pub changed: bool,
}

impl HWUpdate {
    fn no_change(hw: Offset) -> Self {
        Self {
            old_hw: hw,
            new_hw: hw,
            changed: false,
        }
    }
}

// ─── HighWatermarkManager ────────────────────────────────────────────

/// HW/LEO 管理器
///
/// 职责:
/// 1. 计算新的 HW (基于 ISR 中最小 LEO)
/// 2. 更新 Leader 和 Follower 的 HW
/// 3. 确保 HW 单调递增
pub struct HighWatermarkManager;

impl HighWatermarkManager {
    /// 创建 HW 管理器
    pub fn new() -> Self {
        Self
    }

    /// 计算并更新 HW
    ///
    /// 返回 HW 更新结果。
    /// 在以下时机调用:
    /// 1. Leader 收到 Follower Fetch (follower LEO 更新后)
    /// 2. Leader 完成 append (leader LEO 更新后)
    pub fn update_hw(&self, replica_set: &mut PartitionReplicaSet) -> HWUpdate {
        let old_hw = replica_set.leader_hw;
        let new_hw = replica_set.compute_new_hw();

        if new_hw.0 > old_hw.0 {
            debug!(
                topic = %replica_set.topic.0,
                partition = replica_set.partition.0,
                old_hw = old_hw.0,
                new_hw = new_hw.0,
                "HW updated"
            );
            replica_set.leader_hw = new_hw;
            // 同步更新 Leader 的 ReplicaInfo
            if let Some(leader_info) = replica_set.replicas.get_mut(&replica_set.leader_id) {
                leader_info.high_watermark = new_hw;
            }
            HWUpdate {
                old_hw,
                new_hw,
                changed: true,
            }
        } else {
            HWUpdate::no_change(old_hw)
        }
    }

    /// 更新 Leader LEO (在 append 之后)
    ///
    /// 同时更新 Leader 的 ReplicaInfo。
    pub fn update_leader_leo(&self, replica_set: &mut PartitionReplicaSet, new_leo: Offset) {
        trace!(
            topic = %replica_set.topic.0,
            partition = replica_set.partition.0,
            old_leo = replica_set.leader_leo.0,
            new_leo = new_leo.0,
            "Leader LEO updated"
        );
        replica_set.update_leader_offsets(new_leo, replica_set.leader_hw);
    }

    /// 更新 Follower 的 LEO (在 Follower Fetch 响应后)
    ///
    /// 返回是否需要重新计算 HW。
    pub fn update_follower_leo(
        &self,
        replica_set: &mut PartitionReplicaSet,
        follower_id: BrokerId,
        new_leo: Offset,
        leader_epoch: i32,
    ) -> bool {
        if let Some(info) = replica_set.replicas.get_mut(&follower_id) {
            let old_leo = info.log_end_offset;
            info.update_fetch_progress(new_leo, replica_set.leader_hw, leader_epoch);

            // 更新状态
            if info.state == crate::replica::ReplicaState::New {
                info.state = crate::replica::ReplicaState::Syncing;
            }

            trace!(
                follower = follower_id.0,
                old_leo = old_leo.0,
                new_leo = new_leo.0,
                "Follower LEO updated"
            );

            // 如果 follower 在 ISR 中，HW 可能需要更新
            replica_set.isr.contains(&follower_id)
        } else {
            false
        }
    }

    /// 检查消费者是否可以读取指定 offset
    ///
    /// 只有 offset < HW 的数据才允许消费。
    pub fn is_offset_readable(&self, replica_set: &PartitionReplicaSet, offset: Offset) -> bool {
        offset.0 < replica_set.leader_hw.0
    }

    /// 获取当前 HW
    pub fn current_hw(&self, replica_set: &PartitionReplicaSet) -> Offset {
        replica_set.leader_hw
    }

    /// 获取当前 Leader LEO
    pub fn current_leo(&self, replica_set: &PartitionReplicaSet) -> Offset {
        replica_set.leader_leo
    }

    /// 计算指定 Follower 的 lag
    pub fn follower_lag(
        &self,
        replica_set: &PartitionReplicaSet,
        follower_id: BrokerId,
    ) -> Option<i64> {
        replica_set
            .replica_info(follower_id)
            .map(|info| info.lag_behind(replica_set.leader_leo))
    }

    /// 获取所有 Follower 的 lag 统计
    pub fn lag_stats(&self, replica_set: &PartitionReplicaSet) -> LagStats {
        let lags: Vec<i64> = replica_set
            .replicas
            .iter()
            .filter(|(&id, info)| {
                id != replica_set.leader_id && info.role == crate::replica::ReplicaRole::Follower
            })
            .map(|(_, info)| info.lag_behind(replica_set.leader_leo))
            .collect();

        if lags.is_empty() {
            return LagStats {
                min_lag: 0,
                max_lag: 0,
                avg_lag: 0,
                follower_count: 0,
            };
        }

        let min_lag = *lags.iter().min().unwrap();
        let max_lag = *lags.iter().max().unwrap();
        let avg_lag = lags.iter().sum::<i64>() / lags.len() as i64;

        LagStats {
            min_lag,
            max_lag,
            avg_lag,
            follower_count: lags.len(),
        }
    }
}

impl Default for HighWatermarkManager {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Lag 统计 ────────────────────────────────────────────────────────

/// Follower Lag 统计信息
#[derive(Debug, Clone)]
pub struct LagStats {
    /// 最小 lag
    pub min_lag: i64,
    /// 最大 lag
    pub max_lag: i64,
    /// 平均 lag
    pub avg_lag: i64,
    /// Follower 数量
    pub follower_count: usize,
}

// ─── Tests ───────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replica::{PartitionReplicaSet, ReplicaState};
    use rk_core::types::{BrokerId, Offset, PartitionId, TopicName};

    fn broker(id: i32) -> BrokerId {
        BrokerId(id)
    }

    fn topic(name: &str) -> TopicName {
        TopicName(name.to_string())
    }

    fn partition(id: i32) -> PartitionId {
        PartitionId(id)
    }

    fn make_test_set() -> PartitionReplicaSet {
        let assignment = vec![broker(1), broker(2), broker(3)];
        PartitionReplicaSet::new(topic("test"), partition(0), broker(1), assignment)
    }

    #[test]
    fn test_hw_update_basic() {
        let mut prs = make_test_set();
        let hw_mgr = HighWatermarkManager::new();

        // Leader append: LEO = 100
        hw_mgr.update_leader_leo(&mut prs, Offset(100));
        assert_eq!(prs.leader_leo, Offset(100));

        // HW 还没变 (followers 没追上)
        let update = hw_mgr.update_hw(&mut prs);
        // 初始 ISR 中 follower LEO = 0, 所以 HW = 0
        assert_eq!(update.new_hw, Offset(0));
        assert!(!update.changed);

        // Follower 2 追赶到 80
        hw_mgr.update_follower_leo(&mut prs, broker(2), Offset(80), 0);
        // Follower 3 追赶到 60
        hw_mgr.update_follower_leo(&mut prs, broker(3), Offset(60), 0);

        // HW = min(100, 80, 60) = 60
        let update = hw_mgr.update_hw(&mut prs);
        assert_eq!(update.new_hw, Offset(60));
        assert!(update.changed);
        assert_eq!(update.old_hw, Offset(0));
    }

    #[test]
    fn test_hw_monotonic_increase() {
        let mut prs = make_test_set();
        let hw_mgr = HighWatermarkManager::new();

        hw_mgr.update_leader_leo(&mut prs, Offset(100));
        prs.replicas.get_mut(&broker(2)).unwrap().log_end_offset = Offset(90);
        prs.replicas.get_mut(&broker(3)).unwrap().log_end_offset = Offset(80);

        let update = hw_mgr.update_hw(&mut prs);
        assert_eq!(update.new_hw, Offset(80));

        // Follower 3 回退到 50 (不应该让 HW 回退)
        prs.replicas.get_mut(&broker(3)).unwrap().log_end_offset = Offset(50);
        let update = hw_mgr.update_hw(&mut prs);
        assert_eq!(update.new_hw, Offset(80)); // 不回退
        assert!(!update.changed);
    }

    #[test]
    fn test_hw_after_isr_shrink() {
        let mut prs = make_test_set();
        let hw_mgr = HighWatermarkManager::new();

        hw_mgr.update_leader_leo(&mut prs, Offset(100));
        prs.replicas.get_mut(&broker(2)).unwrap().log_end_offset = Offset(90);
        prs.replicas.get_mut(&broker(3)).unwrap().log_end_offset = Offset(50);

        // HW = min(100, 90, 50) = 50
        let update = hw_mgr.update_hw(&mut prs);
        assert_eq!(update.new_hw, Offset(50));

        // 移除 broker(3) 出 ISR
        prs.isr.remove(&broker(3));

        // HW = min(100, 90) = 90
        let update = hw_mgr.update_hw(&mut prs);
        assert_eq!(update.new_hw, Offset(90));
        assert!(update.changed);
    }

    #[test]
    fn test_offset_readable() {
        let mut prs = make_test_set();
        let hw_mgr = HighWatermarkManager::new();

        prs.leader_hw = Offset(100);

        assert!(hw_mgr.is_offset_readable(&prs, Offset(99)));
        assert!(hw_mgr.is_offset_readable(&prs, Offset(0)));
        assert!(!hw_mgr.is_offset_readable(&prs, Offset(100))); // offset == HW 不可读
        assert!(!hw_mgr.is_offset_readable(&prs, Offset(101)));
    }

    #[test]
    fn test_follower_lag() {
        let mut prs = make_test_set();
        let hw_mgr = HighWatermarkManager::new();

        prs.update_leader_offsets(Offset(100), Offset(100));
        prs.replicas.get_mut(&broker(2)).unwrap().log_end_offset = Offset(80);
        prs.replicas.get_mut(&broker(3)).unwrap().log_end_offset = Offset(60);

        assert_eq!(hw_mgr.follower_lag(&prs, broker(2)), Some(20));
        assert_eq!(hw_mgr.follower_lag(&prs, broker(3)), Some(40));
        assert_eq!(hw_mgr.follower_lag(&prs, broker(99)), None);
    }

    #[test]
    fn test_lag_stats() {
        let mut prs = make_test_set();
        let hw_mgr = HighWatermarkManager::new();

        prs.update_leader_offsets(Offset(100), Offset(100));
        prs.replicas.get_mut(&broker(2)).unwrap().log_end_offset = Offset(80);
        prs.replicas.get_mut(&broker(3)).unwrap().log_end_offset = Offset(60);

        let stats = hw_mgr.lag_stats(&prs);
        assert_eq!(stats.follower_count, 2);
        assert_eq!(stats.min_lag, 20);
        assert_eq!(stats.max_lag, 40);
        assert_eq!(stats.avg_lag, 30); // (20 + 40) / 2
    }

    #[test]
    fn test_update_follower_sets_syncing_state() {
        let mut prs = make_test_set();
        let hw_mgr = HighWatermarkManager::new();

        // 初始 follower 状态是 New
        assert_eq!(
            prs.replicas.get(&broker(2)).unwrap().state,
            ReplicaState::New
        );

        // 更新 follower LEO 后变为 Syncing
        hw_mgr.update_follower_leo(&mut prs, broker(2), Offset(50), 0);
        assert_eq!(
            prs.replicas.get(&broker(2)).unwrap().state,
            ReplicaState::Syncing
        );
    }

    #[test]
    fn test_lag_stats_no_followers() {
        let assignment = vec![broker(1)];
        let prs = PartitionReplicaSet::new(topic("test"), partition(0), broker(1), assignment);
        let hw_mgr = HighWatermarkManager::new();

        let stats = hw_mgr.lag_stats(&prs);
        assert_eq!(stats.follower_count, 0);
        assert_eq!(stats.min_lag, 0);
        assert_eq!(stats.max_lag, 0);
    }
}
