//! Replica 核心数据结构
//!
//! 定义副本模型的基础类型:
//! - `ReplicaRole`: Leader / Follower / Observer
//! - `ReplicaState`: New → Syncing → InSync → OutOfSync → Failed
//! - `ReplicaInfo`: 单个副本的运行时状态 (LEO, HW, last_fetch_time)
//! - `PartitionReplicaSet`: 一个 Partition 的完整副本集合

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use rk_core::error::{Result, RkError};
use rk_core::types::{BrokerId, Offset, PartitionId, TopicName};
use serde::{Deserialize, Serialize};

// ─── Replica Role ────────────────────────────────────────────────────

/// 副本角色
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ReplicaRole {
    /// Leader: 接受 Producer 写入，协调 Follower 复制
    Leader,
    /// Follower: 从 Leader 拉取数据，加入 ISR
    Follower,
    /// Observer: 数据同步，不参与 ISR (异地复制/热备)
    Observer,
}

impl std::fmt::Display for ReplicaRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReplicaRole::Leader => write!(f, "Leader"),
            ReplicaRole::Follower => write!(f, "Follower"),
            ReplicaRole::Observer => write!(f, "Observer"),
        }
    }
}

// ─── Replica State ───────────────────────────────────────────────────

/// 副本同步状态机
///
/// 状态流转:
/// ```text
/// New → Syncing → InSync
///                    ↓ (lag)
///                OutOfSync
///                    ↓ (recovery)
///                 Syncing
///
/// Any → Failed (不可恢复错误)
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ReplicaState {
    /// 新创建的副本，尚未开始同步
    New,
    /// 正在同步中，但尚未追上 HW
    Syncing,
    /// 已追赶到 ISR 集合 (LEO 接近 Leader HW)
    InSync,
    /// 因 lag 过大被移出 ISR
    OutOfSync,
    /// 不可恢复错误
    Failed,
}

impl std::fmt::Display for ReplicaState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReplicaState::New => write!(f, "New"),
            ReplicaState::Syncing => write!(f, "Syncing"),
            ReplicaState::InSync => write!(f, "InSync"),
            ReplicaState::OutOfSync => write!(f, "OutOfSync"),
            ReplicaState::Failed => write!(f, "Failed"),
        }
    }
}

// ─── ReplicaInfo ─────────────────────────────────────────────────────

/// 单个副本的运行时状态
///
/// 每个 Follower/Observer 在 Leader 端维护一个 ReplicaInfo，
/// 跟踪其复制进度和健康状态。
#[derive(Debug, Clone)]
pub struct ReplicaInfo {
    /// 副本所在 Broker
    pub broker_id: BrokerId,
    /// 副本角色
    pub role: ReplicaRole,
    /// 副本同步状态
    pub state: ReplicaState,
    /// Log End Offset: 该副本已写入的位置
    pub log_end_offset: Offset,
    /// High Watermark: 该副本确认的位置
    pub high_watermark: Offset,
    /// 最后 fetch 时间 (用于 lag 检测)
    pub last_fetch_time: Instant,
    /// 最后 fetch 的 offset (用于计算 lag)
    pub last_fetch_offset: Offset,
    /// Leader Epoch (该副本已知的 leader epoch)
    pub leader_epoch: i32,
}

impl ReplicaInfo {
    /// 创建新的 Follower 副本信息
    pub fn new_follower(broker_id: BrokerId) -> Self {
        Self {
            broker_id,
            role: ReplicaRole::Follower,
            state: ReplicaState::New,
            log_end_offset: Offset(0),
            high_watermark: Offset(0),
            last_fetch_time: Instant::now(),
            last_fetch_offset: Offset(-1),
            leader_epoch: -1,
        }
    }

    /// 创建新的 Observer 副本信息
    pub fn new_observer(broker_id: BrokerId) -> Self {
        Self {
            broker_id,
            role: ReplicaRole::Observer,
            state: ReplicaState::New,
            log_end_offset: Offset(0),
            high_watermark: Offset(0),
            last_fetch_time: Instant::now(),
            last_fetch_offset: Offset(-1),
            leader_epoch: -1,
        }
    }

    /// 更新 Follower 的复制进度
    pub fn update_fetch_progress(&mut self, leo: Offset, hw: Offset, leader_epoch: i32) {
        self.log_end_offset = leo;
        self.high_watermark = hw;
        self.last_fetch_offset = leo;
        self.last_fetch_time = Instant::now();
        self.leader_epoch = leader_epoch;
    }

    /// 计算与 Leader 的 lag (基于 offset 差)
    pub fn lag_behind(&self, leader_leo: Offset) -> i64 {
        (leader_leo.0 - self.log_end_offset.0).max(0)
    }

    /// 自上次 fetch 以来的经过时间
    pub fn time_since_last_fetch(&self) -> std::time::Duration {
        self.last_fetch_time.elapsed()
    }
}

// ─── PartitionReplicaSet ─────────────────────────────────────────────

/// 一个 Partition 的完整副本集合
///
/// 维护:
/// - Leader 信息
/// - 所有 Follower/Observer 副本
/// - ISR 集合 (In-Sync Replicas)
/// - HW/LEO 跟踪
#[derive(Debug)]
pub struct PartitionReplicaSet {
    /// Topic 名称
    pub topic: TopicName,
    /// Partition ID
    pub partition: PartitionId,
    /// 当前 Leader Broker
    pub leader_id: BrokerId,
    /// Leader Epoch (单调递增，每次 Leader 变更 +1)
    pub leader_epoch: i32,
    /// Leader 的 Log End Offset
    pub leader_leo: Offset,
    /// Leader 的 High Watermark
    pub leader_hw: Offset,
    /// 所有副本 (broker_id → ReplicaInfo)
    pub replicas: HashMap<BrokerId, ReplicaInfo>,
    /// ISR 集合 (broker_id 列表，包含 Leader)
    pub isr: HashSet<BrokerId>,
    /// 副本分配列表 (有序，第一个是 preferred leader)
    pub replica_assignment: Vec<BrokerId>,
}

impl PartitionReplicaSet {
    /// 创建新的副本集合 (初始状态: 只有 Leader)
    pub fn new(
        topic: TopicName,
        partition: PartitionId,
        leader_id: BrokerId,
        replica_assignment: Vec<BrokerId>,
    ) -> Self {
        let mut replicas = HashMap::new();

        // Leader 自身
        let mut leader_info = ReplicaInfo::new_follower(leader_id);
        leader_info.role = ReplicaRole::Leader;
        leader_info.state = ReplicaState::InSync;
        replicas.insert(leader_id, leader_info);

        // Follower/Observer 副本
        for &broker_id in &replica_assignment {
            if broker_id != leader_id {
                replicas.insert(broker_id, ReplicaInfo::new_follower(broker_id));
            }
        }

        // 初始 ISR = 所有分配的副本
        let isr: HashSet<BrokerId> = replica_assignment.iter().copied().collect();

        Self {
            topic,
            partition,
            leader_id,
            leader_epoch: 0,
            leader_leo: Offset(0),
            leader_hw: Offset(0),
            replicas,
            isr,
            replica_assignment,
        }
    }

    /// 获取 Leader 的 ReplicaInfo
    pub fn leader_info(&self) -> Option<&ReplicaInfo> {
        self.replicas.get(&self.leader_id)
    }

    /// 获取指定 Broker 的副本信息
    pub fn replica_info(&self, broker_id: BrokerId) -> Option<&ReplicaInfo> {
        self.replicas.get(&broker_id)
    }

    /// 获取指定 Broker 的可变副本信息
    pub fn replica_info_mut(&mut self, broker_id: BrokerId) -> Option<&mut ReplicaInfo> {
        self.replicas.get_mut(&broker_id)
    }

    /// 是否为 Leader
    pub fn is_leader(&self, broker_id: BrokerId) -> bool {
        broker_id == self.leader_id
    }

    /// 是否在 ISR 中
    pub fn is_in_isr(&self, broker_id: BrokerId) -> bool {
        self.isr.contains(&broker_id)
    }

    /// ISR 大小
    pub fn isr_size(&self) -> usize {
        self.isr.len()
    }

    /// 副本总数
    pub fn replica_count(&self) -> usize {
        self.replicas.len()
    }

    /// 获取 preferred leader (replica_assignment 中的第一个)
    pub fn preferred_leader(&self) -> Option<BrokerId> {
        self.replica_assignment.first().copied()
    }

    /// 当前 Leader 是否是 preferred leader
    pub fn is_preferred_leader(&self) -> bool {
        self.replica_assignment.first() == Some(&self.leader_id)
    }

    /// 更新 Leader 的 LEO 和 HW
    pub fn update_leader_offsets(&mut self, leo: Offset, hw: Offset) {
        self.leader_leo = leo;
        self.leader_hw = hw;
        if let Some(leader) = self.replicas.get_mut(&self.leader_id) {
            leader.log_end_offset = leo;
            leader.high_watermark = hw;
        }
    }

    /// 添加新的 Follower 副本 (扩容)
    pub fn add_follower(&mut self, broker_id: BrokerId) -> Result<()> {
        if self.replicas.contains_key(&broker_id) {
            return Err(RkError::Protocol(format!(
                "Replica for broker {} already exists on {}-{}",
                broker_id.0, self.topic.0, self.partition.0
            )));
        }

        let info = ReplicaInfo::new_follower(broker_id);
        self.replicas.insert(broker_id, info);
        self.replica_assignment.push(broker_id);

        // 新副本初始不在 ISR 中 (需要先追赶)
        // 注意: 不自动加入 ISR，需要等 Follower fetch 追赶后再加入

        Ok(())
    }

    /// 移除副本 (缩容)
    pub fn remove_replica(&mut self, broker_id: BrokerId) -> Result<()> {
        if broker_id == self.leader_id {
            return Err(RkError::Protocol(format!(
                "Cannot remove leader replica (broker {}) from {}-{}",
                broker_id.0, self.topic.0, self.partition.0
            )));
        }

        self.replicas.remove(&broker_id);
        self.isr.remove(&broker_id);
        self.replica_assignment.retain(|&b| b != broker_id);

        Ok(())
    }

    /// 获取所有 ISR 中最小的 LEO (用于计算 HW)
    pub fn min_isr_leo(&self) -> Offset {
        self.isr
            .iter()
            .filter_map(|broker_id| self.replicas.get(broker_id))
            .map(|r| r.log_end_offset)
            .min()
            .unwrap_or(Offset(0))
    }

    /// 计算新的 HW = min(ISR 中所有副本的 LEO)
    ///
    /// HW 只能单调递增，不会回退。
    pub fn compute_new_hw(&self) -> Offset {
        let min_leo = self.min_isr_leo();
        // HW 只能前进，不能回退
        if min_leo.0 > self.leader_hw.0 {
            min_leo
        } else {
            self.leader_hw
        }
    }

    /// 获取所有 Follower 的 broker_id 列表
    pub fn follower_broker_ids(&self) -> Vec<BrokerId> {
        self.replicas
            .iter()
            .filter(|(&id, info)| info.role == ReplicaRole::Follower && id != self.leader_id)
            .map(|(&id, _)| id)
            .collect()
    }

    /// 获取所有非 Leader 副本的 broker_id (Follower + Observer)
    pub fn non_leader_broker_ids(&self) -> Vec<BrokerId> {
        self.replicas
            .iter()
            .filter(|(&id, _)| id != self.leader_id)
            .map(|(&id, _)| id)
            .collect()
    }
}

// ─── Tests ───────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn broker(id: i32) -> BrokerId {
        BrokerId(id)
    }

    fn topic(name: &str) -> TopicName {
        TopicName(name.to_string())
    }

    fn partition(id: i32) -> PartitionId {
        PartitionId(id)
    }

    #[test]
    fn test_replica_role_display() {
        assert_eq!(format!("{}", ReplicaRole::Leader), "Leader");
        assert_eq!(format!("{}", ReplicaRole::Follower), "Follower");
        assert_eq!(format!("{}", ReplicaRole::Observer), "Observer");
    }

    #[test]
    fn test_replica_state_display() {
        assert_eq!(format!("{}", ReplicaState::New), "New");
        assert_eq!(format!("{}", ReplicaState::InSync), "InSync");
        assert_eq!(format!("{}", ReplicaState::OutOfSync), "OutOfSync");
        assert_eq!(format!("{}", ReplicaState::Failed), "Failed");
    }

    #[test]
    fn test_replica_info_new_follower() {
        let info = ReplicaInfo::new_follower(broker(2));
        assert_eq!(info.broker_id, broker(2));
        assert_eq!(info.role, ReplicaRole::Follower);
        assert_eq!(info.state, ReplicaState::New);
        assert_eq!(info.log_end_offset, Offset(0));
        assert_eq!(info.high_watermark, Offset(0));
        assert_eq!(info.leader_epoch, -1);
    }

    #[test]
    fn test_replica_info_update_fetch_progress() {
        let mut info = ReplicaInfo::new_follower(broker(2));
        info.update_fetch_progress(Offset(100), Offset(80), 5);
        assert_eq!(info.log_end_offset, Offset(100));
        assert_eq!(info.high_watermark, Offset(80));
        assert_eq!(info.last_fetch_offset, Offset(100));
        assert_eq!(info.leader_epoch, 5);
    }

    #[test]
    fn test_replica_info_lag_behind() {
        let mut info = ReplicaInfo::new_follower(broker(2));
        info.log_end_offset = Offset(80);
        assert_eq!(info.lag_behind(Offset(100)), 20);
        assert_eq!(info.lag_behind(Offset(80)), 0);
        assert_eq!(info.lag_behind(Offset(50)), 0); // lag 不为负
    }

    #[test]
    fn test_partition_replica_set_new() {
        let assignment = vec![broker(1), broker(2), broker(3)];
        let prs =
            PartitionReplicaSet::new(topic("test"), partition(0), broker(1), assignment.clone());

        assert_eq!(prs.leader_id, broker(1));
        assert_eq!(prs.leader_epoch, 0);
        assert_eq!(prs.replica_count(), 3);
        assert_eq!(prs.isr_size(), 3);
        assert!(prs.is_in_isr(broker(1)));
        assert!(prs.is_in_isr(broker(2)));
        assert!(prs.is_in_isr(broker(3)));
        assert!(prs.is_leader(broker(1)));
        assert!(!prs.is_leader(broker(2)));
        assert_eq!(prs.preferred_leader(), Some(broker(1)));
        assert!(prs.is_preferred_leader());
    }

    #[test]
    fn test_partition_replica_set_add_remove_follower() {
        let assignment = vec![broker(1), broker(2)];
        let mut prs = PartitionReplicaSet::new(topic("test"), partition(0), broker(1), assignment);

        // 添加 follower
        prs.add_follower(broker(3)).unwrap();
        assert_eq!(prs.replica_count(), 3);
        assert!(!prs.is_in_isr(broker(3))); // 新 follower 不在 ISR

        // 重复添加报错
        assert!(prs.add_follower(broker(3)).is_err());

        // 移除 follower
        prs.remove_replica(broker(3)).unwrap();
        assert_eq!(prs.replica_count(), 2);

        // 不能移除 leader
        assert!(prs.remove_replica(broker(1)).is_err());
    }

    #[test]
    fn test_partition_replica_set_hw_computation() {
        let assignment = vec![broker(1), broker(2), broker(3)];
        let mut prs = PartitionReplicaSet::new(topic("test"), partition(0), broker(1), assignment);

        // Leader LEO = 100
        prs.update_leader_offsets(Offset(100), Offset(0));

        // Follower 2 LEO = 90
        prs.replicas.get_mut(&broker(2)).unwrap().log_end_offset = Offset(90);
        // Follower 3 LEO = 80
        prs.replicas.get_mut(&broker(3)).unwrap().log_end_offset = Offset(80);

        // HW = min(ISR LEOs) = min(100, 90, 80) = 80
        let new_hw = prs.compute_new_hw();
        assert_eq!(new_hw, Offset(80));
        // 模拟 HighWatermarkManager 应用新 HW
        prs.leader_hw = new_hw;

        // 移除 broker(3) 出 ISR
        prs.isr.remove(&broker(3));
        // HW = min(100, 90) = 90
        let new_hw = prs.compute_new_hw();
        assert_eq!(new_hw, Offset(90));
        prs.leader_hw = new_hw;

        // HW 不回退: 即使 follower 回退
        prs.replicas.get_mut(&broker(2)).unwrap().log_end_offset = Offset(50);
        // leader_hw 已经是 90，compute_new_hw 不会返回低于 90 的值
        let new_hw = prs.compute_new_hw();
        assert_eq!(new_hw, Offset(90));
    }

    #[test]
    fn test_partition_replica_set_min_isr_leo() {
        let assignment = vec![broker(1), broker(2)];
        let mut prs = PartitionReplicaSet::new(topic("test"), partition(0), broker(1), assignment);

        prs.update_leader_offsets(Offset(100), Offset(0));
        prs.replicas.get_mut(&broker(2)).unwrap().log_end_offset = Offset(75);

        assert_eq!(prs.min_isr_leo(), Offset(75));

        // 移除 broker(2) 出 ISR
        prs.isr.remove(&broker(2));
        assert_eq!(prs.min_isr_leo(), Offset(100));
    }

    #[test]
    fn test_partition_replica_set_follower_broker_ids() {
        let assignment = vec![broker(1), broker(2), broker(3)];
        let prs = PartitionReplicaSet::new(topic("test"), partition(0), broker(1), assignment);

        let followers = prs.follower_broker_ids();
        assert_eq!(followers.len(), 2);
        assert!(followers.contains(&broker(2)));
        assert!(followers.contains(&broker(3)));
        assert!(!followers.contains(&broker(1)));
    }

    #[test]
    fn test_partition_replica_set_non_leader() {
        let assignment = vec![broker(1), broker(2), broker(3)];
        let prs = PartitionReplicaSet::new(
            topic("test"),
            partition(0),
            broker(2), // leader is broker 2
            assignment,
        );

        let non_leaders = prs.non_leader_broker_ids();
        assert_eq!(non_leaders.len(), 2);
        assert!(non_leaders.contains(&broker(1)));
        assert!(non_leaders.contains(&broker(3)));
        assert!(!non_leaders.contains(&broker(2)));

        assert!(!prs.is_preferred_leader()); // preferred is broker(1), leader is broker(2)
    }
}
