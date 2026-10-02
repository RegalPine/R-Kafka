//! Leader Replica — Leader 副本管理器
//!
//! 管理 Leader 视角的副本同步:
//! - 跟踪每个 Follower 的复制进度 (LEO)
//! - 协调 ISR 扩缩容
//! - 计算 HW (High Watermark)
//! - 处理 Follower Fetch 请求
//!
//! ```text
//! Leader Replica 管理:
//!
//! Follower 2 Fetch ──→ 更新 Follower2 LEO ──→ ISR 检查 ──→ HW 推进
//! Follower 3 Fetch ──→ 更新 Follower3 LEO ──→ ISR 检查 ──→ HW 推进
//!
//! HW = min(ISR 中所有 LEO)
//! ```

use std::collections::HashMap;
use std::time::Instant;

use rk_core::error::{RkError, Result};
use rk_core::types::{BrokerId, Offset, PartitionId, TopicName};
use tracing::{debug, info};

use crate::replica::PartitionReplicaSet;

// ─── Follower 复制进度 ───────────────────────────────────────────────

/// Follower 复制进度跟踪
#[derive(Debug, Clone)]
pub struct FollowerProgress {
    /// Follower Broker ID
    pub broker_id: BrokerId,
    /// Follower 的 Log End Offset
    pub log_end_offset: Offset,
    /// Follower 的 High Watermark
    pub high_watermark: Offset,
    /// 最后 fetch 时间
    pub last_fetch_time: Instant,
    /// 最后 fetch 的 offset
    pub last_fetch_offset: Offset,
    /// 累计 fetch 次数
    pub fetch_count: u64,
    /// 累计复制字节数
    pub bytes_replicated: i64,
}

impl FollowerProgress {
    /// 创建新的 Follower 进度
    pub fn new(broker_id: BrokerId) -> Self {
        Self {
            broker_id,
            log_end_offset: Offset(0),
            high_watermark: Offset(0),
            last_fetch_time: Instant::now(),
            last_fetch_offset: Offset(-1),
            fetch_count: 0,
            bytes_replicated: 0,
        }
    }

    /// 更新 fetch 进度
    pub fn update(&mut self, fetch_offset: Offset, bytes_fetched: i64, leader_hw: Offset) {
        self.log_end_offset = Offset(fetch_offset.0 + bytes_fetched.max(0));
        self.last_fetch_offset = fetch_offset;
        self.last_fetch_time = Instant::now();
        self.high_watermark = leader_hw;
        self.fetch_count += 1;
        self.bytes_replicated += bytes_fetched;
    }

    /// 计算与 Leader 的 lag
    pub fn lag(&self, leader_leo: Offset) -> i64 {
        (leader_leo.0 - self.log_end_offset.0).max(0)
    }

    /// 自上次 fetch 以来的时间
    pub fn time_since_fetch(&self) -> std::time::Duration {
        self.last_fetch_time.elapsed()
    }
}

// ─── LeaderReplica ───────────────────────────────────────────────────

/// Leader 副本管理器
///
/// 管理一个 Partition 的 Leader 视角:
/// - 维护所有 Follower 的复制进度
/// - 计算 HW
/// - 提供 Follower fetch 响应所需的元数据
pub struct LeaderReplica {
    /// Topic
    topic: TopicName,
    /// Partition
    partition: PartitionId,
    /// Leader Broker ID
    leader_id: BrokerId,
    /// Leader Epoch
    leader_epoch: i32,
    /// Leader LEO
    leader_leo: Offset,
    /// Leader HW
    leader_hw: Offset,
    /// Follower 复制进度
    followers: HashMap<BrokerId, FollowerProgress>,
    /// ISR 集合
    isr: Vec<BrokerId>,
    /// 副本分配列表
    replica_assignment: Vec<BrokerId>,
}

impl LeaderReplica {
    /// 创建新的 Leader 副本
    pub fn new(
        topic: TopicName,
        partition: PartitionId,
        leader_id: BrokerId,
        leader_epoch: i32,
        replica_assignment: Vec<BrokerId>,
    ) -> Self {
        let mut followers = HashMap::new();
        for &broker_id in &replica_assignment {
            if broker_id != leader_id {
                followers.insert(broker_id, FollowerProgress::new(broker_id));
            }
        }

        let isr = replica_assignment.clone();

        info!(
            topic = %topic.0,
            partition = partition.0,
            leader = leader_id.0,
            epoch = leader_epoch,
            replicas = replica_assignment.len(),
            "LeaderReplica created"
        );

        Self {
            topic,
            partition,
            leader_id,
            leader_epoch,
            leader_leo: Offset(0),
            leader_hw: Offset(0),
            followers,
            isr,
            replica_assignment,
        }
    }

    /// 获取 Topic
    pub fn topic(&self) -> &TopicName {
        &self.topic
    }

    /// 获取 Partition
    pub fn partition(&self) -> PartitionId {
        self.partition
    }

    /// 获取 Leader ID
    pub fn leader_id(&self) -> BrokerId {
        self.leader_id
    }

    /// 获取 Leader Epoch
    pub fn leader_epoch(&self) -> i32 {
        self.leader_epoch
    }

    /// 获取 Leader LEO
    pub fn leader_leo(&self) -> Offset {
        self.leader_leo
    }

    /// 获取 Leader HW
    pub fn leader_hw(&self) -> Offset {
        self.leader_hw
    }

    /// 获取 ISR 集合
    pub fn isr(&self) -> &[BrokerId] {
        &self.isr
    }

    /// ISR 大小
    pub fn isr_size(&self) -> usize {
        self.isr.len()
    }

    /// 更新 Leader LEO (写入新数据后调用)
    pub fn update_leader_leo(&mut self, new_leo: Offset) {
        if new_leo.0 > self.leader_leo.0 {
            self.leader_leo = new_leo;
        }
    }

    /// 处理 Follower 的 Fetch 请求
    ///
    /// 更新 Follower 的复制进度，并尝试推进 HW。
    /// 返回 (新的 HW, Follower 的 lag)。
    pub fn handle_follower_fetch(
        &mut self,
        follower_id: BrokerId,
        fetch_offset: Offset,
        bytes_fetched: i64,
    ) -> Result<(Offset, i64)> {
        if !self.followers.contains_key(&follower_id) {
            return Err(RkError::Protocol(format!(
                "Unknown follower {} for {}-{}",
                follower_id.0, self.topic.0, self.partition.0
            )));
        }

        // 更新 Follower 进度 (在块作用域内释放借用)
        {
            let progress = self.followers.get_mut(&follower_id).unwrap();
            progress.update(fetch_offset, bytes_fetched, self.leader_hw);
        }

        // 尝试推进 HW
        self.try_advance_hw();

        let lag = self.followers.get(&follower_id).unwrap().lag(self.leader_leo);
        Ok((self.leader_hw, lag))
    }

    /// 尝试推进 HW
    ///
    /// HW = min(ISR 中所有副本的 LEO)
    /// HW 只能单调递增。
    fn try_advance_hw(&mut self) {
        // 收集 ISR 中所有副本的 LEO
        let leader_leo = self.leader_leo;
        let min_isr_leo = self
            .isr
            .iter()
            .map(|broker_id| {
                if *broker_id == self.leader_id {
                    leader_leo
                } else if let Some(p) = self.followers.get(broker_id) {
                    p.log_end_offset
                } else {
                    Offset(0)
                }
            })
            .min()
            .unwrap_or(Offset(0));

        // HW 只能前进
        if min_isr_leo.0 > self.leader_hw.0 {
            let old_hw = self.leader_hw;
            self.leader_hw = min_isr_leo;
            debug!(
                topic = %self.topic.0,
                partition = self.partition.0,
                old_hw = old_hw.0,
                new_hw = self.leader_hw.0,
                "HW advanced"
            );
        }
    }

    /// 检查 ISR 并更新 (由 ISRTracker 调用后同步结果)
    ///
    /// 将 Follower 加入 ISR
    pub fn add_to_isr(&mut self, broker_id: BrokerId) {
        if !self.isr.contains(&broker_id) && self.followers.contains_key(&broker_id) {
            self.isr.push(broker_id);
            info!(
                topic = %self.topic.0,
                partition = self.partition.0,
                broker = broker_id.0,
                "Follower added to ISR"
            );
        }
    }

    /// 将 Follower 移出 ISR
    pub fn remove_from_isr(&mut self, broker_id: BrokerId) {
        if broker_id != self.leader_id {
            self.isr.retain(|&b| b != broker_id);
            info!(
                topic = %self.topic.0,
                partition = self.partition.0,
                broker = broker_id.0,
                "Follower removed from ISR"
            );
        }
    }

    /// 添加新的 Follower (扩容)
    pub fn add_follower(&mut self, broker_id: BrokerId) {
        if !self.followers.contains_key(&broker_id) {
            self.followers.insert(broker_id, FollowerProgress::new(broker_id));
            if !self.replica_assignment.contains(&broker_id) {
                self.replica_assignment.push(broker_id);
            }
        }
    }

    /// 移除 Follower (缩容)
    pub fn remove_follower(&mut self, broker_id: BrokerId) {
        self.followers.remove(&broker_id);
        self.isr.retain(|&b| b != broker_id);
        self.replica_assignment.retain(|&b| b != broker_id);
    }

    /// 获取 Follower 的复制进度
    pub fn follower_progress(&self, broker_id: BrokerId) -> Option<&FollowerProgress> {
        self.followers.get(&broker_id)
    }

    /// 获取所有 Follower 的进度
    pub fn all_follower_progress(&self) -> Vec<&FollowerProgress> {
        self.followers.values().collect()
    }

    /// 获取所有 Follower 的最大 lag
    pub fn max_follower_lag(&self) -> i64 {
        self.followers
            .values()
            .map(|p| p.lag(self.leader_leo))
            .max()
            .unwrap_or(0)
    }

    /// 获取 LeaderReplica 摘要
    pub fn summary(&self) -> LeaderReplicaSummary {
        let total_lag: i64 = self.followers.values().map(|p| p.lag(self.leader_leo)).sum();
        let total_bytes: i64 = self.followers.values().map(|p| p.bytes_replicated).sum();
        let total_fetches: u64 = self.followers.values().map(|p| p.fetch_count).sum();

        LeaderReplicaSummary {
            topic: self.topic.clone(),
            partition: self.partition,
            leader_id: self.leader_id,
            leader_epoch: self.leader_epoch,
            leader_leo: self.leader_leo,
            leader_hw: self.leader_hw,
            isr_size: self.isr.len(),
            follower_count: self.followers.len(),
            max_lag: self.max_follower_lag(),
            total_lag,
            total_bytes_replicated: total_bytes,
            total_fetches,
        }
    }

    /// 从 PartitionReplicaSet 构建 LeaderReplica
    pub fn from_replica_set(replica_set: &PartitionReplicaSet) -> Self {
        let mut leader = Self::new(
            replica_set.topic.clone(),
            replica_set.partition,
            replica_set.leader_id,
            replica_set.leader_epoch,
            replica_set.replica_assignment.clone(),
        );

        leader.leader_leo = replica_set.leader_leo;
        leader.leader_hw = replica_set.leader_hw;
        leader.isr = replica_set.isr.iter().copied().collect();

        // 同步 Follower 进度
        for (&broker_id, info) in &replica_set.replicas {
            if broker_id != replica_set.leader_id {
                if let Some(progress) = leader.followers.get_mut(&broker_id) {
                    progress.log_end_offset = info.log_end_offset;
                    progress.high_watermark = info.high_watermark;
                }
            }
        }

        leader
    }
}

// ─── 摘要 ────────────────────────────────────────────────────────────

/// Leader 副本摘要
#[derive(Debug, Clone)]
pub struct LeaderReplicaSummary {
    pub topic: TopicName,
    pub partition: PartitionId,
    pub leader_id: BrokerId,
    pub leader_epoch: i32,
    pub leader_leo: Offset,
    pub leader_hw: Offset,
    pub isr_size: usize,
    pub follower_count: usize,
    pub max_lag: i64,
    pub total_lag: i64,
    pub total_bytes_replicated: i64,
    pub total_fetches: u64,
}

// ─── Tests ───────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replica::PartitionReplicaSet;
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

    #[test]
    fn test_leader_replica_new() {
        let leader = LeaderReplica::new(
            topic("test"),
            partition(0),
            broker(1),
            0,
            vec![broker(1), broker(2), broker(3)],
        );
        assert_eq!(leader.leader_id(), broker(1));
        assert_eq!(leader.leader_epoch(), 0);
        assert_eq!(leader.leader_leo(), Offset(0));
        assert_eq!(leader.leader_hw(), Offset(0));
        assert_eq!(leader.isr_size(), 3);
        assert_eq!(leader.isr(), &[broker(1), broker(2), broker(3)]);
    }

    #[test]
    fn test_follower_progress_update() {
        let mut progress = FollowerProgress::new(broker(2));
        progress.update(Offset(0), 100, Offset(0));
        assert_eq!(progress.log_end_offset, Offset(100));
        assert_eq!(progress.fetch_count, 1);
        assert_eq!(progress.bytes_replicated, 100);

        progress.update(Offset(100), 50, Offset(80));
        assert_eq!(progress.log_end_offset, Offset(150));
        assert_eq!(progress.fetch_count, 2);
        assert_eq!(progress.bytes_replicated, 150);
    }

    #[test]
    fn test_follower_progress_lag() {
        let mut progress = FollowerProgress::new(broker(2));
        progress.log_end_offset = Offset(80);
        assert_eq!(progress.lag(Offset(100)), 20);
        assert_eq!(progress.lag(Offset(80)), 0);
        assert_eq!(progress.lag(Offset(50)), 0); // 不为负
    }

    #[test]
    fn test_handle_follower_fetch() {
        let mut leader = LeaderReplica::new(
            topic("test"),
            partition(0),
            broker(1),
            0,
            vec![broker(1), broker(2), broker(3)],
        );

        // Leader 写入到 offset 100
        leader.update_leader_leo(Offset(100));

        // Follower 2 fetch: 从 0 开始，获取 80 字节 (简化为 offset 0-79)
        let (hw, lag) = leader.handle_follower_fetch(broker(2), Offset(0), 80).unwrap();
        assert_eq!(leader.follower_progress(broker(2)).unwrap().log_end_offset, Offset(80));
        // HW = min(leader=100, f2=80, f3=0) = 0
        assert_eq!(hw, Offset(0));
        assert_eq!(lag, 20); // 100 - 80

        // Follower 3 fetch: 从 0 开始，获取 90 字节
        let (hw, lag) = leader.handle_follower_fetch(broker(3), Offset(0), 90).unwrap();
        // HW = min(100, 80, 90) = 80
        assert_eq!(hw, Offset(80));
        assert_eq!(lag, 10);
    }

    #[test]
    fn test_hw_monotonic_increase() {
        let mut leader = LeaderReplica::new(
            topic("test"),
            partition(0),
            broker(1),
            0,
            vec![broker(1), broker(2)],
        );

        leader.update_leader_leo(Offset(100));
        leader.handle_follower_fetch(broker(2), Offset(0), 80).unwrap();
        assert_eq!(leader.leader_hw(), Offset(80));

        // Follower 回退 (模拟重传)，HW 不应回退
        leader.handle_follower_fetch(broker(2), Offset(0), 50).unwrap();
        assert_eq!(leader.leader_hw(), Offset(80)); // 保持 80
    }

    #[test]
    fn test_isr_add_remove() {
        let mut leader = LeaderReplica::new(
            topic("test"),
            partition(0),
            broker(1),
            0,
            vec![broker(1), broker(2), broker(3)],
        );

        assert_eq!(leader.isr_size(), 3);

        leader.remove_from_isr(broker(2));
        assert_eq!(leader.isr_size(), 2);
        assert!(!leader.isr().contains(&broker(2)));

        leader.add_to_isr(broker(2));
        assert_eq!(leader.isr_size(), 3);
        assert!(leader.isr().contains(&broker(2)));

        // 不能移除 leader
        leader.remove_from_isr(broker(1));
        assert!(leader.isr().contains(&broker(1))); // leader 始终在 ISR
    }

    #[test]
    fn test_add_remove_follower() {
        let mut leader = LeaderReplica::new(
            topic("test"),
            partition(0),
            broker(1),
            0,
            vec![broker(1), broker(2)],
        );

        leader.add_follower(broker(3));
        assert!(leader.follower_progress(broker(3)).is_some());

        leader.remove_follower(broker(3));
        assert!(leader.follower_progress(broker(3)).is_none());
    }

    #[test]
    fn test_max_follower_lag() {
        let mut leader = LeaderReplica::new(
            topic("test"),
            partition(0),
            broker(1),
            0,
            vec![broker(1), broker(2), broker(3)],
        );

        leader.update_leader_leo(Offset(100));
        leader.handle_follower_fetch(broker(2), Offset(0), 80).unwrap();
        leader.handle_follower_fetch(broker(3), Offset(0), 60).unwrap();

        assert_eq!(leader.max_follower_lag(), 40); // broker(3) lag = 100 - 60
    }

    #[test]
    fn test_handle_unknown_follower() {
        let mut leader = LeaderReplica::new(
            topic("test"),
            partition(0),
            broker(1),
            0,
            vec![broker(1), broker(2)],
        );

        assert!(leader.handle_follower_fetch(broker(99), Offset(0), 10).is_err());
    }

    #[test]
    fn test_leader_replica_summary() {
        let mut leader = LeaderReplica::new(
            topic("test"),
            partition(0),
            broker(1),
            5,
            vec![broker(1), broker(2), broker(3)],
        );

        leader.update_leader_leo(Offset(100));
        leader.handle_follower_fetch(broker(2), Offset(0), 80).unwrap();
        leader.handle_follower_fetch(broker(3), Offset(0), 60).unwrap();

        let summary = leader.summary();
        assert_eq!(summary.topic, topic("test"));
        assert_eq!(summary.partition, partition(0));
        assert_eq!(summary.leader_id, broker(1));
        assert_eq!(summary.leader_epoch, 5);
        assert_eq!(summary.leader_leo, Offset(100));
        assert_eq!(summary.isr_size, 3);
        assert_eq!(summary.follower_count, 2);
        assert_eq!(summary.max_lag, 40);
        assert_eq!(summary.total_lag, 60); // 20 + 40
    }

    #[test]
    fn test_from_replica_set() {
        let mut prs = PartitionReplicaSet::new(
            topic("test"),
            partition(0),
            broker(1),
            vec![broker(1), broker(2), broker(3)],
        );
        prs.update_leader_offsets(Offset(100), Offset(80));
        prs.replicas.get_mut(&broker(2)).unwrap().log_end_offset = Offset(90);
        prs.replicas.get_mut(&broker(3)).unwrap().log_end_offset = Offset(80);

        let leader = LeaderReplica::from_replica_set(&prs);
        assert_eq!(leader.leader_leo(), Offset(100));
        assert_eq!(leader.leader_hw(), Offset(80));
        assert_eq!(leader.follower_progress(broker(2)).unwrap().log_end_offset, Offset(90));
        assert_eq!(leader.follower_progress(broker(3)).unwrap().log_end_offset, Offset(80));
    }
}
