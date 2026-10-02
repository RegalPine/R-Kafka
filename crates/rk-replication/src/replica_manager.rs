//! Replica Manager
//!
//! 管理 Broker 上所有 Partition 的副本状态:
//! - 创建/删除 Partition 的副本集合
//! - 协调 HW/LEO 更新
//! - 协调 ISR 扩缩容
//! - 提供副本状态查询接口
//!
//! Phase 1: 单 Broker 模式，所有 Partition 的 Leader 都是本地 Broker。
//! Phase 3: 多 Broker 模式，支持 Follower Replica 管理。

use dashmap::DashMap;
use rk_core::error::{Result, RkError};
use rk_core::types::{BrokerId, Offset, PartitionId, TopicName};
use tracing::info;

use crate::hw_manager::{HWUpdate, HighWatermarkManager, LagStats};
use crate::isr::{ISRConfig, ISREvent, ISRTracker};
use crate::replica::PartitionReplicaSet;

// ─── Partition 键 ────────────────────────────────────────────────────

/// Partition 唯一键
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ReplicaPartitionKey {
    pub topic: TopicName,
    pub partition: PartitionId,
}

impl ReplicaPartitionKey {
    pub fn new(topic: TopicName, partition: PartitionId) -> Self {
        Self { topic, partition }
    }
}

// ─── ReplicaManager ──────────────────────────────────────────────────

/// 副本管理器
///
/// 管理 Broker 上所有 Partition 的副本状态。
/// 提供:
/// - 副本集合的 CRUD
/// - HW/LEO 更新协调
/// - ISR 扩缩容协调
/// - 状态查询 (lag stats, ISR snapshot 等)
pub struct ReplicaManager {
    /// 本地 Broker ID
    local_broker_id: BrokerId,
    /// 所有 Partition 的副本集合
    replica_sets: DashMap<ReplicaPartitionKey, PartitionReplicaSet>,
    /// HW/LEO 管理器
    hw_manager: HighWatermarkManager,
    /// ISR 追踪器
    isr_tracker: std::sync::Mutex<ISRTracker>,
}

impl ReplicaManager {
    /// 创建副本管理器
    pub fn new(local_broker_id: BrokerId, isr_config: ISRConfig) -> Self {
        Self {
            local_broker_id,
            replica_sets: DashMap::new(),
            hw_manager: HighWatermarkManager::new(),
            isr_tracker: std::sync::Mutex::new(ISRTracker::new(isr_config)),
        }
    }

    /// 创建默认配置的副本管理器
    pub fn with_defaults(local_broker_id: BrokerId) -> Self {
        Self::new(local_broker_id, ISRConfig::default())
    }

    /// 获取本地 Broker ID
    pub fn local_broker_id(&self) -> BrokerId {
        self.local_broker_id
    }

    // ─── 副本集合 CRUD ───────────────────────────────────────────

    /// 创建新的 Partition 副本集合
    ///
    /// Phase 1: 本地 Broker 是 Leader，其他 Broker 是 Follower。
    pub fn create_replica_set(
        &self,
        topic: TopicName,
        partition: PartitionId,
        replica_assignment: Vec<BrokerId>,
    ) -> Result<()> {
        let key = ReplicaPartitionKey::new(topic.clone(), partition);

        if self.replica_sets.contains_key(&key) {
            return Err(RkError::Protocol(format!(
                "Replica set already exists for {}-{}",
                topic.0, partition.0
            )));
        }

        let replica_set = PartitionReplicaSet::new(
            topic.clone(),
            partition,
            self.local_broker_id,
            replica_assignment,
        );

        info!(
            topic = %topic.0,
            partition = partition.0,
            replicas = replica_set.replica_count(),
            "Replica set created"
        );

        self.replica_sets.insert(key, replica_set);
        Ok(())
    }

    /// 删除 Partition 的副本集合
    pub fn delete_replica_set(&self, topic: &TopicName, partition: PartitionId) -> Result<()> {
        let key = ReplicaPartitionKey::new(topic.clone(), partition);

        match self.replica_sets.remove(&key) {
            Some(_) => {
                info!(
                    topic = %topic.0,
                    partition = partition.0,
                    "Replica set deleted"
                );
                Ok(())
            }
            None => Err(RkError::Protocol(format!(
                "Replica set not found for {}-{}",
                topic.0, partition.0
            ))),
        }
    }

    /// 获取 Partition 的副本集合 (只读)
    pub fn get_replica_set(
        &self,
        topic: &TopicName,
        partition: PartitionId,
    ) -> Option<dashmap::mapref::one::Ref<'_, ReplicaPartitionKey, PartitionReplicaSet>> {
        let key = ReplicaPartitionKey::new(topic.clone(), partition);
        self.replica_sets.get(&key)
    }

    /// 获取 Partition 的副本集合 (可变)
    pub fn get_replica_set_mut(
        &self,
        topic: &TopicName,
        partition: PartitionId,
    ) -> Option<dashmap::mapref::one::RefMut<'_, ReplicaPartitionKey, PartitionReplicaSet>> {
        let key = ReplicaPartitionKey::new(topic.clone(), partition);
        self.replica_sets.get_mut(&key)
    }

    /// 检查 Partition 是否存在
    pub fn has_replica_set(&self, topic: &TopicName, partition: PartitionId) -> bool {
        let key = ReplicaPartitionKey::new(topic.clone(), partition);
        self.replica_sets.contains_key(&key)
    }

    /// 获取所有管理的 Partition 数量
    pub fn partition_count(&self) -> usize {
        self.replica_sets.len()
    }

    /// 获取所有 Topic 名称
    pub fn topic_names(&self) -> Vec<TopicName> {
        self.replica_sets
            .iter()
            .map(|entry| entry.value().topic.clone())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect()
    }

    // ─── HW/LEO 操作 ────────────────────────────────────────────

    /// Leader append 后更新 LEO 和 HW
    ///
    /// 流程:
    /// 1. 更新 Leader LEO
    /// 2. 重新计算 HW
    /// 3. 返回 HW 更新结果
    pub fn on_leader_append(
        &self,
        topic: &TopicName,
        partition: PartitionId,
        new_leo: Offset,
    ) -> Result<HWUpdate> {
        let key = ReplicaPartitionKey::new(topic.clone(), partition);
        let mut entry = self.replica_sets.get_mut(&key).ok_or_else(|| {
            RkError::Protocol(format!(
                "Replica set not found for {}-{}",
                topic.0, partition.0
            ))
        })?;

        let replica_set = entry.value_mut();
        self.hw_manager.update_leader_leo(replica_set, new_leo);
        let update = self.hw_manager.update_hw(replica_set);

        Ok(update)
    }

    /// Follower Fetch 后更新 Follower LEO
    ///
    /// 流程:
    /// 1. 更新 Follower LEO
    /// 2. 检查 ISR 扩缩容
    /// 3. 重新计算 HW
    /// 4. 返回 ISR 事件和 HW 更新
    pub fn on_follower_fetch(
        &self,
        topic: &TopicName,
        partition: PartitionId,
        follower_id: BrokerId,
        follower_leo: Offset,
        leader_epoch: i32,
    ) -> Result<(Vec<ISREvent>, HWUpdate)> {
        let key = ReplicaPartitionKey::new(topic.clone(), partition);
        let mut entry = self.replica_sets.get_mut(&key).ok_or_else(|| {
            RkError::Protocol(format!(
                "Replica set not found for {}-{}",
                topic.0, partition.0
            ))
        })?;

        let replica_set = entry.value_mut();

        // 1. 更新 Follower LEO
        let isr_may_change = self.hw_manager.update_follower_leo(
            replica_set,
            follower_id,
            follower_leo,
            leader_epoch,
        );

        // 2. 检查 ISR 扩缩容
        let isr_events = if isr_may_change || true {
            // 总是检查 ISR (因为可能超时)
            let mut tracker = self
                .isr_tracker
                .lock()
                .map_err(|e| RkError::Internal(format!("ISR tracker lock poisoned: {}", e)))?;
            tracker.check_and_update_isr(replica_set)
        } else {
            Vec::new()
        };

        // 3. 重新计算 HW
        let hw_update = self.hw_manager.update_hw(replica_set);

        Ok((isr_events, hw_update))
    }

    // ─── 查询接口 ────────────────────────────────────────────────

    /// 获取指定 Partition 的 HW
    pub fn get_hw(&self, topic: &TopicName, partition: PartitionId) -> Option<Offset> {
        self.get_replica_set(topic, partition)
            .map(|rs| rs.leader_hw)
    }

    /// 获取指定 Partition 的 Leader LEO
    pub fn get_leo(&self, topic: &TopicName, partition: PartitionId) -> Option<Offset> {
        self.get_replica_set(topic, partition)
            .map(|rs| rs.leader_leo)
    }

    /// 获取指定 Partition 的 ISR 集合
    pub fn get_isr(&self, topic: &TopicName, partition: PartitionId) -> Option<Vec<BrokerId>> {
        self.get_replica_set(topic, partition)
            .map(|rs| rs.isr.iter().copied().collect())
    }

    /// 获取指定 Partition 的 Leader ID
    pub fn get_leader(&self, topic: &TopicName, partition: PartitionId) -> Option<BrokerId> {
        self.get_replica_set(topic, partition)
            .map(|rs| rs.leader_id)
    }

    /// 获取指定 Partition 的 Leader Epoch
    pub fn get_leader_epoch(&self, topic: &TopicName, partition: PartitionId) -> Option<i32> {
        self.get_replica_set(topic, partition)
            .map(|rs| rs.leader_epoch)
    }

    /// 获取指定 Follower 的 lag
    pub fn get_follower_lag(
        &self,
        topic: &TopicName,
        partition: PartitionId,
        follower_id: BrokerId,
    ) -> Option<i64> {
        self.get_replica_set(topic, partition)
            .and_then(|rs| self.hw_manager.follower_lag(&rs, follower_id))
    }

    /// 获取指定 Partition 的 lag 统计
    pub fn get_lag_stats(&self, topic: &TopicName, partition: PartitionId) -> Option<LagStats> {
        self.get_replica_set(topic, partition)
            .map(|rs| self.hw_manager.lag_stats(&rs))
    }

    /// 获取指定 Partition 的 ISR 大小
    pub fn get_isr_size(&self, topic: &TopicName, partition: PartitionId) -> Option<usize> {
        self.get_replica_set(topic, partition)
            .map(|rs| rs.isr_size())
    }

    /// 检查 ISR 是否满足最小大小要求
    pub fn is_isr_sufficient(&self, topic: &TopicName, partition: PartitionId) -> bool {
        let tracker = self.isr_tracker.lock().unwrap();
        self.get_replica_set(topic, partition)
            .map(|rs| tracker.is_isr_sufficient(&rs))
            .unwrap_or(false)
    }

    /// 获取所有 Partition 的摘要信息
    pub fn summary(&self) -> ReplicaManagerSummary {
        let mut total_replicas = 0;
        let mut total_isr_members = 0;
        let mut leader_count = 0;

        for entry in self.replica_sets.iter() {
            let rs = entry.value();
            total_replicas += rs.replica_count();
            total_isr_members += rs.isr_size();
            if rs.is_leader(self.local_broker_id) {
                leader_count += 1;
            }
        }

        ReplicaManagerSummary {
            partition_count: self.replica_sets.len(),
            total_replicas,
            total_isr_members,
            leader_count,
            local_broker_id: self.local_broker_id,
        }
    }
}

// ─── 摘要信息 ────────────────────────────────────────────────────────

/// ReplicaManager 摘要
#[derive(Debug, Clone)]
pub struct ReplicaManagerSummary {
    /// 管理的 Partition 数量
    pub partition_count: usize,
    /// 总副本数
    pub total_replicas: usize,
    /// 总 ISR 成员数
    pub total_isr_members: usize,
    /// 本地 Broker 是 Leader 的 Partition 数
    pub leader_count: usize,
    /// 本地 Broker ID
    pub local_broker_id: BrokerId,
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

    fn make_manager() -> ReplicaManager {
        ReplicaManager::with_defaults(broker(1))
    }

    #[test]
    fn test_create_replica_set() {
        let mgr = make_manager();
        let assignment = vec![broker(1), broker(2), broker(3)];

        mgr.create_replica_set(topic("test"), partition(0), assignment.clone())
            .unwrap();

        assert!(mgr.has_replica_set(&topic("test"), partition(0)));
        assert_eq!(mgr.partition_count(), 1);

        // 重复创建报错
        assert!(mgr
            .create_replica_set(topic("test"), partition(0), assignment)
            .is_err());
    }

    #[test]
    fn test_delete_replica_set() {
        let mgr = make_manager();
        let assignment = vec![broker(1), broker(2)];

        mgr.create_replica_set(topic("test"), partition(0), assignment)
            .unwrap();
        assert!(mgr.has_replica_set(&topic("test"), partition(0)));

        mgr.delete_replica_set(&topic("test"), partition(0))
            .unwrap();
        assert!(!mgr.has_replica_set(&topic("test"), partition(0)));

        // 删除不存在的报错
        assert!(mgr
            .delete_replica_set(&topic("test"), partition(0))
            .is_err());
    }

    #[test]
    fn test_on_leader_append() {
        let mgr = make_manager();
        let assignment = vec![broker(1), broker(2), broker(3)];
        mgr.create_replica_set(topic("test"), partition(0), assignment)
            .unwrap();

        let _update = mgr
            .on_leader_append(&topic("test"), partition(0), Offset(100))
            .unwrap();

        assert_eq!(mgr.get_leo(&topic("test"), partition(0)), Some(Offset(100)));
        // HW 可能没变 (follower 没追上)
    }

    #[test]
    fn test_on_follower_fetch() {
        let mgr = make_manager();
        let assignment = vec![broker(1), broker(2), broker(3)];
        mgr.create_replica_set(topic("test"), partition(0), assignment)
            .unwrap();

        // Leader append
        mgr.on_leader_append(&topic("test"), partition(0), Offset(100))
            .unwrap();

        // Follower 2 fetch
        let (_isr_events, _hw_update) = mgr
            .on_follower_fetch(&topic("test"), partition(0), broker(2), Offset(90), 0)
            .unwrap();

        // Follower 3 fetch
        let (_isr_events2, _hw_update2) = mgr
            .on_follower_fetch(&topic("test"), partition(0), broker(3), Offset(80), 0)
            .unwrap();

        // HW 应该更新为 min(100, 90, 80) = 80
        assert_eq!(mgr.get_hw(&topic("test"), partition(0)), Some(Offset(80)));
    }

    #[test]
    fn test_get_isr() {
        let mgr = make_manager();
        let assignment = vec![broker(1), broker(2), broker(3)];
        mgr.create_replica_set(topic("test"), partition(0), assignment)
            .unwrap();

        let isr = mgr.get_isr(&topic("test"), partition(0)).unwrap();
        assert_eq!(isr.len(), 3);
        assert!(isr.contains(&broker(1)));
        assert!(isr.contains(&broker(2)));
        assert!(isr.contains(&broker(3)));
    }

    #[test]
    fn test_get_leader() {
        let mgr = make_manager();
        let assignment = vec![broker(1), broker(2)];
        mgr.create_replica_set(topic("test"), partition(0), assignment)
            .unwrap();

        assert_eq!(
            mgr.get_leader(&topic("test"), partition(0)),
            Some(broker(1))
        );
    }

    #[test]
    fn test_get_follower_lag() {
        let mgr = make_manager();
        let assignment = vec![broker(1), broker(2), broker(3)];
        mgr.create_replica_set(topic("test"), partition(0), assignment)
            .unwrap();

        mgr.on_leader_append(&topic("test"), partition(0), Offset(100))
            .unwrap();

        mgr.on_follower_fetch(&topic("test"), partition(0), broker(2), Offset(80), 0)
            .unwrap();

        assert_eq!(
            mgr.get_follower_lag(&topic("test"), partition(0), broker(2)),
            Some(20)
        );
    }

    #[test]
    fn test_lag_stats() {
        let mgr = make_manager();
        let assignment = vec![broker(1), broker(2), broker(3)];
        mgr.create_replica_set(topic("test"), partition(0), assignment)
            .unwrap();

        mgr.on_leader_append(&topic("test"), partition(0), Offset(100))
            .unwrap();

        mgr.on_follower_fetch(&topic("test"), partition(0), broker(2), Offset(80), 0)
            .unwrap();

        mgr.on_follower_fetch(&topic("test"), partition(0), broker(3), Offset(60), 0)
            .unwrap();

        let stats = mgr.get_lag_stats(&topic("test"), partition(0)).unwrap();
        assert_eq!(stats.follower_count, 2);
        assert_eq!(stats.min_lag, 20);
        assert_eq!(stats.max_lag, 40);
        assert_eq!(stats.avg_lag, 30);
    }

    #[test]
    fn test_summary() {
        let mgr = make_manager();

        mgr.create_replica_set(
            topic("test"),
            partition(0),
            vec![broker(1), broker(2), broker(3)],
        )
        .unwrap();

        mgr.create_replica_set(topic("test"), partition(1), vec![broker(1), broker(2)])
            .unwrap();

        let summary = mgr.summary();
        assert_eq!(summary.partition_count, 2);
        assert_eq!(summary.total_replicas, 5); // 3 + 2
        assert_eq!(summary.total_isr_members, 5); // 3 + 2
        assert_eq!(summary.leader_count, 2); // both leaders are broker(1)
        assert_eq!(summary.local_broker_id, broker(1));
    }

    #[test]
    fn test_topic_names() {
        let mgr = make_manager();

        mgr.create_replica_set(topic("topic-a"), partition(0), vec![broker(1)])
            .unwrap();

        mgr.create_replica_set(topic("topic-b"), partition(0), vec![broker(1)])
            .unwrap();

        let names = mgr.topic_names();
        assert_eq!(names.len(), 2);
    }

    #[test]
    fn test_is_isr_sufficient() {
        let mgr = make_manager();
        let assignment = vec![broker(1), broker(2), broker(3)];
        mgr.create_replica_set(topic("test"), partition(0), assignment)
            .unwrap();

        // Default min_isr_size = 1, ISR = 3 → sufficient
        assert!(mgr.is_isr_sufficient(&topic("test"), partition(0)));
    }

    #[test]
    fn test_nonexistent_partition() {
        let mgr = make_manager();

        assert_eq!(mgr.get_hw(&topic("test"), partition(0)), None);
        assert_eq!(mgr.get_leo(&topic("test"), partition(0)), None);
        assert_eq!(mgr.get_isr(&topic("test"), partition(0)), None);
        assert_eq!(mgr.get_leader(&topic("test"), partition(0)), None);
        assert!(!mgr.is_isr_sufficient(&topic("test"), partition(0)));
    }
}
