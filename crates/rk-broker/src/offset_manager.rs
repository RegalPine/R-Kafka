//! Offset Manager
//!
//! 管理消费者组的已提交偏移量。
//! Phase 1: DashMap 内存存储，支持持久化到 __consumer_offsets 内部 topic。
//! Phase 3: 迁移到 KRaft metadata log。

use std::collections::HashMap;
use std::path::PathBuf;

use dashmap::DashMap;
use rk_core::error::Result;
use tracing::{debug, info};

/// 偏移量键: (group_id, topic, partition)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OffsetKey {
    pub group_id: String,
    pub topic: String,
    pub partition: i32,
}

/// 已提交的偏移量记录
#[derive(Debug, Clone)]
pub struct CommittedOffset {
    pub offset: i64,
    pub leader_epoch: i32,
    pub metadata: Option<String>,
    pub commit_timestamp_ms: i64,
}

/// Offset Manager: 管理所有消费者组的偏移量
pub struct OffsetManager {
    /// (group_id, topic, partition) → CommittedOffset
    offsets: DashMap<OffsetKey, CommittedOffset>,
    /// 持久化目录 (Phase 1: 可选)
    _persist_dir: Option<PathBuf>,
}

impl OffsetManager {
    /// 创建新的 OffsetManager
    pub fn new(persist_dir: Option<PathBuf>) -> Self {
        Self {
            offsets: DashMap::new(),
            _persist_dir: persist_dir,
        }
    }

    /// 提交偏移量
    pub fn commit_offset(
        &self,
        group_id: &str,
        topic: &str,
        partition: i32,
        offset: i64,
        leader_epoch: i32,
        metadata: Option<String>,
    ) -> Result<()> {
        let key = OffsetKey {
            group_id: group_id.to_string(),
            topic: topic.to_string(),
            partition,
        };

        let committed = CommittedOffset {
            offset,
            leader_epoch,
            metadata,
            commit_timestamp_ms: current_timestamp_ms(),
        };

        self.offsets.insert(key, committed);

        debug!(
            group_id = group_id,
            topic = topic,
            partition = partition,
            offset = offset,
            "Offset committed"
        );

        Ok(())
    }

    /// 查询已提交的偏移量
    pub fn fetch_offset(
        &self,
        group_id: &str,
        topic: &str,
        partition: i32,
    ) -> Option<CommittedOffset> {
        let key = OffsetKey {
            group_id: group_id.to_string(),
            topic: topic.to_string(),
            partition,
        };
        self.offsets.get(&key).map(|e| e.clone())
    }

    /// 查询指定 group + topic 的全部 partition 偏移量
    pub fn fetch_offsets_for_topic(
        &self,
        group_id: &str,
        topic: &str,
    ) -> HashMap<i32, CommittedOffset> {
        let mut result = HashMap::new();
        for entry in self.offsets.iter() {
            if entry.key().group_id == group_id && entry.key().topic == topic {
                result.insert(entry.key().partition, entry.value().clone());
            }
        }
        result
    }

    /// 查询指定 group 的全部偏移量 (按 topic 分组)
    pub fn fetch_all_offsets_for_group(
        &self,
        group_id: &str,
    ) -> HashMap<String, HashMap<i32, CommittedOffset>> {
        let mut result: HashMap<String, HashMap<i32, CommittedOffset>> = HashMap::new();
        for entry in self.offsets.iter() {
            if entry.key().group_id == group_id {
                result
                    .entry(entry.key().topic.clone())
                    .or_default()
                    .insert(entry.key().partition, entry.value().clone());
            }
        }
        result
    }

    /// 删除指定 group 的全部偏移量
    pub fn delete_group_offsets(&self, group_id: &str) {
        let keys_to_remove: Vec<OffsetKey> = self
            .offsets
            .iter()
            .filter(|e| e.key().group_id == group_id)
            .map(|e| e.key().clone())
            .collect();

        for key in keys_to_remove {
            self.offsets.remove(&key);
        }

        info!(group_id = group_id, "Deleted all offsets for group");
    }

    /// 获取已知的 group_id 列表
    pub fn list_groups(&self) -> Vec<String> {
        let mut groups = std::collections::HashSet::new();
        for entry in self.offsets.iter() {
            groups.insert(entry.key().group_id.clone());
        }
        groups.into_iter().collect()
    }

    /// 获取已提交的偏移量总数
    pub fn offset_count(&self) -> usize {
        self.offsets.len()
    }

    /// 删除指定 (group, topic, partition) 的偏移量
    pub fn delete_offset(&self, group_id: &str, topic: &str, partition: i32) {
        let key = OffsetKey {
            group_id: group_id.to_string(),
            topic: topic.to_string(),
            partition,
        };
        self.offsets.remove(&key);
        debug!(
            group_id = group_id,
            topic = topic,
            partition = partition,
            "Deleted offset"
        );
    }

    /// 获取指定 (group, topic, partition) 的偏移量 (别名)
    pub fn get_offset(&self, group_id: &str, topic: &str, partition: i32) -> Option<i64> {
        self.fetch_offset(group_id, topic, partition).map(|co| co.offset)
    }
}

/// 当前时间戳 (毫秒)
fn current_timestamp_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_offset_manager_commit_and_fetch() {
        let om = OffsetManager::new(None);

        om.commit_offset("group-1", "topic-a", 0, 42, -1, None).unwrap();

        let offset = om.fetch_offset("group-1", "topic-a", 0).unwrap();
        assert_eq!(offset.offset, 42);
        assert_eq!(offset.leader_epoch, -1);
        assert!(offset.metadata.is_none());
    }

    #[test]
    fn test_offset_manager_overwrite() {
        let om = OffsetManager::new(None);

        om.commit_offset("group-1", "topic-a", 0, 10, -1, None).unwrap();
        om.commit_offset("group-1", "topic-a", 0, 20, -1, Some("meta".to_string())).unwrap();

        let offset = om.fetch_offset("group-1", "topic-a", 0).unwrap();
        assert_eq!(offset.offset, 20);
        assert_eq!(offset.metadata, Some("meta".to_string()));
    }

    #[test]
    fn test_offset_manager_fetch_nonexistent() {
        let om = OffsetManager::new(None);
        assert!(om.fetch_offset("group-1", "topic-a", 0).is_none());
    }

    #[test]
    fn test_offset_manager_fetch_for_topic() {
        let om = OffsetManager::new(None);

        om.commit_offset("group-1", "topic-a", 0, 10, -1, None).unwrap();
        om.commit_offset("group-1", "topic-a", 1, 20, -1, None).unwrap();
        om.commit_offset("group-1", "topic-b", 0, 30, -1, None).unwrap();

        let offsets = om.fetch_offsets_for_topic("group-1", "topic-a");
        assert_eq!(offsets.len(), 2);
        assert_eq!(offsets[&0].offset, 10);
        assert_eq!(offsets[&1].offset, 20);
    }

    #[test]
    fn test_offset_manager_fetch_all_for_group() {
        let om = OffsetManager::new(None);

        om.commit_offset("group-1", "topic-a", 0, 10, -1, None).unwrap();
        om.commit_offset("group-1", "topic-b", 0, 20, -1, None).unwrap();
        om.commit_offset("group-2", "topic-a", 0, 30, -1, None).unwrap();

        let all = om.fetch_all_offsets_for_group("group-1");
        assert_eq!(all.len(), 2);
        assert_eq!(all["topic-a"][&0].offset, 10);
        assert_eq!(all["topic-b"][&0].offset, 20);
    }

    #[test]
    fn test_offset_manager_delete_group() {
        let om = OffsetManager::new(None);

        om.commit_offset("group-1", "topic-a", 0, 10, -1, None).unwrap();
        om.commit_offset("group-1", "topic-b", 0, 20, -1, None).unwrap();
        om.commit_offset("group-2", "topic-a", 0, 30, -1, None).unwrap();

        om.delete_group_offsets("group-1");

        assert!(om.fetch_offset("group-1", "topic-a", 0).is_none());
        assert!(om.fetch_offset("group-1", "topic-b", 0).is_none());
        assert!(om.fetch_offset("group-2", "topic-a", 0).is_some());
    }

    #[test]
    fn test_offset_manager_list_groups() {
        let om = OffsetManager::new(None);

        om.commit_offset("group-1", "topic-a", 0, 10, -1, None).unwrap();
        om.commit_offset("group-2", "topic-a", 0, 20, -1, None).unwrap();

        let groups = om.list_groups();
        assert_eq!(groups.len(), 2);
        assert!(groups.contains(&"group-1".to_string()));
        assert!(groups.contains(&"group-2".to_string()));
    }

    #[test]
    fn test_offset_manager_count() {
        let om = OffsetManager::new(None);

        assert_eq!(om.offset_count(), 0);
        om.commit_offset("group-1", "topic-a", 0, 10, -1, None).unwrap();
        assert_eq!(om.offset_count(), 1);
        om.commit_offset("group-1", "topic-a", 0, 20, -1, None).unwrap();
        assert_eq!(om.offset_count(), 1); // overwrite, not new
        om.commit_offset("group-1", "topic-a", 1, 30, -1, None).unwrap();
        assert_eq!(om.offset_count(), 2);
    }
}
