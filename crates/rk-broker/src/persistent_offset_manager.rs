//! Persistent Offset Manager
//!
//! 在 OffsetManager 基础上增加磁盘持久化能力。
//! 定期将消费者组偏移量快照写入 JSON 文件，重启时自动恢复。
//! Phase 2: 迁移到 __consumer_offsets 内部 topic。

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use rk_core::error::Result;
use tracing::{debug, info, warn};

use crate::offset_manager::OffsetManager;

/// 持久化偏移量管理器
pub struct PersistentOffsetManager {
    /// 底层内存 OffsetManager
    inner: Arc<OffsetManager>,
    /// 持久化目录
    persist_dir: PathBuf,
    /// 快照文件名
    snapshot_file: String,
}

impl PersistentOffsetManager {
    /// 创建 PersistentOffsetManager
    pub fn new(inner: Arc<OffsetManager>, persist_dir: PathBuf) -> Self {
        Self {
            inner,
            persist_dir: persist_dir.clone(),
            snapshot_file: "offset_snapshot.json".to_string(),
        }
    }

    /// 获取底层 OffsetManager 引用
    pub fn offset_manager(&self) -> &Arc<OffsetManager> {
        &self.inner
    }

    /// 从磁盘恢复偏移量
    pub fn load_from_disk(&self) -> Result<usize> {
        let path = self.snapshot_path();
        if !path.exists() {
            info!("No offset snapshot found, starting fresh");
            return Ok(0);
        }

        let data = std::fs::read_to_string(&path)?;
        if data.is_empty() {
            return Ok(0);
        }

        let snapshot: OffsetSnapshot = match serde_json::from_str(&data) {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, "Failed to parse offset snapshot, starting fresh");
                return Ok(0);
            }
        };

        let mut count = 0usize;
        for (group_id, topic_offsets) in &snapshot.groups {
            for (topic, partition_offsets) in topic_offsets {
                for (partition_str, committed) in partition_offsets {
                    if let Ok(partition) = partition_str.parse::<i32>() {
                        self.inner.commit_offset(
                            group_id,
                            topic,
                            partition,
                            committed.offset,
                            committed.leader_epoch,
                            committed.metadata.clone(),
                        )?;
                        count += 1;
                    }
                }
            }
        }

        info!(count = count, "Loaded offsets from snapshot");
        Ok(count)
    }

    /// 将当前偏移量快照持久化到磁盘
    pub fn save_to_disk(&self) -> Result<usize> {
        let snapshot = self.build_snapshot();
        let count = snapshot.total_offsets();

        // 确保目录存在
        if !self.persist_dir.exists() {
            std::fs::create_dir_all(&self.persist_dir)?;
        }

        let json = serde_json::to_string_pretty(&snapshot).map_err(|e| {
            rk_core::error::RkError::Storage(format!("JSON serialize error: {}", e))
        })?;

        let path = self.snapshot_path();
        // 先写临时文件，再 rename (原子性)
        let tmp_path = path.with_extension("json.tmp");
        std::fs::write(&tmp_path, &json)?;
        std::fs::rename(&tmp_path, &path)?;

        debug!(count = count, path = %path.display(), "Offset snapshot saved");
        Ok(count)
    }

    /// 构建快照
    fn build_snapshot(&self) -> OffsetSnapshot {
        let mut groups: HashMap<String, HashMap<String, HashMap<String, CommittedOffsetDto>>> =
            HashMap::new();

        // 从底层 OffsetManager 获取所有 group
        for group_id in self.inner.list_groups() {
            let all_offsets = self.inner.fetch_all_offsets_for_group(&group_id);
            let mut topic_map: HashMap<String, HashMap<String, CommittedOffsetDto>> =
                HashMap::new();

            for (topic, partition_offsets) in &all_offsets {
                let mut part_map: HashMap<String, CommittedOffsetDto> = HashMap::new();
                for (partition, committed) in partition_offsets {
                    part_map.insert(
                        partition.to_string(),
                        CommittedOffsetDto {
                            offset: committed.offset,
                            leader_epoch: committed.leader_epoch,
                            metadata: committed.metadata.clone(),
                            commit_timestamp_ms: committed.commit_timestamp_ms,
                        },
                    );
                }
                topic_map.insert(topic.clone(), part_map);
            }
            groups.insert(group_id, topic_map);
        }

        OffsetSnapshot { groups }
    }

    fn snapshot_path(&self) -> PathBuf {
        self.persist_dir.join(&self.snapshot_file)
    }
}

// ─── 序列化结构 ──────────────────────────────────────────────────────

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct OffsetSnapshot {
    groups: HashMap<String, HashMap<String, HashMap<String, CommittedOffsetDto>>>,
}

impl OffsetSnapshot {
    fn total_offsets(&self) -> usize {
        self.groups
            .values()
            .flat_map(|t| t.values())
            .map(|p| p.len())
            .sum()
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
struct CommittedOffsetDto {
    offset: i64,
    leader_epoch: i32,
    metadata: Option<String>,
    commit_timestamp_ms: i64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_persistent_offset_manager_save_and_load() {
        let dir = tempdir().unwrap();
        let persist_dir = dir.path().to_path_buf();

        // Phase 1: 写入偏移量并持久化
        {
            let om = Arc::new(OffsetManager::new(None));
            om.commit_offset("group-1", "topic-a", 0, 42, -1, None)
                .unwrap();
            om.commit_offset("group-1", "topic-a", 1, 100, -1, Some("meta".to_string()))
                .unwrap();
            om.commit_offset("group-1", "topic-b", 0, 200, -1, None)
                .unwrap();
            om.commit_offset("group-2", "topic-a", 0, 300, -1, None)
                .unwrap();

            let pom = PersistentOffsetManager::new(om.clone(), persist_dir.clone());
            let saved = pom.save_to_disk().unwrap();
            assert_eq!(saved, 4);
        }

        // Phase 2: 从磁盘恢复
        {
            let om2 = Arc::new(OffsetManager::new(None));
            let pom2 = PersistentOffsetManager::new(om2.clone(), persist_dir.clone());
            let loaded = pom2.load_from_disk().unwrap();
            assert_eq!(loaded, 4);

            // 验证数据完整
            assert_eq!(
                om2.fetch_offset("group-1", "topic-a", 0).unwrap().offset,
                42
            );
            assert_eq!(
                om2.fetch_offset("group-1", "topic-a", 1).unwrap().offset,
                100
            );
            assert_eq!(
                om2.fetch_offset("group-1", "topic-a", 1).unwrap().metadata,
                Some("meta".to_string())
            );
            assert_eq!(
                om2.fetch_offset("group-1", "topic-b", 0).unwrap().offset,
                200
            );
            assert_eq!(
                om2.fetch_offset("group-2", "topic-a", 0).unwrap().offset,
                300
            );
        }
    }

    #[test]
    fn test_persistent_offset_manager_load_empty() {
        let dir = tempdir().unwrap();
        let persist_dir = dir.path().to_path_buf();

        let om = Arc::new(OffsetManager::new(None));
        let pom = PersistentOffsetManager::new(om, persist_dir);
        let loaded = pom.load_from_disk().unwrap();
        assert_eq!(loaded, 0);
    }

    #[test]
    fn test_persistent_offset_manager_load_corrupted() {
        let dir = tempdir().unwrap();
        let persist_dir = dir.path().to_path_buf();
        std::fs::create_dir_all(&persist_dir).unwrap();
        std::fs::write(
            persist_dir.join("offset_snapshot.json"),
            "not valid json{{{",
        )
        .unwrap();

        let om = Arc::new(OffsetManager::new(None));
        let pom = PersistentOffsetManager::new(om, persist_dir);
        let loaded = pom.load_from_disk().unwrap();
        assert_eq!(loaded, 0); // 损坏文件应被跳过
    }

    #[test]
    fn test_persistent_offset_manager_overwrite_on_load() {
        let dir = tempdir().unwrap();
        let persist_dir = dir.path().to_path_buf();

        // 保存快照
        {
            let om = Arc::new(OffsetManager::new(None));
            om.commit_offset("group-1", "topic-a", 0, 10, -1, None)
                .unwrap();
            let pom = PersistentOffsetManager::new(om, persist_dir.clone());
            pom.save_to_disk().unwrap();
        }

        // 加载到已有数据的 manager
        {
            let om2 = Arc::new(OffsetManager::new(None));
            om2.commit_offset("group-1", "topic-a", 0, 999, -1, None)
                .unwrap();

            let pom2 = PersistentOffsetManager::new(om2.clone(), persist_dir.clone());
            pom2.load_from_disk().unwrap();

            // 快照中的值应覆盖内存中的值
            assert_eq!(
                om2.fetch_offset("group-1", "topic-a", 0).unwrap().offset,
                10
            );
        }
    }
}
