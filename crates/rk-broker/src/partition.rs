//! Partition Manager
//!
//! 管理所有 Topic-Partition 的 CommitLog。
//! Phase 1: DashMap<PartitionKey, CommitLog> 直接管理。
//! Phase 2: 迁移到 Thread-per-Core Actor 模型 (消息传递)。
//!
//! 设计说明:
//! - 每个 (topic, partition) 唯一对应一个 CommitLog
//! - DashMap 提供分片锁，不同 partition 可并发读写
//! - 同一 partition 的写操作串行化 (保证 offset 单调递增)

use std::collections::HashMap;
use std::path::PathBuf;

use dashmap::DashMap;
use rk_core::error::{Result, RkError};
use rk_core::types::{Offset, PartitionId, TopicName};
use rk_storage::recovery::recover_partition;
use rk_storage::CommitLog;
use tracing::{info, warn};

/// Partition 唯一键: (TopicName, PartitionId)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PartitionKey {
    pub topic: TopicName,
    pub partition: PartitionId,
}

impl PartitionKey {
    pub fn new(topic: TopicName, partition: PartitionId) -> Self {
        Self { topic, partition }
    }
}

/// Topic 元数据 (Phase 1 简化版)
#[derive(Debug, Clone)]
pub struct TopicMetadata {
    pub name: TopicName,
    pub partition_count: u32,
    pub replication_factor: u16,
    /// 是否为内部 Topic (如 __consumer_offsets)
    pub is_internal: bool,
}

/// Partition Manager 恢复结果
#[derive(Debug)]
pub struct PartitionManagerRecoveryResult {
    /// 恢复的 Topic 数量
    pub topics_recovered: usize,
    /// 恢复的 Partition 数量
    pub partitions_recovered: usize,
}

/// Partition Manager: 管理所有 Partition 的 CommitLog
pub struct PartitionManager {
    /// 数据根目录
    data_dir: PathBuf,
    /// Segment 最大大小
    segment_max_size: u64,
    /// Partition → CommitLog 映射 (分片锁)
    partitions: DashMap<PartitionKey, CommitLog>,
    /// Topic 元数据
    topics: DashMap<TopicName, TopicMetadata>,
    /// Topic 配置覆盖 (运行时, 不持久化)
    topic_configs: DashMap<TopicName, HashMap<String, String>>,
    /// Broker ID
    broker_id: i32,
}

impl PartitionManager {
    /// 创建新的 (空) PartitionManager
    pub fn new(data_dir: PathBuf, segment_max_size: u64, broker_id: i32) -> Self {
        Self {
            data_dir,
            segment_max_size,
            partitions: DashMap::new(),
            topics: DashMap::new(),
            topic_configs: DashMap::new(),
            broker_id,
        }
    }

    /// 从数据目录恢复 PartitionManager
    ///
    /// 扫描 data_dir 下所有 `{topic}-{partition}/` 目录，
    /// 使用 Crash Recovery 恢复每个 Partition 的 CommitLog。
    pub fn recover(
        data_dir: PathBuf,
        segment_max_size: u64,
        broker_id: i32,
    ) -> Result<(Self, PartitionManagerRecoveryResult)> {
        let pm = Self::new(data_dir.clone(), segment_max_size, broker_id);

        if !data_dir.exists() {
            std::fs::create_dir_all(&data_dir)?;
            info!(data_dir = %data_dir.display(), "Created new data directory");
            return Ok((
                pm,
                PartitionManagerRecoveryResult {
                    topics_recovered: 0,
                    partitions_recovered: 0,
                },
            ));
        }

        // 扫描 data_dir 下的子目录，解析 {topic}-{partition} 格式
        let mut topic_partitions: Vec<(String, i32)> = Vec::new();

        for entry in std::fs::read_dir(&data_dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let dir_name = entry.file_name();
            let dir_name = dir_name.to_string_lossy();

            // 格式: {topic}-{partition_id}
            // topic 名可包含连字符，所以从最后一个 '-' 分割
            if let Some(last_dash) = dir_name.rfind('-') {
                let topic_name = &dir_name[..last_dash];
                let partition_str = &dir_name[last_dash + 1..];
                if let Ok(partition_id) = partition_str.parse::<i32>() {
                    if !topic_name.is_empty() {
                        topic_partitions.push((topic_name.to_string(), partition_id));
                    }
                }
            }
        }

        if topic_partitions.is_empty() {
            info!("No existing partitions found in {}", data_dir.display());
            return Ok((
                pm,
                PartitionManagerRecoveryResult {
                    topics_recovered: 0,
                    partitions_recovered: 0,
                },
            ));
        }

        // 按 topic 分组，计算每个 topic 的 partition 数
        let mut topic_map: std::collections::HashMap<String, Vec<i32>> =
            std::collections::HashMap::new();
        for (topic, pid) in &topic_partitions {
            topic_map.entry(topic.clone()).or_default().push(*pid);
        }

        let mut total_partitions = 0usize;

        // 恢复每个 topic-partition
        for (topic_name, partition_ids) in &mut topic_map {
            partition_ids.sort();
            let partition_count = partition_ids.len() as u32;

            // 创建 topic 元数据
            let meta = TopicMetadata {
                name: TopicName(topic_name.clone()),
                partition_count,
                replication_factor: 1,
                is_internal: topic_name.starts_with("__"),
            };
            pm.topics.insert(TopicName(topic_name.clone()), meta);

            for &pid in partition_ids.iter() {
                match recover_partition(
                    &data_dir,
                    TopicName(topic_name.clone()),
                    PartitionId(pid),
                    segment_max_size,
                ) {
                    Ok((cl, recovery_result)) => {
                        if recovery_result.bytes_truncated > 0 {
                            warn!(
                                topic = %topic_name,
                                partition = pid,
                                bytes_truncated = recovery_result.bytes_truncated,
                                "Crash recovery truncated corrupted data"
                            );
                        }
                        let key =
                            PartitionKey::new(TopicName(topic_name.clone()), PartitionId(pid));
                        pm.partitions.insert(key, cl);
                        total_partitions += 1;
                    }
                    Err(e) => {
                        warn!(
                            topic = %topic_name,
                            partition = pid,
                            error = %e,
                            "Failed to recover partition, skipping"
                        );
                    }
                }
            }
        }

        info!(
            topics = topic_map.len(),
            partitions = total_partitions,
            data_dir = %data_dir.display(),
            "Partition recovery complete"
        );

        Ok((
            pm,
            PartitionManagerRecoveryResult {
                topics_recovered: topic_map.len(),
                partitions_recovered: total_partitions,
            },
        ))
    }

    /// 创建或获取 Topic (自动创建 Topic)
    pub fn get_or_create_topic(&self, topic_name: &str, partition_count: u32) -> TopicMetadata {
        let name = TopicName(topic_name.to_string());
        if let Some(meta) = self.topics.get(&name) {
            return meta.clone();
        }

        let meta = TopicMetadata {
            name: name.clone(),
            partition_count,
            replication_factor: 1, // Phase 1: 单副本
            is_internal: topic_name.starts_with("__"),
        };
        self.topics.insert(name.clone(), meta.clone());

        // 为每个 partition 创建 CommitLog
        for p in 0..partition_count {
            let key = PartitionKey::new(name.clone(), PartitionId(p as i32));
            if !self.partitions.contains_key(&key) {
                if let Ok(cl) = CommitLog::create(
                    &self.data_dir,
                    name.clone(),
                    PartitionId(p as i32),
                    self.segment_max_size,
                ) {
                    self.partitions.insert(key, cl);
                }
            }
        }

        meta
    }

    /// 确保指定 partition 存在
    pub fn ensure_partition(&self, topic_name: &str, partition_id: i32) -> Result<()> {
        let name = TopicName(topic_name.to_string());
        let key = PartitionKey::new(name.clone(), PartitionId(partition_id));

        if !self.partitions.contains_key(&key) {
            let cl = CommitLog::create(
                &self.data_dir,
                name,
                PartitionId(partition_id),
                self.segment_max_size,
            )?;
            self.partitions.insert(key, cl);
        }
        Ok(())
    }

    /// 追加写入 RecordBatch 到指定 partition
    ///
    /// 返回该 batch 的 base_offset
    pub fn append_batch(&self, topic: &str, partition: i32, batch_bytes: &[u8]) -> Result<Offset> {
        let key = PartitionKey::new(TopicName(topic.to_string()), PartitionId(partition));

        // 确保 partition 存在
        self.ensure_partition(topic, partition)?;

        let mut entry = self.partitions.get_mut(&key).ok_or_else(|| {
            RkError::Storage(format!("Partition {}-{} not found", topic, partition))
        })?;

        let offset = entry.append_batch(batch_bytes)?;

        // Phase 1: 单 Broker 模式下 HW = LEO (写入立即可见)
        let leo = entry.log_end_offset();
        entry.set_high_watermark(leo);

        Ok(offset)
    }

    /// 从指定 partition 读取数据 (从 offset 开始, 最多 max_bytes)
    ///
    /// 返回 RecordBatch 字节数组的 Vec
    pub fn read_batches(
        &self,
        topic: &str,
        partition: i32,
        start_offset: Offset,
        max_bytes: usize,
    ) -> Result<Vec<Vec<u8>>> {
        let key = PartitionKey::new(TopicName(topic.to_string()), PartitionId(partition));

        let mut entry = self.partitions.get_mut(&key).ok_or_else(|| {
            RkError::Storage(format!("Partition {}-{} not found", topic, partition))
        })?;

        let batches = entry.read_range(start_offset, max_bytes)?;
        Ok(batches.into_iter().map(|(bytes, _pos)| bytes).collect())
    }

    /// 获取 partition 的 Log End Offset
    pub fn log_end_offset(&self, topic: &str, partition: i32) -> Result<Offset> {
        let key = PartitionKey::new(TopicName(topic.to_string()), PartitionId(partition));

        let entry = self.partitions.get(&key).ok_or_else(|| {
            RkError::Storage(format!("Partition {}-{} not found", topic, partition))
        })?;

        Ok(entry.log_end_offset())
    }

    /// 获取 partition 的 High Watermark
    pub fn high_watermark(&self, topic: &str, partition: i32) -> Result<Offset> {
        let key = PartitionKey::new(TopicName(topic.to_string()), PartitionId(partition));

        let entry = self.partitions.get(&key).ok_or_else(|| {
            RkError::Storage(format!("Partition {}-{} not found", topic, partition))
        })?;

        Ok(entry.high_watermark())
    }

    /// 按 timestamp 查找 offset (用于 ListOffsets)
    pub fn offset_for_timestamp(
        &self,
        topic: &str,
        partition: i32,
        timestamp: i64,
    ) -> Result<Option<Offset>> {
        let key = PartitionKey::new(TopicName(topic.to_string()), PartitionId(partition));

        let mut entry = self.partitions.get_mut(&key).ok_or_else(|| {
            RkError::Storage(format!("Partition {}-{} not found", topic, partition))
        })?;

        match entry.read_at_timestamp(timestamp)? {
            Some((batch_bytes, _)) => {
                // 从 batch header 中提取 base_offset
                use rk_protocol::record::{decode_batch_header, HEADER_SIZE};
                use rk_protocol::types::KafkaReader;
                if batch_bytes.len() >= HEADER_SIZE {
                    let mut reader = KafkaReader::new(&batch_bytes);
                    let hdr = decode_batch_header(&mut reader)?;
                    Ok(Some(Offset(hdr.base_offset)))
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }

    /// 获取所有 topic 名称
    pub fn list_topics(&self) -> Vec<TopicName> {
        self.topics.iter().map(|e| e.key().clone()).collect()
    }

    /// 获取 topic 元数据
    pub fn get_topic_metadata(&self, topic_name: &str) -> Option<TopicMetadata> {
        let name = TopicName(topic_name.to_string());
        self.topics.get(&name).map(|e| e.clone())
    }

    /// 获取所有 topic 的元数据
    pub fn get_all_topic_metadata(&self) -> Vec<TopicMetadata> {
        self.topics.iter().map(|e| e.value().clone()).collect()
    }

    /// 获取 topic 的所有 partition 信息
    pub fn get_partition_infos(&self, topic_name: &str) -> Vec<PartitionInfo> {
        let name = TopicName(topic_name.to_string());
        let meta = match self.topics.get(&name) {
            Some(m) => m.clone(),
            None => return Vec::new(),
        };

        let mut infos = Vec::new();
        for p in 0..meta.partition_count {
            let key = PartitionKey::new(name.clone(), PartitionId(p as i32));
            if let Some(entry) = self.partitions.get(&key) {
                infos.push(PartitionInfo {
                    partition_id: p as i32,
                    leader: self.broker_id,
                    leader_epoch: 0,
                    log_end_offset: entry.log_end_offset(),
                    high_watermark: entry.high_watermark(),
                    replicas: vec![self.broker_id],
                    isr: vec![self.broker_id],
                });
            }
        }
        infos
    }

    /// 获取 Broker ID
    pub fn broker_id(&self) -> i32 {
        self.broker_id
    }

    /// 删除 Topic (移除所有 partition 和元数据)
    pub fn delete_topic(&self, topic_name: &str) {
        let name = TopicName(topic_name.to_string());

        // 移除 topic 元数据
        self.topics.remove(&name);
        // 移除 topic 配置
        self.topic_configs.remove(&name);

        // 移除所有相关的 partition
        let keys_to_remove: Vec<PartitionKey> = self
            .partitions
            .iter()
            .filter(|e| e.key().topic == name)
            .map(|e| e.key().clone())
            .collect();

        for key in keys_to_remove {
            self.partitions.remove(&key);
        }
    }

    // ─── 扩展查询/管理 ─────────────────────────────────────────────────

    /// 获取 partition 的 LEO (返回 Option，不报错)
    pub fn get_log_end_offset(&self, key: &PartitionKey) -> Option<Offset> {
        self.partitions.get(key).map(|e| e.log_end_offset())
    }

    /// 获取 topic 的当前 partition 数量
    pub fn get_partition_count(&self, topic_name: &str) -> Option<i32> {
        let name = TopicName(topic_name.to_string());
        self.topics.get(&name).map(|m| m.partition_count as i32)
    }

    /// 为已有 topic 增加 partition (从当前数量扩展到 new_total)
    pub fn add_partitions(&self, topic_name: &str, new_total: i32) -> bool {
        let name = TopicName(topic_name.to_string());
        if let Some(meta_ref) = self.topics.get(&name) {
            let mut meta = meta_ref.clone();
            let current = meta.partition_count as i32;
            if new_total <= current {
                return false;
            }
            // 更新 topic 元数据中的 partition 数量
            meta.partition_count = new_total as u32;
            drop(meta_ref);
            self.topics.insert(name.clone(), meta);

            // 创建新的 partition
            for pid in current..new_total {
                let key = PartitionKey::new(name.clone(), PartitionId(pid));
                if !self.partitions.contains_key(&key) {
                    if let Ok(cl) = CommitLog::create(
                        &self.data_dir,
                        name.clone(),
                        PartitionId(pid),
                        self.segment_max_size,
                    ) {
                        self.partitions.insert(key, cl);
                    }
                }
            }
            true
        } else {
            false
        }
    }

    /// 删除指定 partition 中指定偏移量之前的记录 (前移 log_start_offset)
    ///
    /// 返回新的 log_start_offset，如果 partition 不存在返回 None。
    pub fn delete_records(&self, key: &PartitionKey, before_offset: i64) -> Option<i64> {
        if let Some(mut entry) = self.partitions.get_mut(key) {
            if let Ok(new_lso) = entry.advance_log_start_offset(before_offset) {
                return Some(new_lso.0);
            }
        }
        None
    }

    // ─── Topic 配置管理 ─────────────────────────────────────────────────

    /// 获取 Topic 配置 (覆盖 + 默认值)
    pub fn get_topic_config(&self, topic_name: &str, key: &str) -> Option<String> {
        let name = TopicName(topic_name.to_string());
        self.topic_configs
            .get(&name)
            .and_then(|configs| configs.get(key).cloned())
    }

    /// 获取 Topic 全部配置覆盖
    pub fn get_topic_all_configs(&self, topic_name: &str) -> HashMap<String, String> {
        let name = TopicName(topic_name.to_string());
        self.topic_configs
            .get(&name)
            .map(|c| c.clone())
            .unwrap_or_default()
    }

    /// 设置 Topic 配置 (运行时覆盖)
    pub fn set_topic_config(&self, topic_name: &str, key: &str, value: &str) {
        let name = TopicName(topic_name.to_string());
        let mut entry = self.topic_configs.entry(name).or_default();
        entry.insert(key.to_string(), value.to_string());
    }

    /// 批量设置 Topic 配置
    pub fn set_topic_configs(&self, topic_name: &str, configs: HashMap<String, String>) {
        let name = TopicName(topic_name.to_string());
        let mut entry = self.topic_configs.entry(name).or_default();
        for (k, v) in configs {
            entry.insert(k, v);
        }
    }

    /// 删除 Topic 配置项 (恢复默认)
    pub fn remove_topic_config(&self, topic_name: &str, key: &str) {
        let name = TopicName(topic_name.to_string());
        if let Some(mut entry) = self.topic_configs.get_mut(&name) {
            entry.remove(key);
        }
    }
}

/// Partition 运行时信息
#[derive(Debug, Clone)]
pub struct PartitionInfo {
    pub partition_id: i32,
    pub leader: i32,
    pub leader_epoch: i32,
    pub log_end_offset: Offset,
    pub high_watermark: Offset,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rk_storage::log_io::build_batch_bytes;
    use tempfile::tempdir;

    fn make_batch(base_offset: i64, record_count: i32) -> Vec<u8> {
        let records = vec![0u8; record_count as usize * 10];
        build_batch_bytes(
            base_offset,
            1,
            0,
            1000,
            2000,
            -1,
            -1,
            -1,
            &records,
            record_count,
        )
    }

    #[test]
    fn test_partition_manager_create_topic() {
        let dir = tempdir().unwrap();
        let pm = PartitionManager::new(dir.path().to_path_buf(), 1_073_741_824, 1);

        let meta = pm.get_or_create_topic("test-topic", 3);
        assert_eq!(meta.name.0, "test-topic");
        assert_eq!(meta.partition_count, 3);

        // 再次获取应返回已有 topic
        let meta2 = pm.get_or_create_topic("test-topic", 3);
        assert_eq!(meta2.partition_count, 3);
    }

    #[test]
    fn test_partition_manager_append_and_read() {
        let dir = tempdir().unwrap();
        let pm = PartitionManager::new(dir.path().to_path_buf(), 1_073_741_824, 1);

        pm.get_or_create_topic("test", 1);

        let batch = make_batch(0, 5);
        let offset = pm.append_batch("test", 0, &batch).unwrap();
        assert_eq!(offset, Offset(0));

        let leo = pm.log_end_offset("test", 0).unwrap();
        assert_eq!(leo, Offset(5));

        let batches = pm.read_batches("test", 0, Offset(0), 1_000_000).unwrap();
        assert_eq!(batches.len(), 1);
    }

    #[test]
    fn test_partition_manager_multiple_partitions() {
        let dir = tempdir().unwrap();
        let pm = PartitionManager::new(dir.path().to_path_buf(), 1_073_741_824, 1);

        pm.get_or_create_topic("test", 3);

        // 写入不同 partition
        pm.append_batch("test", 0, &make_batch(0, 3)).unwrap();
        pm.append_batch("test", 1, &make_batch(0, 5)).unwrap();
        pm.append_batch("test", 2, &make_batch(0, 2)).unwrap();

        assert_eq!(pm.log_end_offset("test", 0).unwrap(), Offset(3));
        assert_eq!(pm.log_end_offset("test", 1).unwrap(), Offset(5));
        assert_eq!(pm.log_end_offset("test", 2).unwrap(), Offset(2));
    }

    #[test]
    fn test_partition_manager_list_topics() {
        let dir = tempdir().unwrap();
        let pm = PartitionManager::new(dir.path().to_path_buf(), 1_073_741_824, 1);

        pm.get_or_create_topic("topic-a", 1);
        pm.get_or_create_topic("topic-b", 2);

        let topics = pm.list_topics();
        assert_eq!(topics.len(), 2);
    }

    #[test]
    fn test_partition_manager_high_watermark() {
        let dir = tempdir().unwrap();
        let pm = PartitionManager::new(dir.path().to_path_buf(), 1_073_741_824, 1);

        pm.get_or_create_topic("test", 1);
        pm.append_batch("test", 0, &make_batch(0, 10)).unwrap();

        // Phase 1: HW = LEO (单副本，写入立即可见)
        let hw = pm.high_watermark("test", 0).unwrap();
        assert_eq!(hw, Offset(10)); // append 10 records → LEO=10, HW=10
    }

    #[test]
    fn test_partition_manager_partition_info() {
        let dir = tempdir().unwrap();
        let pm = PartitionManager::new(dir.path().to_path_buf(), 1_073_741_824, 1);

        pm.get_or_create_topic("test", 2);
        pm.append_batch("test", 0, &make_batch(0, 5)).unwrap();

        let infos = pm.get_partition_infos("test");
        assert_eq!(infos.len(), 2);
        assert_eq!(infos[0].leader, 1);
        assert_eq!(infos[0].log_end_offset, Offset(5));
        assert_eq!(infos[1].log_end_offset, Offset(0));
    }

    #[test]
    fn test_partition_manager_recovery() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_path_buf();

        // Phase 1: 创建数据并写入
        {
            let pm = PartitionManager::new(data_dir.clone(), 1_073_741_824, 1);
            pm.get_or_create_topic("my-topic", 2);
            pm.append_batch("my-topic", 0, &make_batch(0, 5)).unwrap();
            pm.append_batch("my-topic", 0, &make_batch(5, 3)).unwrap();
            pm.append_batch("my-topic", 1, &make_batch(0, 10)).unwrap();

            // 刷盘确保持久化
            let key = PartitionKey::new(TopicName("my-topic".into()), PartitionId(0));
            let entry = pm.partitions.get(&key).unwrap();
            entry.flush_all().unwrap();
            let key1 = PartitionKey::new(TopicName("my-topic".into()), PartitionId(1));
            let entry1 = pm.partitions.get(&key1).unwrap();
            entry1.flush_all().unwrap();
        }
        // pm 被 drop

        // Phase 2: "重启" — 从磁盘恢复
        let (pm2, result) = PartitionManager::recover(data_dir.clone(), 1_073_741_824, 1).unwrap();

        assert_eq!(result.topics_recovered, 1);
        assert_eq!(result.partitions_recovered, 2);

        // 验证数据完整
        let leo0 = pm2.log_end_offset("my-topic", 0).unwrap();
        assert_eq!(leo0, Offset(8)); // 5 + 3 = 8

        let leo1 = pm2.log_end_offset("my-topic", 1).unwrap();
        assert_eq!(leo1, Offset(10));

        // 验证 topic 元数据恢复
        let meta = pm2.get_topic_metadata("my-topic").unwrap();
        assert_eq!(meta.partition_count, 2);

        // 验证数据可读
        let batches = pm2
            .read_batches("my-topic", 0, Offset(0), 1_000_000)
            .unwrap();
        assert!(!batches.is_empty());
    }

    #[test]
    fn test_partition_manager_recovery_empty_dir() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_path_buf();

        let (pm, result) = PartitionManager::recover(data_dir, 1_073_741_824, 1).unwrap();

        assert_eq!(result.topics_recovered, 0);
        assert_eq!(result.partitions_recovered, 0);
        assert!(pm.list_topics().is_empty());
    }

    #[test]
    fn test_partition_manager_recovery_nonexistent_dir() {
        let data_dir = std::path::PathBuf::from("/tmp/rk-test-nonexistent-dir-12345");
        // 确保目录不存在
        let _ = std::fs::remove_dir_all(&data_dir);

        let (_pm, result) = PartitionManager::recover(data_dir.clone(), 1_073_741_824, 1).unwrap();

        assert_eq!(result.topics_recovered, 0);
        assert_eq!(result.partitions_recovered, 0);

        // 目录应被创建
        assert!(data_dir.exists());

        // 清理
        let _ = std::fs::remove_dir_all(&data_dir);
    }
}
