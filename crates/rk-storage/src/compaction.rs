//! Log Compaction — 日志压缩
//!
//! 实现 Kafka 风格的 Log Compaction，保留每个 Key 的最新值:
//!
//! ```text
//! Compaction 流程:
//!
//! ┌─────────────────────────────────────────────┐
//! │  Before Compaction                          │
//! │  Segments: [k1=v1, k2=v2, k1=v3, k2=v4]   │
//! └────────────────────┬────────────────────────┘
//!                      │ LogCleaner
//!                      │ 1. build_key_index (扫描所有 batch)
//!                      │ 2. 保留每个 key 的最新值
//!                      │ 3. rewrite_compacted_log
//!                      ▼
//! ┌─────────────────────────────────────────────┐
//! │  After Compaction                           │
//! │  Segments: [k1=v3, k2=v4]                  │
//! └─────────────────────────────────────────────┘
//! ```
//!
//! ## Cleanup Policy
//!
//! - `Delete`: 基于时间/大小删除旧 segment (现有 retention)
//! - `Compact`: 保留每个 key 的最新值
//! - `CompactDelete`: 两者结合
//!
//! ## Tombstone
//!
//! key 存在但 value 为 null 的记录称为 tombstone。
//! Compaction 保留 tombstone 一段时间 (delete_retention_ms) 后删除。

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use tracing::{debug, info};

use rk_core::error::Result;
use rk_core::types::Offset;

// ─── Cleanup Policy ─────────────────────────────────────────────────

/// 日志清理策略
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CleanupPolicy {
    /// 基于时间/大小删除旧 segment
    Delete,
    /// 保留每个 key 的最新值
    Compact,
    /// 两者结合
    CompactDelete,
}

impl CleanupPolicy {
    /// 从配置字符串解析
    pub fn from_str(s: &str) -> Self {
        match s.trim().to_lowercase().as_str() {
            "compact" => CleanupPolicy::Compact,
            "compact,delete" | "delete,compact" => CleanupPolicy::CompactDelete,
            _ => CleanupPolicy::Delete,
        }
    }

    /// 是否需要 compaction
    pub fn needs_compaction(&self) -> bool {
        matches!(self, CleanupPolicy::Compact | CleanupPolicy::CompactDelete)
    }

    /// 是否需要 retention 删除
    pub fn needs_deletion(&self) -> bool {
        matches!(self, CleanupPolicy::Delete | CleanupPolicy::CompactDelete)
    }
}

impl std::fmt::Display for CleanupPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CleanupPolicy::Delete => write!(f, "delete"),
            CleanupPolicy::Compact => write!(f, "compact"),
            CleanupPolicy::CompactDelete => write!(f, "compact,delete"),
        }
    }
}

// ─── Compaction 配置 ────────────────────────────────────────────────

/// Compaction 配置
#[derive(Debug, Clone)]
pub struct CompactionConfig {
    /// 清理策略
    pub cleanup_policy: CleanupPolicy,
    /// Tombstone 保留时间 (ms), 超过后可删除
    pub delete_retention_ms: i64,
    /// 最小可压缩比例 (0.0-1.0), 低于此值不执行 compaction
    pub min_compaction_ratio: f64,
    /// 最小 dirty 字节数 (低于此值不触发)
    pub min_dirty_bytes: u64,
    /// 最大 dirty 日志比例 (dirty_bytes / total_bytes)
    pub max_dirty_ratio: f64,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            cleanup_policy: CleanupPolicy::Delete,
            delete_retention_ms: 86_400_000, // 24 hours
            min_compaction_ratio: 0.5,
            min_dirty_bytes: 1024 * 1024, // 1MB
            max_dirty_ratio: 0.5,
        }
    }
}

// ─── Key-Value 记录 ─────────────────────────────────────────────────

/// 逻辑记录 (从 batch 中提取的 key-value)
#[derive(Debug, Clone)]
pub struct KeyValueRecord {
    /// 消息 key (None = 无 key, 不参与 compaction)
    pub key: Option<Vec<u8>>,
    /// 消息 value (None = tombstone)
    pub value: Option<Vec<u8>>,
    /// 消息 offset
    pub offset: Offset,
    /// 消息时间戳
    pub timestamp: i64,
    /// 原始 batch 字节 (用于重写)
    pub raw_bytes: Vec<u8>,
}

impl KeyValueRecord {
    /// 是否为 tombstone (key 存在但 value 为 null)
    pub fn is_tombstone(&self) -> bool {
        self.key.is_some() && self.value.is_none()
    }

    /// 是否有 key (无 key 的记录不参与 compaction)
    pub fn has_key(&self) -> bool {
        self.key.is_some()
    }
}

// ─── Key 索引 ───────────────────────────────────────────────────────

/// Key 在日志中的位置
#[derive(Debug, Clone)]
pub struct KeyPosition {
    /// 记录 offset
    pub offset: Offset,
    /// 记录时间戳
    pub timestamp: i64,
    /// 在索引中的位置 (segment_idx, byte_offset)
    pub segment_idx: usize,
    pub byte_offset: u64,
    /// 原始 batch 字节大小
    pub batch_size: u32,
}

/// Key 索引 — 记录每个 key 的最新位置
#[derive(Debug)]
pub struct KeyIndex {
    /// key → 最新位置
    entries: HashMap<Vec<u8>, KeyPosition>,
    /// 总 key 数量 (含覆盖)
    total_keys_seen: u64,
    /// 唯一 key 数量
    unique_keys: u64,
}

impl KeyIndex {
    /// 创建空索引
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
            total_keys_seen: 0,
            unique_keys: 0,
        }
    }

    /// 插入或更新 key 位置
    pub fn upsert(&mut self, key: Vec<u8>, position: KeyPosition) {
        self.total_keys_seen += 1;
        if !self.entries.contains_key(&key) {
            self.unique_keys += 1;
        }
        self.entries.insert(key, position);
    }

    /// 获取 key 的最新位置
    pub fn get(&self, key: &[u8]) -> Option<&KeyPosition> {
        self.entries.get(key)
    }

    /// 所有唯一 key 的数量
    pub fn unique_key_count(&self) -> u64 {
        self.unique_keys
    }

    /// 总共看到的 key 数量 (含覆盖)
    pub fn total_keys_seen(&self) -> u64 {
        self.total_keys_seen
    }

    /// 可压缩的 key 数量 (total - unique)
    pub fn compactable_count(&self) -> u64 {
        self.total_keys_seen.saturating_sub(self.unique_keys)
    }

    /// 获取所有保留的记录位置 (按 offset 排序)
    pub fn retained_positions(&self) -> Vec<&KeyPosition> {
        let mut positions: Vec<&KeyPosition> = self.entries.values().collect();
        positions.sort_by_key(|p| p.offset);
        positions
    }

    /// 获取所有 key
    pub fn keys(&self) -> Vec<&Vec<u8>> {
        self.entries.keys().collect()
    }
}

impl Default for KeyIndex {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Compaction 结果 ────────────────────────────────────────────────

/// Compaction 执行结果
#[derive(Debug, Clone)]
pub struct CompactionResult {
    /// 扫描的 segment 数量
    pub segments_scanned: usize,
    /// 扫描的总字节数
    pub total_bytes_scanned: u64,
    /// 唯一 key 数量
    pub unique_keys: u64,
    /// 删除的重复记录数
    pub records_removed: u64,
    /// 释放的字节数
    pub bytes_freed: u64,
    /// 压缩后字节数
    pub compacted_bytes: u64,
    /// 删除的 tombstone 数量
    pub tombstones_removed: u64,
    /// 压缩比 (compacted / original)
    pub compaction_ratio: f64,
    /// 执行时间 (ms)
    pub duration_ms: u64,
}

impl std::fmt::Display for CompactionResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Compaction(scanned={}B, keys={}, removed={}, freed={}B, ratio={:.2}, {}ms)",
            self.total_bytes_scanned,
            self.unique_keys,
            self.records_removed,
            self.bytes_freed,
            self.compaction_ratio,
            self.duration_ms,
        )
    }
}

// ─── LogCleaner ─────────────────────────────────────────────────────

/// 日志压缩器
///
/// 扫描 CommitLog 中的 sealed segments，构建 key→latest 索引，
/// 重写 compacted log 只保留每个 key 的最新值。
pub struct LogCleaner {
    config: CompactionConfig,
}

impl LogCleaner {
    /// 创建日志压缩器
    pub fn new(config: CompactionConfig) -> Self {
        Self { config }
    }

    /// 获取配置
    pub fn config(&self) -> &CompactionConfig {
        &self.config
    }

    /// 从记录列表构建 key 索引
    ///
    /// 遍历所有记录，为每个 key 保留最新 (最大 offset) 的位置。
    pub fn build_key_index(&self, records: &[KeyValueRecord]) -> KeyIndex {
        let mut index = KeyIndex::new();

        for record in records.iter() {
            if let Some(ref key) = record.key {
                let position = KeyPosition {
                    offset: record.offset,
                    timestamp: record.timestamp,
                    segment_idx: 0, // 简化: 不跟踪 segment
                    byte_offset: 0,
                    batch_size: record.raw_bytes.len() as u32,
                };
                index.upsert(key.clone(), position);
            }
        }

        debug!(
            total_keys = index.total_keys_seen(),
            unique_keys = index.unique_key_count(),
            compactable = index.compactable_count(),
            "Key index built"
        );

        index
    }

    /// 确定哪些记录应该保留
    ///
    /// 规则:
    /// 1. 无 key 的记录始终保留
    /// 2. 有 key 的记录只保留最新值
    /// 3. Tombstone 在保留期内保留
    pub fn determine_retained_records(
        &self,
        records: &[KeyValueRecord],
        key_index: &KeyIndex,
        now_ms: i64,
    ) -> Vec<usize> {
        let mut retained = Vec::new();

        for (idx, record) in records.iter().enumerate() {
            match &record.key {
                None => {
                    // 无 key → 始终保留
                    retained.push(idx);
                }
                Some(key) => {
                    // 有 key → 检查是否为最新版本
                    if let Some(pos) = key_index.get(key) {
                        if pos.offset == record.offset {
                            // 这是最新版本
                            if record.is_tombstone() {
                                // Tombstone: 检查是否过期
                                let age_ms = now_ms - record.timestamp;
                                if age_ms < self.config.delete_retention_ms {
                                    retained.push(idx); // 未过期 → 保留
                                }
                                // 过期 → 不保留 (跳过)
                            } else {
                                retained.push(idx); // 非 tombstone → 保留
                            }
                        }
                        // 不是最新版本 → 跳过
                    }
                }
            }
        }

        retained
    }

    /// 执行 compaction (逻辑层)
    ///
    /// 输入: 所有记录
    /// 输出: compacted 后的记录 + 统计结果
    pub fn compact_records(
        &self,
        records: &[KeyValueRecord],
        now_ms: i64,
    ) -> Result<(Vec<KeyValueRecord>, CompactionResult)> {
        let start = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let total_bytes: u64 = records.iter().map(|r| r.raw_bytes.len() as u64).sum();

        // 1. 构建 key 索引
        let key_index = self.build_key_index(records);

        // 2. 确定保留的记录
        let retained_indices = self.determine_retained_records(records, &key_index, now_ms);

        // 3. 收集保留的记录
        let mut compacted: Vec<KeyValueRecord> = retained_indices
            .iter()
            .map(|&idx| records[idx].clone())
            .collect();

        // 4. 重新编号 offset (保持连续)
        let base_offset = compacted.first().map(|r| r.offset.0).unwrap_or(0);
        for (new_idx, record) in compacted.iter_mut().enumerate() {
            record.offset = Offset(base_offset + new_idx as i64);
        }

        let compacted_bytes: u64 = compacted.iter().map(|r| r.raw_bytes.len() as u64).sum();
        let records_removed = records.len() as u64 - compacted.len() as u64;
        let tombstones_removed = records.iter()
            .filter(|r| r.is_tombstone())
            .count() as u64
            - compacted.iter()
                .filter(|r| r.is_tombstone())
                .count() as u64;

        let end = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let ratio = if total_bytes > 0 {
            compacted_bytes as f64 / total_bytes as f64
        } else {
            1.0
        };

        let result = CompactionResult {
            segments_scanned: 0,
            total_bytes_scanned: total_bytes,
            unique_keys: key_index.unique_key_count(),
            records_removed,
            bytes_freed: total_bytes - compacted_bytes,
            compacted_bytes,
            tombstones_removed,
            compaction_ratio: ratio,
            duration_ms: end - start,
        };

        info!(%result, "Log compaction completed");

        Ok((compacted, result))
    }

    /// 检查是否需要执行 compaction
    pub fn should_compact(&self, total_bytes: u64, dirty_bytes: u64) -> bool {
        if !self.config.cleanup_policy.needs_compaction() {
            return false;
        }

        if dirty_bytes < self.config.min_dirty_bytes {
            return false;
        }

        if total_bytes > 0 {
            let dirty_ratio = dirty_bytes as f64 / total_bytes as f64;
            dirty_ratio > self.config.max_dirty_ratio
        } else {
            false
        }
    }
}

// ─── Tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn record(key: Option<&str>, value: Option<&str>, offset: i64, ts: i64) -> KeyValueRecord {
        KeyValueRecord {
            key: key.map(|k| k.as_bytes().to_vec()),
            value: value.map(|v| v.as_bytes().to_vec()),
            offset: Offset(offset),
            timestamp: ts,
            raw_bytes: vec![0u8; 100], // 模拟 100 字节
        }
    }

    fn default_config() -> CompactionConfig {
        CompactionConfig {
            cleanup_policy: CleanupPolicy::Compact,
            delete_retention_ms: 86_400_000,
            min_compaction_ratio: 0.5,
            min_dirty_bytes: 0,
            max_dirty_ratio: 0.5,
        }
    }

    // ─── CleanupPolicy ──────────────────────────────────────────

    #[test]
    fn test_cleanup_policy_from_str() {
        assert_eq!(CleanupPolicy::from_str("delete"), CleanupPolicy::Delete);
        assert_eq!(CleanupPolicy::from_str("compact"), CleanupPolicy::Compact);
        assert_eq!(
            CleanupPolicy::from_str("compact,delete"),
            CleanupPolicy::CompactDelete
        );
        assert_eq!(
            CleanupPolicy::from_str("delete,compact"),
            CleanupPolicy::CompactDelete
        );
        assert_eq!(CleanupPolicy::from_str("unknown"), CleanupPolicy::Delete);
    }

    #[test]
    fn test_cleanup_policy_flags() {
        assert!(!CleanupPolicy::Delete.needs_compaction());
        assert!(CleanupPolicy::Delete.needs_deletion());

        assert!(CleanupPolicy::Compact.needs_compaction());
        assert!(!CleanupPolicy::Compact.needs_deletion());

        assert!(CleanupPolicy::CompactDelete.needs_compaction());
        assert!(CleanupPolicy::CompactDelete.needs_deletion());
    }

    // ─── KeyValueRecord ─────────────────────────────────────────

    #[test]
    fn test_tombstone_detection() {
        let r1 = record(Some("key1"), None, 0, 1000);
        assert!(r1.is_tombstone());
        assert!(r1.has_key());

        let r2 = record(Some("key1"), Some("value"), 0, 1000);
        assert!(!r2.is_tombstone());
        assert!(r2.has_key());

        let r3 = record(None, Some("value"), 0, 1000);
        assert!(!r3.is_tombstone());
        assert!(!r3.has_key());
    }

    // ─── KeyIndex ───────────────────────────────────────────────

    #[test]
    fn test_key_index_basic() {
        let mut index = KeyIndex::new();
        assert_eq!(index.unique_key_count(), 0);

        index.upsert(
            b"key1".to_vec(),
            KeyPosition {
                offset: Offset(0),
                timestamp: 1000,
                segment_idx: 0,
                byte_offset: 0,
                batch_size: 100,
            },
        );
        assert_eq!(index.unique_key_count(), 1);
        assert_eq!(index.total_keys_seen(), 1);

        // 覆盖 key1
        index.upsert(
            b"key1".to_vec(),
            KeyPosition {
                offset: Offset(5),
                timestamp: 2000,
                segment_idx: 0,
                byte_offset: 500,
                batch_size: 100,
            },
        );
        assert_eq!(index.unique_key_count(), 1); // 唯一数不变
        assert_eq!(index.total_keys_seen(), 2); // 总计数增加
        assert_eq!(index.get(b"key1").unwrap().offset, Offset(5)); // 最新版本
    }

    #[test]
    fn test_key_index_retained_positions_sorted() {
        let mut index = KeyIndex::new();
        index.upsert(
            b"b".to_vec(),
            KeyPosition { offset: Offset(10), timestamp: 0, segment_idx: 0, byte_offset: 0, batch_size: 0 },
        );
        index.upsert(
            b"a".to_vec(),
            KeyPosition { offset: Offset(5), timestamp: 0, segment_idx: 0, byte_offset: 0, batch_size: 0 },
        );

        let positions = index.retained_positions();
        assert_eq!(positions.len(), 2);
        assert_eq!(positions[0].offset, Offset(5));
        assert_eq!(positions[1].offset, Offset(10));
    }

    // ─── LogCleaner ─────────────────────────────────────────────

    #[test]
    fn test_build_key_index() {
        let cleaner = LogCleaner::new(default_config());
        let records = vec![
            record(Some("k1"), Some("v1"), 0, 1000),
            record(Some("k2"), Some("v2"), 1, 1001),
            record(Some("k1"), Some("v3"), 2, 1002), // k1 覆盖
        ];

        let index = cleaner.build_key_index(&records);
        assert_eq!(index.unique_key_count(), 2); // k1, k2
        assert_eq!(index.total_keys_seen(), 3); // k1, k2, k1
        assert_eq!(index.compactable_count(), 1); // 1 个可压缩

        // k1 最新版本是 offset=2
        assert_eq!(index.get(b"k1").unwrap().offset, Offset(2));
        // k2 最新版本是 offset=1
        assert_eq!(index.get(b"k2").unwrap().offset, Offset(1));
    }

    #[test]
    fn test_compact_basic() {
        let cleaner = LogCleaner::new(default_config());
        let records = vec![
            record(Some("k1"), Some("v1"), 0, 1000),
            record(Some("k2"), Some("v2"), 1, 1001),
            record(Some("k1"), Some("v3"), 2, 1002),
            record(Some("k2"), Some("v4"), 3, 1003),
        ];

        let now_ms = 2000;
        let (compacted, result) = cleaner.compact_records(&records, now_ms).unwrap();

        // 只保留 k1=v3, k2=v4
        assert_eq!(compacted.len(), 2);
        assert_eq!(result.records_removed, 2);
        assert_eq!(result.unique_keys, 2);
        assert!(result.compaction_ratio < 1.0);
    }

    #[test]
    fn test_compact_preserves_keyless_records() {
        let cleaner = LogCleaner::new(default_config());
        let records = vec![
            record(None, Some("no-key-1"), 0, 1000),
            record(Some("k1"), Some("v1"), 1, 1001),
            record(None, Some("no-key-2"), 2, 1002),
            record(Some("k1"), Some("v2"), 3, 1003),
        ];

        let (compacted, _) = cleaner.compact_records(&records, 2000).unwrap();

        // 无 key 记录 + k1 最新版本
        assert_eq!(compacted.len(), 3);
    }

    #[test]
    fn test_tombstone_retained_within_expiry() {
        let cleaner = LogCleaner::new(default_config());
        let records = vec![
            record(Some("k1"), Some("v1"), 0, 1000),
            record(Some("k1"), None, 1, 1500), // tombstone
        ];

        // now=2000, tombstone age=500ms < 86_400_000ms → 保留
        let (compacted, result) = cleaner.compact_records(&records, 2000).unwrap();
        assert_eq!(compacted.len(), 1);
        assert!(compacted[0].is_tombstone());
        assert_eq!(result.tombstones_removed, 0);
    }

    #[test]
    fn test_tombstone_removed_after_expiry() {
        let mut config = default_config();
        config.delete_retention_ms = 1000; // 1 秒过期

        let cleaner = LogCleaner::new(config);
        let records = vec![
            record(Some("k1"), Some("v1"), 0, 1000),
            record(Some("k1"), None, 1, 1500), // tombstone
        ];

        // now=5000, tombstone age=3500ms > 1000ms → 过期删除
        let (compacted, result) = cleaner.compact_records(&records, 5000).unwrap();
        assert_eq!(compacted.len(), 0);
        assert_eq!(result.tombstones_removed, 1);
    }

    #[test]
    fn test_offset_renumbering() {
        let cleaner = LogCleaner::new(default_config());
        let records = vec![
            record(Some("k1"), Some("v1"), 0, 1000),
            record(Some("k2"), Some("v2"), 1, 1001),
            record(Some("k1"), Some("v3"), 10, 1002), // offset 跳跃
        ];

        let (compacted, _) = cleaner.compact_records(&records, 2000).unwrap();
        assert_eq!(compacted.len(), 2);
        // 重新编号为连续 offset (基于保留记录的 base_offset=1)
        assert_eq!(compacted[0].offset, Offset(1));
        assert_eq!(compacted[1].offset, Offset(2));
    }

    #[test]
    fn test_should_compact() {
        let config = CompactionConfig {
            cleanup_policy: CleanupPolicy::Compact,
            min_dirty_bytes: 1024,
            max_dirty_ratio: 0.5,
            ..default_config()
        };
        let cleaner = LogCleaner::new(config);

        // dirty_bytes < min → false
        assert!(!cleaner.should_compact(10000, 500));

        // dirty_ratio > max → true
        assert!(cleaner.should_compact(10000, 6000));

        // Delete policy → false
        let delete_cleaner = LogCleaner::new(CompactionConfig {
            cleanup_policy: CleanupPolicy::Delete,
            ..default_config()
        });
        assert!(!delete_cleaner.should_compact(10000, 9000));
    }

    #[test]
    fn test_compaction_result_display() {
        let result = CompactionResult {
            segments_scanned: 3,
            total_bytes_scanned: 10000,
            unique_keys: 50,
            records_removed: 100,
            bytes_freed: 5000,
            compacted_bytes: 5000,
            tombstones_removed: 5,
            compaction_ratio: 0.5,
            duration_ms: 42,
        };
        let display = format!("{}", result);
        assert!(display.contains("keys=50"));
        assert!(display.contains("removed=100"));
        assert!(display.contains("ratio=0.50"));
    }

    #[test]
    fn test_empty_records() {
        let cleaner = LogCleaner::new(default_config());
        let (compacted, result) = cleaner.compact_records(&[], 2000).unwrap();
        assert!(compacted.is_empty());
        assert_eq!(result.records_removed, 0);
        assert_eq!(result.unique_keys, 0);
    }

    #[test]
    fn test_all_same_key() {
        let cleaner = LogCleaner::new(default_config());
        let records = vec![
            record(Some("k1"), Some("v1"), 0, 1000),
            record(Some("k1"), Some("v2"), 1, 1001),
            record(Some("k1"), Some("v3"), 2, 1002),
            record(Some("k1"), Some("v4"), 3, 1003),
        ];

        let (compacted, result) = cleaner.compact_records(&records, 2000).unwrap();
        assert_eq!(compacted.len(), 1);
        assert_eq!(result.records_removed, 3);
        assert_eq!(result.unique_keys, 1);
        // 保留最新的 v4
        assert_eq!(compacted[0].value, Some(b"v4".to_vec()));
    }
}
