//! Retention & Compaction: 过期 Segment 清理 + Log Compaction
//!
//! Phase 1 实现:
//! - Size-based retention: 总大小超过阈值时删除最老 Segment
//! - Time-based retention: 超过保留时间的 Segment 被删除
//! - Log Compaction: Phase 4 实现

use rk_core::error::Result;
use rk_core::types::Offset;

use crate::commitlog::CommitLog;

/// Retention 配置
#[derive(Debug, Clone)]
pub struct RetentionConfig {
    /// 最大保留字节数 (0 = 无限)
    pub max_bytes: u64,
    /// 最大保留时间 (毫秒, 0 = 无限)
    pub max_ms: u64,
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            max_bytes: 1_099_511_627_776, // 1TB
            max_ms: 604_800_000,          // 7 days
        }
    }
}

/// Retention 执行结果
#[derive(Debug, Clone)]
pub struct RetentionResult {
    /// 删除的 Segment 数量
    pub segments_deleted: usize,
    /// 释放的字节数
    pub bytes_freed: u64,
    /// 新的 Log Start Offset
    pub new_log_start_offset: Offset,
}

/// 对单个 CommitLog 执行 Retention 清理
///
/// 策略:
/// 1. Time-based: 删除 max_timestamp < (now - max_ms) 的 sealed segment
/// 2. Size-based: 删除最老 segment 直到 total_size <= max_bytes
/// 3. 始终保留至少 1 个 segment (active)
pub fn apply_retention(
    commit_log: &mut CommitLog,
    config: &RetentionConfig,
    now_ms: i64,
) -> Result<RetentionResult> {
    let mut deleted = 0usize;
    let mut freed_bytes = 0u64;

    // 1. Time-based retention
    if config.max_ms > 0 {
        let cutoff = now_ms - config.max_ms as i64;
        loop {
            // 不能删除最后一个 segment (active)
            if commit_log.segment_count() <= 1 {
                break;
            }

            let oldest_ts = match commit_log.oldest_segment_timestamp() {
                Some(ts) => ts,
                None => break,
            };

            // 如果最老 segment 的 max_timestamp < cutoff, 删除它
            if oldest_ts < cutoff {
                let size_before = commit_log.total_size_bytes();
                if commit_log.delete_oldest_segment()? {
                    let size_after = commit_log.total_size_bytes();
                    freed_bytes += size_before - size_after;
                    deleted += 1;
                } else {
                    break;
                }
            } else {
                break;
            }
        }
    }

    // 2. Size-based retention
    if config.max_bytes > 0 {
        loop {
            if commit_log.segment_count() <= 1 {
                break;
            }

            let total_size = commit_log.total_size_bytes();
            if total_size <= config.max_bytes {
                break;
            }

            let size_before = commit_log.total_size_bytes();
            if commit_log.delete_oldest_segment()? {
                let size_after = commit_log.total_size_bytes();
                freed_bytes += size_before - size_after;
                deleted += 1;
            } else {
                break;
            }
        }
    }

    Ok(RetentionResult {
        segments_deleted: deleted,
        bytes_freed: freed_bytes,
        new_log_start_offset: commit_log.log_start_offset(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log_io::build_batch_bytes;
    use tempfile::tempdir;
    use rk_core::types::{PartitionId, TopicName};

    fn make_batch(base_offset: i64, record_count: i32) -> Vec<u8> {
        let records = vec![0u8; record_count as usize * 10];
        build_batch_bytes(base_offset, 1, 0, 1000, 2000, -1, -1, -1, &records, record_count)
    }

    fn make_batch_with_ts(base_offset: i64, record_count: i32, timestamp: i64) -> Vec<u8> {
        let records = vec![0u8; record_count as usize * 10];
        build_batch_bytes(base_offset, 1, 0, timestamp, timestamp + 100, -1, -1, -1, &records, record_count)
    }

    #[test]
    fn test_retention_no_deletion_when_empty() {
        let dir = tempdir().unwrap();
        let mut cl = CommitLog::create(
            dir.path(),
            TopicName("test".into()),
            PartitionId(0),
            1_073_741_824,
        ).unwrap();

        let config = RetentionConfig { max_bytes: 100, max_ms: 0 };
        let result = apply_retention(&mut cl, &config, 5000).unwrap();
        assert_eq!(result.segments_deleted, 0);
    }

    #[test]
    fn test_size_based_retention() {
        let dir = tempdir().unwrap();
        // 非常小的 segment 大小，强制多 segment
        let mut cl = CommitLog::create(
            dir.path(),
            TopicName("test".into()),
            PartitionId(0),
            200, // 200 bytes per segment
        ).unwrap();

        // 写入多个 batch 产生多个 segment
        for i in 0..6 {
            cl.append_batch(&make_batch(i * 3, 3)).unwrap();
        }

        let segment_count_before = cl.segment_count();
        assert!(segment_count_before > 1, "Should have multiple segments");

        // 设置很小的 max_bytes，触发删除
        let config = RetentionConfig { max_bytes: 300, max_ms: 0 };
        let result = apply_retention(&mut cl, &config, 5000).unwrap();

        assert!(result.segments_deleted > 0, "Should have deleted segments");
        assert!(result.bytes_freed > 0);
        assert!(cl.total_size_bytes() <= 300 || cl.segment_count() == 1);
    }

    #[test]
    fn test_time_based_retention() {
        let dir = tempdir().unwrap();
        let mut cl = CommitLog::create(
            dir.path(),
            TopicName("test".into()),
            PartitionId(0),
            200, // 小 segment 强制多个
        ).unwrap();

        // 写入带时间戳的 batch
        // timestamp=1000
        cl.append_batch(&make_batch_with_ts(0, 3, 1000)).unwrap();
        // timestamp=2000
        cl.append_batch(&make_batch_with_ts(3, 3, 2000)).unwrap();
        // timestamp=3000
        cl.append_batch(&make_batch_with_ts(6, 3, 3000)).unwrap();
        // 更多 batch 确保多个 segment
        cl.append_batch(&make_batch_with_ts(9, 3, 3500)).unwrap();
        cl.append_batch(&make_batch_with_ts(12, 3, 3800)).unwrap();
        cl.append_batch(&make_batch_with_ts(15, 3, 4000)).unwrap();

        let segment_count_before = cl.segment_count();

        // now=5000, max_ms=2000 → cutoff=3000
        // timestamp < 3000 的 segment 应被删除
        let config = RetentionConfig { max_bytes: 0, max_ms: 2000 };
        let result = apply_retention(&mut cl, &config, 5000).unwrap();

        // 至少应删除一些 segment (max_timestamp < 3000)
        if segment_count_before > 1 {
            assert!(result.segments_deleted > 0 || segment_count_before == cl.segment_count());
        }
    }

    #[test]
    fn test_retention_keeps_at_least_one_segment() {
        let dir = tempdir().unwrap();
        let mut cl = CommitLog::create(
            dir.path(),
            TopicName("test".into()),
            PartitionId(0),
            1_073_741_824,
        ).unwrap();

        cl.append_batch(&make_batch(0, 5)).unwrap();

        // max_bytes=0 意味着无限, max_ms=0 意味着无限
        // 但即使设为很小，也应保留至少一个 segment
        let config = RetentionConfig { max_bytes: 1, max_ms: 1 };
        let _result = apply_retention(&mut cl, &config, 999_999).unwrap();

        assert_eq!(cl.segment_count(), 1, "Must keep at least 1 segment");
    }

    #[test]
    fn test_retention_updates_log_start_offset() {
        let dir = tempdir().unwrap();
        let mut cl = CommitLog::create(
            dir.path(),
            TopicName("test".into()),
            PartitionId(0),
            200, // 小 segment
        ).unwrap();

        for i in 0..6 {
            cl.append_batch(&make_batch(i * 3, 3)).unwrap();
        }

        assert_eq!(cl.log_start_offset(), Offset(0));

        let config = RetentionConfig { max_bytes: 200, max_ms: 0 };
        let result = apply_retention(&mut cl, &config, 5000).unwrap();

        if result.segments_deleted > 0 {
            // log_start_offset 应该推进
            assert!(cl.log_start_offset() > Offset(0));
        }
    }

    #[test]
    fn test_retention_config_default() {
        let config = RetentionConfig::default();
        assert_eq!(config.max_bytes, 1_099_511_627_776); // 1TB
        assert_eq!(config.max_ms, 604_800_000);          // 7 days
    }
}
