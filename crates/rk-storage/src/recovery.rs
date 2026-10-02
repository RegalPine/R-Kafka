//! Crash Recovery: 从尾部向前扫描 CRC32C，截断损坏数据
//!
//! 恢复流程:
//! 1. 加载所有 Segment 元数据 (base_offset, file_size)
//! 2. 定位 Active Segment (最后一个未 Sealed 的)
//! 3. 从 Segment 尾部向前扫描，逐条验证 CRC32C
//! 4. CRC 正常 → 更新 log_end_offset
//! 5. CRC 异常 → 截断 (truncate) 到最后一个完整 Record
//! 6. 重建 Offset Index 和 Time Index
//! 7. 恢复 High Watermark (从 Replica 状态，不在本模块)

use std::fs::{self, File, OpenOptions};
use std::path::Path;
use rk_core::error::Result;
use rk_core::types::{Offset, PartitionId, TopicName};

use crate::commitlog::CommitLog;
use crate::log_io;

/// 恢复结果
#[derive(Debug)]
pub struct RecoveryResult {
    /// 恢复后的 Log End Offset
    pub log_end_offset: Offset,
    /// 扫描的 batch 总数
    pub batches_scanned: u64,
    /// 截断的字节数 (损坏数据)
    pub bytes_truncated: u64,
    /// 恢复的 Segment 数量
    pub segments_recovered: usize,
}

/// 恢复单个 Partition 的 CommitLog
///
/// 如果数据目录不存在，创建新的 CommitLog。
/// 如果数据目录存在，扫描并恢复已有数据。
pub fn recover_partition(
    data_dir: &Path,
    topic: TopicName,
    partition: PartitionId,
    segment_max_size: u64,
) -> Result<(CommitLog, RecoveryResult)> {
    let dir = data_dir.join(format!("{}-{}", topic.0, partition.0));

    if !dir.exists() {
        // 全新 Partition
        let cl = CommitLog::create(data_dir, topic, partition, segment_max_size)?;
        let leo = cl.log_end_offset();
        return Ok((cl, RecoveryResult {
            log_end_offset: leo,
            batches_scanned: 0,
            bytes_truncated: 0,
            segments_recovered: 1,
        }));
    }

    // 扫描 .log 文件
    let mut base_offsets: Vec<u64> = Vec::new();
    for entry in fs::read_dir(&dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if name_str.ends_with(".log") {
            if let Ok(offset) = name_str.trim_end_matches(".log").parse::<u64>() {
                base_offsets.push(offset);
            }
        }
    }
    base_offsets.sort();

    if base_offsets.is_empty() {
        let cl = CommitLog::create(data_dir, topic, partition, segment_max_size)?;
        let leo = cl.log_end_offset();
        return Ok((cl, RecoveryResult {
            log_end_offset: leo,
            batches_scanned: 0,
            bytes_truncated: 0,
            segments_recovered: 1,
        }));
    }

    // 恢复每个 Segment
    let mut total_batches = 0u64;
    let mut total_truncated = 0u64;

    for (i, &base_offset) in base_offsets.iter().enumerate() {
        let _is_last = i == base_offsets.len() - 1;
        let base_name = log_io::format_segment_base_name(base_offset);
        let log_path = dir.join(format!("{}.log", base_name));

        let file_size = fs::metadata(&log_path)?.len();

        // 扫描有效 batch
        let mut file = File::open(&log_path)?;
        let valid_batches = log_io::scan_batches_from_file(&mut file, 0, file_size);
        let valid_end = valid_batches.last().map(|(pos, bytes)| {
            pos + bytes.len() as u64
        }).unwrap_or(0);

        total_batches += valid_batches.len() as u64;

        // 如果有损坏数据，截断
        if valid_end < file_size {
            let truncated = file_size - valid_end;
            total_truncated += truncated;

            // 截断 .log 文件
            let log_file = OpenOptions::new().write(true).open(&log_path)?;
            log_file.set_len(valid_end)?;
        }
    }

    // 使用 CommitLog::recover 进行完整恢复 (含索引重建)
    let cl = CommitLog::recover(data_dir, topic, partition, segment_max_size)?;
    let leo = cl.log_end_offset();

    Ok((cl, RecoveryResult {
        log_end_offset: leo,
        batches_scanned: total_batches,
        bytes_truncated: total_truncated,
        segments_recovered: base_offsets.len(),
    }))
}

/// 验证单个 Segment 文件的数据完整性
///
/// 返回 (有效 batch 数, 有效字节数, 损坏字节数)
pub fn verify_segment_integrity(
    log_path: &Path,
) -> Result<(u64, u64, u64)> {
    let file_size = fs::metadata(log_path)?.len();
    let mut file = File::open(log_path)?;
    let valid_batches = log_io::scan_batches_from_file(&mut file, 0, file_size);

    let valid_bytes = valid_batches
        .last()
        .map(|(pos, bytes)| pos + bytes.len() as u64)
        .unwrap_or(0);

    let corrupted_bytes = file_size.saturating_sub(valid_bytes);

    Ok((valid_batches.len() as u64, valid_bytes, corrupted_bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log_io::build_batch_bytes;
    use std::io::Write;
    use tempfile::tempdir;

    fn make_batch(base_offset: i64, record_count: i32) -> Vec<u8> {
        let records = vec![0u8; record_count as usize * 10];
        build_batch_bytes(base_offset, 1, 0, 1000, 2000, -1, -1, -1, &records, record_count)
    }

    #[test]
    fn test_recover_new_partition() {
        let dir = tempdir().unwrap();
        let (cl, result) = recover_partition(
            dir.path(),
            TopicName("test".into()),
            PartitionId(0),
            1_073_741_824,
        ).unwrap();

        assert_eq!(result.batches_scanned, 0);
        assert_eq!(result.bytes_truncated, 0);
        assert_eq!(result.segments_recovered, 1);
        assert_eq!(cl.log_end_offset(), Offset(0));
    }

    #[test]
    fn test_recover_existing_data() {
        let dir = tempdir().unwrap();

        // 先创建并写入
        {
            let mut cl = CommitLog::create(
                dir.path(),
                TopicName("test".into()),
                PartitionId(0),
                1_073_741_824,
            ).unwrap();
            cl.append_batch(&make_batch(0, 5)).unwrap();
            cl.append_batch(&make_batch(5, 3)).unwrap();
            cl.flush_all().unwrap();
        }

        // 恢复
        let (cl, result) = recover_partition(
            dir.path(),
            TopicName("test".into()),
            PartitionId(0),
            1_073_741_824,
        ).unwrap();

        assert_eq!(result.batches_scanned, 2);
        assert_eq!(result.bytes_truncated, 0);
        assert_eq!(cl.log_end_offset(), Offset(8));
    }

    #[test]
    fn test_recover_with_corrupted_tail() {
        let dir = tempdir().unwrap();

        // 创建并写入
        {
            let mut cl = CommitLog::create(
                dir.path(),
                TopicName("test".into()),
                PartitionId(0),
                1_073_741_824,
            ).unwrap();
            cl.append_batch(&make_batch(0, 5)).unwrap();
            cl.flush_all().unwrap();
        }

        // 在 .log 文件尾部追加损坏数据
        let log_path = dir.path().join("test-0").join("00000000000000000000.log");
        {
            let mut file = OpenOptions::new().append(true).open(&log_path).unwrap();
            file.write_all(&[0xFFu8; 200]).unwrap();
        }

        // 恢复
        let (cl, result) = recover_partition(
            dir.path(),
            TopicName("test".into()),
            PartitionId(0),
            1_073_741_824,
        ).unwrap();

        assert_eq!(result.batches_scanned, 1);
        assert!(result.bytes_truncated > 0);
        assert_eq!(cl.log_end_offset(), Offset(5));

        // 验证文件已被截断
        let _file_size = fs::metadata(&log_path).unwrap().len();
        let (valid, _valid_bytes, corrupted) = verify_segment_integrity(&log_path).unwrap();
        assert_eq!(valid, 1);
        assert_eq!(corrupted, 0); // 截断后无损坏
    }

    #[test]
    fn test_verify_segment_integrity() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("test.log");

        // 写入有效数据
        let batch = make_batch(0, 5);
        {
            let mut file = File::create(&log_path).unwrap();
            file.write_all(&batch).unwrap();
        }

        let (count, valid_bytes, corrupted) = verify_segment_integrity(&log_path).unwrap();
        assert_eq!(count, 1);
        assert_eq!(valid_bytes, batch.len() as u64);
        assert_eq!(corrupted, 0);
    }
}
