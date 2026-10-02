//! CommitLog: 每个 Partition 的 append-only 日志
//!
//! 管理多个 LogSegment，提供:
//! - 追加写入 (自动滚动 Segment)
//! - 按 Offset 读取
//! - 按 Timestamp 查找
//! - High Watermark / Log End Offset 管理
//!
//! Thread-per-Core: 每个 Partition 的 CommitLog 由一个 Partition Actor 独占持有，
//! 无需 Arc<Mutex<>>。并发通过消息传递 (channel) 路由到对应 Actor。

use std::fs;
use std::path::{Path, PathBuf};

use rk_core::error::{RkError, Result};
use rk_core::types::{Offset, PartitionId, TopicName};
use rk_protocol::record::{HEADER_SIZE, decode_batch_header};
use rk_protocol::types::KafkaReader;

use crate::index::DEFAULT_MAX_INDEX_SIZE;
use crate::segment::LogSegment;

/// 默认 Segment 最大大小 (1GB)
#[allow(dead_code)]
const DEFAULT_SEGMENT_MAX_SIZE: u64 = 1_073_741_824;

/// 每个 Partition 独占一个 CommitLog，绑定到单个 Partition Actor (Thread-per-Core)
pub struct CommitLog {
    /// Topic 名称
    topic: TopicName,
    /// Partition ID
    partition: PartitionId,
    /// 数据目录 (topic-partition 目录)
    dir: PathBuf,
    /// 所有 Segment (按 base_offset 排序)
    segments: Vec<LogSegment>,
    /// 当前 Active Segment 的索引 (始终指向最后一个)
    active_segment_idx: usize,
    /// Log Start Offset (第一条可用消息的 offset, retention 会推进)
    log_start_offset: Offset,
    /// Log End Offset (下一条消息的 offset)
    log_end_offset: Offset,
    /// High Watermark (消费者可见的最大 offset)
    high_watermark: Offset,
    /// Segment 最大大小
    segment_max_size: u64,
    /// 索引文件最大大小
    max_index_size: usize,
}

impl CommitLog {
    /// 创建新的 CommitLog (新 Partition)
    pub fn create(
        data_dir: &Path,
        topic: TopicName,
        partition: PartitionId,
        segment_max_size: u64,
    ) -> Result<Self> {
        let dir = data_dir.join(format!("{}-{}", topic.0, partition.0));
        fs::create_dir_all(&dir)?;

        // 创建第一个 Segment (base_offset = 0)
        let segment = LogSegment::create(&dir, 0, DEFAULT_MAX_INDEX_SIZE)?;
        let segments = vec![segment];

        Ok(Self {
            topic,
            partition,
            dir,
            segments,
            active_segment_idx: 0,
            log_start_offset: Offset(0),
            log_end_offset: Offset(0),
            high_watermark: Offset(0),
            segment_max_size,
            max_index_size: DEFAULT_MAX_INDEX_SIZE,
        })
    }

    /// 从已有数据恢复 CommitLog
    pub fn recover(
        data_dir: &Path,
        topic: TopicName,
        partition: PartitionId,
        segment_max_size: u64,
    ) -> Result<Self> {
        let dir = data_dir.join(format!("{}-{}", topic.0, partition.0));

        if !dir.exists() {
            return Self::create(data_dir, topic, partition, segment_max_size);
        }

        // 扫描目录，找到所有 .log 文件
        let mut base_offsets: Vec<u64> = Vec::new();
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.ends_with(".log") {
                if let Ok(offset) = name.trim_end_matches(".log").parse::<u64>() {
                    base_offsets.push(offset);
                }
            }
        }
        base_offsets.sort();

        if base_offsets.is_empty() {
            return Self::create(data_dir, topic, partition, segment_max_size);
        }

        // 打开所有 Segment
        let mut segments = Vec::new();
        for base_offset in &base_offsets {
            let mut seg = LogSegment::open(&dir, *base_offset, DEFAULT_MAX_INDEX_SIZE)?;
            // 重建索引
            seg.rebuild_indexes()?;
            segments.push(seg);
        }

        // 最后一个 Segment 设为 Active
        let active_idx = segments.len() - 1;
        segments[active_idx].set_active();

        // 计算 log_end_offset
        let last_seg = &segments[active_idx];
        let last_base = last_seg.base_offset();
        let last_rel = last_seg.last_relative_offset() as i64;
        let leo = if last_seg.batch_count() > 0 {
            Offset(last_base as i64 + last_rel + 1)
        } else {
            Offset(last_base as i64)
        };

        // log_start_offset = 第一个 segment 的 base_offset
        let lso = segments[0].base_offset() as i64;

        Ok(Self {
            topic,
            partition,
            dir,
            segments,
            active_segment_idx: active_idx,
            log_start_offset: Offset(lso),
            log_end_offset: leo,
            high_watermark: Offset(0), // 恢复后从副本状态重建
            segment_max_size,
            max_index_size: DEFAULT_MAX_INDEX_SIZE,
        })
    }

    /// 追加写入一个 RecordBatch
    ///
    /// 返回本 batch 第一条消息的绝对 offset。
    /// 如果 Active Segment 已满，自动滚动到新 Segment。
    pub fn append_batch(&mut self, batch_bytes: &[u8]) -> Result<Offset> {
        // 解析 batch header 获取信息
        if batch_bytes.len() < HEADER_SIZE {
            return Err(RkError::Protocol("Batch too small for append".to_string()));
        }

        let mut reader = KafkaReader::new(batch_bytes);
        let hdr = decode_batch_header(&mut reader)?;

        // 检查是否需要滚动 Segment
        let active = &self.segments[self.active_segment_idx];
        if active.write_position() >= self.segment_max_size {
            self.roll_segment()?;
        }

        // 写入 Active Segment
        let active = &mut self.segments[self.active_segment_idx];
        let base_offset = active.append(batch_bytes)?;

        // 更新 LEO
        let last_offset = hdr.base_offset + hdr.last_offset_delta as i64;
        self.log_end_offset = Offset(last_offset + 1);

        Ok(Offset(base_offset as i64))
    }

    /// 滚动到新 Segment
    fn roll_segment(&mut self) -> Result<()> {
        // Seal 当前 Active Segment
        self.segments[self.active_segment_idx].seal()?;

        // 新 Segment 的 base_offset = 当前 LEO
        let new_base_offset = self.log_end_offset.0 as u64;
        let new_segment = LogSegment::create(&self.dir, new_base_offset, self.max_index_size)?;

        self.segments.push(new_segment);
        self.active_segment_idx = self.segments.len() - 1;

        Ok(())
    }

    /// 按 Offset 查找: 定位到 ≥ target_offset 的第一条消息
    ///
    /// 返回 (batch_bytes, batch_file_position)
    pub fn read_at_offset(&mut self, target_offset: Offset) -> Result<Option<(Vec<u8>, u64)>> {
        if target_offset.0 >= self.log_end_offset.0 {
            return Ok(None);
        }

        // 定位到对应 Segment
        let seg_idx = self.find_segment_for_offset(target_offset.0 as u64);
        let segment = &mut self.segments[seg_idx];

        let base_offset = segment.base_offset();
        let relative_offset = (target_offset.0 as u64).wrapping_sub(base_offset) as u32;

        // 在索引中查找
        if let Some(entry) = segment.lookup_offset(relative_offset) {
            // 从索引位置顺序扫描，找到精确的 batch
            let mut pos = entry.physical_position as u64;
            let file_size = segment.write_position();

            while pos < file_size {
                let (batch_bytes, batch_size) = segment.read_batch(pos)?;

                // 解析 header 检查这个 batch 是否包含目标 offset
                let mut reader = KafkaReader::new(&batch_bytes);
                let hdr = decode_batch_header(&mut reader)?;

                let batch_start = hdr.base_offset as u64;
                let batch_end = batch_start + hdr.last_offset_delta as u64;

                if target_offset.0 as u64 >= batch_start && target_offset.0 as u64 <= batch_end {
                    return Ok(Some((batch_bytes, pos)));
                }

                pos += batch_size;
            }
        }

        Ok(None)
    }

    /// 按 Timestamp 查找: 定位到 ≥ target_timestamp 的第一条消息
    pub fn read_at_timestamp(&mut self, target_timestamp: i64) -> Result<Option<(Vec<u8>, u64)>> {
        // 从最后一个 Segment 向前搜索
        for seg_idx in (0..self.segments.len()).rev() {
            let segment = &self.segments[seg_idx];
            if segment.max_timestamp() < target_timestamp {
                continue; // 本 segment 所有消息都早于目标时间
            }

            // 在 time index 中查找
            if let Some(time_entry) = segment.lookup_time(target_timestamp) {
                // 找到最近的 timestamp，转为 offset 查找
                let abs_offset = segment.base_offset() as i64 + time_entry.relative_offset as i64;
                return self.read_at_offset(Offset(abs_offset));
            }
        }

        // 如果所有 segment 都没找到，尝试从第一个 segment 开始
        if !self.segments.is_empty() {
            let first_seg = &self.segments[0];
            if let Some(time_entry) = first_seg.lookup_time(target_timestamp) {
                let abs_offset = first_seg.base_offset() as i64 + time_entry.relative_offset as i64;
                return self.read_at_offset(Offset(abs_offset));
            }
        }

        Ok(None)
    }

    /// 查找包含指定 offset 的 Segment 索引
    fn find_segment_for_offset(&self, target_offset: u64) -> usize {
        // 二分查找: 找到 base_offset ≤ target 的最大 segment
        let idx = self.segments.partition_point(|s| s.base_offset() <= target_offset);
        if idx == 0 { 0 } else { idx - 1 }
    }

    /// 获取 Log End Offset
    pub fn log_end_offset(&self) -> Offset {
        self.log_end_offset
    }

    /// 获取 High Watermark
    pub fn high_watermark(&self) -> Offset {
        self.high_watermark
    }

    /// 更新 High Watermark
    pub fn set_high_watermark(&mut self, hw: Offset) {
        if hw.0 > self.high_watermark.0 {
            self.high_watermark = hw;
        }
    }

    /// 获取 Topic 名称
    pub fn topic(&self) -> &TopicName {
        &self.topic
    }

    /// 获取 Partition ID
    pub fn partition(&self) -> PartitionId {
        self.partition
    }

    /// 获取数据目录
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// 获取所有 Segment 的 base_offset 列表
    pub fn segment_base_offsets(&self) -> Vec<u64> {
        self.segments.iter().map(|s| s.base_offset()).collect()
    }

    /// 刷盘所有 Segment
    pub fn flush_all(&self) -> Result<()> {
        for seg in &self.segments {
            seg.flush()?;
        }
        Ok(())
    }

    /// 获取指定范围的 batch (用于 Fetch 响应)
    ///
    /// 从 start_offset 开始，最多读取 max_bytes 字节。
    /// 返回 Vec<(batch_bytes, position)>
    pub fn read_range(
        &mut self,
        start_offset: Offset,
        max_bytes: usize,
    ) -> Result<Vec<(Vec<u8>, u64)>> {
        let mut results = Vec::new();
        let mut total_bytes = 0usize;

        if start_offset.0 >= self.log_end_offset.0 {
            return Ok(results);
        }

        let seg_idx = self.find_segment_for_offset(start_offset.0 as u64);

        for seg_idx in seg_idx..self.segments.len() {
            if total_bytes >= max_bytes {
                break;
            }

            let segment = &mut self.segments[seg_idx];
            let base_offset = segment.base_offset();
            let relative_offset = (start_offset.0 as u64).wrapping_sub(base_offset) as u32;

            // 找到起始位置
            let start_pos = if let Some(entry) = segment.lookup_offset(relative_offset) {
                entry.physical_position as u64
            } else {
                continue;
            };

            // 顺序扫描
            let mut pos = start_pos;
            let file_size = segment.write_position();

            while pos < file_size && total_bytes < max_bytes {
                let (batch_bytes, batch_size) = segment.read_batch(pos)?;

                // 检查 batch 是否包含 ≥ start_offset 的消息
                let mut reader = KafkaReader::new(&batch_bytes);
                let hdr = decode_batch_header(&mut reader)?;
                let batch_end = hdr.base_offset + hdr.last_offset_delta as i64;

                if batch_end >= start_offset.0 {
                    total_bytes += batch_bytes.len();
                    results.push((batch_bytes, pos));
                }

                pos += batch_size;
            }
        }

        Ok(results)
    }

    /// 删除最老的 Segment (用于 retention)
    ///
    /// 返回 true 如果成功删除。至少保留一个 segment (active)。
    pub fn delete_oldest_segment(&mut self) -> Result<bool> {
        if self.segments.len() <= 1 {
            return Ok(false); // 至少保留一个 segment
        }

        let oldest = self.segments.remove(0);
        // 更新 log_start_offset 为下一个 segment 的 base_offset
        self.log_start_offset = Offset(self.segments[0].base_offset() as i64);
        self.active_segment_idx = self.segments.len() - 1;

        oldest.delete()?;
        Ok(true)
    }

    /// 获取 Log Start Offset
    pub fn log_start_offset(&self) -> Offset {
        self.log_start_offset
    }

    /// 将 log_start_offset 前移到指定偏移量 (DeleteRecords 用)
    ///
    /// 删除所有 base_offset < target_offset 的 segment (至少保留 active segment)。
    /// 返回实际的新 log_start_offset。
    pub fn advance_log_start_offset(&mut self, target_offset: i64) -> Result<Offset> {
        // 删除 base_offset < target_offset 的旧 segment (至少保留 1 个)
        while self.segments.len() > 1 && (self.segments[1].base_offset() as i64) <= target_offset {
            let oldest = self.segments.remove(0);
            self.active_segment_idx = self.segments.len() - 1;
            oldest.delete()?;
        }
        // 更新 log_start_offset
        if target_offset > self.log_start_offset.0 {
            self.log_start_offset = Offset(target_offset);
        }
        Ok(self.log_start_offset)
    }

    /// 获取所有 segment 的总字节数 (仅 .log 文件)
    pub fn total_size_bytes(&self) -> u64 {
        self.segments.iter().map(|s| s.write_position()).sum()
    }

    /// 获取最老 segment 的 max_timestamp
    pub fn oldest_segment_timestamp(&self) -> Option<i64> {
        self.segments.first().map(|s| s.max_timestamp())
    }

    /// 获取 Segment 数量
    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log_io::build_batch_bytes;
    use tempfile::tempdir;

    fn make_batch(base_offset: i64, record_count: i32) -> Vec<u8> {
        let records = vec![0u8; record_count as usize * 10];
        build_batch_bytes(base_offset, 1, 0, 1000, 2000, -1, -1, -1, &records, record_count)
    }

    #[test]
    fn test_commitlog_create() {
        let dir = tempdir().unwrap();
        let cl = CommitLog::create(
            dir.path(),
            TopicName("test".into()),
            PartitionId(0),
            DEFAULT_SEGMENT_MAX_SIZE,
        ).unwrap();

        assert_eq!(cl.log_end_offset(), Offset(0));
        assert_eq!(cl.segment_count(), 1);
    }

    #[test]
    fn test_commitlog_append() {
        let dir = tempdir().unwrap();
        let mut cl = CommitLog::create(
            dir.path(),
            TopicName("test".into()),
            PartitionId(0),
            DEFAULT_SEGMENT_MAX_SIZE,
        ).unwrap();

        let batch = make_batch(0, 5);
        let offset = cl.append_batch(&batch).unwrap();
        assert_eq!(offset, Offset(0));
        assert_eq!(cl.log_end_offset(), Offset(5)); // 0 + 4 + 1 = 5
    }

    #[test]
    fn test_commitlog_multiple_appends() {
        let dir = tempdir().unwrap();
        let mut cl = CommitLog::create(
            dir.path(),
            TopicName("test".into()),
            PartitionId(0),
            DEFAULT_SEGMENT_MAX_SIZE,
        ).unwrap();

        let batch1 = make_batch(0, 5);
        let batch2 = make_batch(5, 3);
        let batch3 = make_batch(8, 2);

        cl.append_batch(&batch1).unwrap();
        cl.append_batch(&batch2).unwrap();
        cl.append_batch(&batch3).unwrap();

        assert_eq!(cl.log_end_offset(), Offset(10)); // 8 + 1 + 1 = 10
    }

    #[test]
    fn test_commitlog_read_at_offset() {
        let dir = tempdir().unwrap();
        let mut cl = CommitLog::create(
            dir.path(),
            TopicName("test".into()),
            PartitionId(0),
            DEFAULT_SEGMENT_MAX_SIZE,
        ).unwrap();

        let batch1 = make_batch(0, 5);
        let batch2 = make_batch(5, 3);
        cl.append_batch(&batch1).unwrap();
        cl.append_batch(&batch2).unwrap();

        // 读取 offset 0 → 应返回 batch1
        let result = cl.read_at_offset(Offset(0)).unwrap();
        assert!(result.is_some());
        let (bytes, _) = result.unwrap();
        let mut reader = KafkaReader::new(&bytes);
        let hdr = decode_batch_header(&mut reader).unwrap();
        assert_eq!(hdr.base_offset, 0);

        // 读取 offset 5 → 应返回 batch2
        let result = cl.read_at_offset(Offset(5)).unwrap();
        assert!(result.is_some());
        let (bytes, _) = result.unwrap();
        let mut reader = KafkaReader::new(&bytes);
        let hdr = decode_batch_header(&mut reader).unwrap();
        assert_eq!(hdr.base_offset, 5);

        // 读取超出范围的 offset
        let result = cl.read_at_offset(Offset(100)).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_commitlog_read_range() {
        let dir = tempdir().unwrap();
        let mut cl = CommitLog::create(
            dir.path(),
            TopicName("test".into()),
            PartitionId(0),
            DEFAULT_SEGMENT_MAX_SIZE,
        ).unwrap();

        cl.append_batch(&make_batch(0, 5)).unwrap();
        cl.append_batch(&make_batch(5, 3)).unwrap();
        cl.append_batch(&make_batch(8, 2)).unwrap();

        // 读取从 offset 0 开始的所有数据
        let batches = cl.read_range(Offset(0), 1_000_000).unwrap();
        assert_eq!(batches.len(), 3);

        // 读取从 offset 5 开始
        let batches = cl.read_range(Offset(5), 1_000_000).unwrap();
        assert!(batches.len() >= 1);
    }

    #[test]
    fn test_commitlog_recover() {
        let dir = tempdir().unwrap();

        // 写入数据
        {
            let mut cl = CommitLog::create(
                dir.path(),
                TopicName("test".into()),
                PartitionId(0),
                DEFAULT_SEGMENT_MAX_SIZE,
            ).unwrap();

            cl.append_batch(&make_batch(0, 5)).unwrap();
            cl.append_batch(&make_batch(5, 3)).unwrap();
            cl.flush_all().unwrap();
        }

        // 恢复
        let cl = CommitLog::recover(
            dir.path(),
            TopicName("test".into()),
            PartitionId(0),
            DEFAULT_SEGMENT_MAX_SIZE,
        ).unwrap();

        assert_eq!(cl.log_end_offset(), Offset(8)); // 5 + 2 + 1 = 8
        assert!(cl.segment_count() >= 1);
    }

    #[test]
    fn test_commitlog_segment_rolling() {
        let dir = tempdir().unwrap();
        let mut cl = CommitLog::create(
            dir.path(),
            TopicName("test".into()),
            PartitionId(0),
            200, // 非常小的 segment 大小，强制滚动
        ).unwrap();

        // 写入多个 batch，每个约 200+ bytes
        for i in 0..5 {
            let batch = make_batch(i * 3, 3);
            cl.append_batch(&batch).unwrap();
        }

        // 应该已经滚动到多个 segment
        assert!(cl.segment_count() > 1, "Expected multiple segments, got {}", cl.segment_count());
    }

    #[test]
    fn test_commitlog_high_watermark() {
        let dir = tempdir().unwrap();
        let mut cl = CommitLog::create(
            dir.path(),
            TopicName("test".into()),
            PartitionId(0),
            DEFAULT_SEGMENT_MAX_SIZE,
        ).unwrap();

        assert_eq!(cl.high_watermark(), Offset(0));

        cl.set_high_watermark(Offset(5));
        assert_eq!(cl.high_watermark(), Offset(5));

        // HW 只能前进
        cl.set_high_watermark(Offset(3));
        assert_eq!(cl.high_watermark(), Offset(5));
    }
}
