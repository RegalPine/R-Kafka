//! LogSegment: 单个日志段文件
//!
//! 每个 Segment 对应三个文件:
//! - `{base_offset}.log`        — 消息日志 (append-only)
//! - `{base_offset}.index`      — Offset → 物理位置 稀疏索引
//! - `{base_offset}.timeindex`  — Timestamp → Offset 稀疏索引
//!
//! 生命周期: Created → Active (写入中) → Sealed (已满) → Deleted

use std::fs::{self, File, OpenOptions};
use std::io::{Write, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use rk_core::error::{RkError, Result};
use rk_protocol::record::{RecordBatchHeader, HEADER_SIZE, decode_batch_header};
use rk_protocol::types::KafkaReader;

use crate::index::{OffsetIndex, TimeIndex};
use crate::log_io;

/// Segment 状态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentState {
    /// 正在接收写入
    Active,
    /// 已满，只读
    Sealed,
}

/// 单个日志段
pub struct LogSegment {
    /// Segment 起始 offset (即文件名中的 base_offset)
    base_offset: u64,
    /// .log 文件句柄
    log_file: File,
    /// 当前写入位置 (在 .log 文件中的字节偏移)
    write_position: u64,
    /// 最大时间戳 (本 segment 中所有 batch 的最大 timestamp)
    max_timestamp: i64,
    /// Segment 状态
    state: SegmentState,
    /// Offset 稀疏索引
    offset_index: OffsetIndex,
    /// Time 稀疏索引
    time_index: TimeIndex,
    /// 文件路径前缀
    dir_path: PathBuf,
    /// 本 segment 中已写入的 batch 数量
    batch_count: u64,
    /// 本 segment 最后一条消息的 offset (相对于 base_offset)
    last_relative_offset: u32,
}

impl LogSegment {
    /// 创建新的 Active Segment
    pub fn create(dir: &Path, base_offset: u64, max_index_size: usize) -> Result<Self> {
        fs::create_dir_all(dir)?;

        let base_name = log_io::format_segment_base_name(base_offset);

        let log_path = dir.join(format!("{}.log", base_name));
        let index_path = dir.join(format!("{}.index", base_name));
        let timeindex_path = dir.join(format!("{}.timeindex", base_name));

        let log_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&log_path)?;

        let offset_index = OffsetIndex::open(&index_path, max_index_size)?;
        let time_index = TimeIndex::open(&timeindex_path, max_index_size)?;

        Ok(Self {
            base_offset,
            log_file,
            write_position: 0,
            max_timestamp: i64::MIN,
            state: SegmentState::Active,
            offset_index,
            time_index,
            dir_path: dir.to_path_buf(),
            batch_count: 0,
            last_relative_offset: 0,
        })
    }

    /// 从已有文件打开 Segment (用于恢复)
    pub fn open(dir: &Path, base_offset: u64, max_index_size: usize) -> Result<Self> {
        let base_name = log_io::format_segment_base_name(base_offset);

        let log_path = dir.join(format!("{}.log", base_name));
        let index_path = dir.join(format!("{}.index", base_name));
        let timeindex_path = dir.join(format!("{}.timeindex", base_name));

        let log_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&log_path)?;

        let write_position = log_file.metadata()?.len();

        let offset_index = OffsetIndex::open(&index_path, max_index_size)?;
        let time_index = TimeIndex::open(&timeindex_path, max_index_size)?;

        // 扫描最后一个 batch 获取 max_timestamp 和 last_relative_offset
        let mut scan_file = File::open(&log_path)?;
        let file_size = write_position;
        let batches = log_io::scan_batches_from_file(&mut scan_file, 0, file_size);

        let (max_timestamp, last_relative_offset, batch_count) = if batches.is_empty() {
            (i64::MIN, 0u32, 0u64)
        } else {
            let batch_count = batches.len() as u64;
            let last_batch = batches.last().unwrap();
            let mut reader = KafkaReader::new(&last_batch.1);
            match decode_batch_header(&mut reader) {
                Ok(hdr) => {
                    let last_rel = (hdr.base_offset as u64)
                        .wrapping_sub(base_offset)
                        .wrapping_add(hdr.last_offset_delta as u64) as u32;
                    (hdr.max_timestamp, last_rel, batch_count)
                }
                Err(_) => (i64::MIN, 0u32, batch_count),
            }
        };

        Ok(Self {
            base_offset,
            log_file,
            write_position,
            max_timestamp,
            state: SegmentState::Sealed, // 恢复时默认 sealed，后续可设 active
            offset_index,
            time_index,
            dir_path: dir.to_path_buf(),
            batch_count,
            last_relative_offset,
        })
    }

    /// 追加写入一个 RecordBatch
    ///
    /// `batch_bytes` 为从 base_offset 开始的完整 batch 字节。
    /// 返回本 batch 第一条消息的绝对 offset。
    pub fn append(&mut self, batch_bytes: &[u8]) -> Result<u64> {
        if self.state != SegmentState::Active {
            return Err(RkError::Storage("Segment is sealed".to_string()));
        }

        // 解析 batch header 获取信息
        if batch_bytes.len() < HEADER_SIZE {
            return Err(RkError::Protocol("Batch too small".to_string()));
        }

        let mut reader = KafkaReader::new(batch_bytes);
        let hdr = decode_batch_header(&mut reader)?;

        let batch_file_offset = self.write_position;
        let first_offset = hdr.base_offset as u64;
        let _record_count = hdr.records_count;

        // 写入 .log 文件
        self.log_file.seek(SeekFrom::End(0))?;
        self.log_file.write_all(batch_bytes)?;
        self.write_position += batch_bytes.len() as u64;

        // 更新稀疏索引 (每写入一个 batch 添加一条索引条目)
        let relative_offset = (hdr.base_offset as u64).wrapping_sub(self.base_offset) as u32;
        self.offset_index.append(relative_offset, batch_file_offset as u32)?;

        // 更新时间索引
        if hdr.max_timestamp > self.max_timestamp {
            self.max_timestamp = hdr.max_timestamp;
        }
        self.time_index.append(hdr.base_timestamp, relative_offset)?;

        // 更新元数据
        self.batch_count += 1;
        self.last_relative_offset = relative_offset
            .wrapping_add(hdr.last_offset_delta as u32);

        Ok(first_offset)
    }

    /// 从指定物理位置读取一个 RecordBatch
    pub fn read_batch(&mut self, position: u64) -> Result<(Vec<u8>, u64)> {
        log_io::read_batch_from_file(&mut self.log_file, position)
    }

    /// 从指定物理位置仅读取 RecordBatch 头部
    pub fn read_batch_header(&mut self, position: u64) -> Result<RecordBatchHeader> {
        log_io::read_batch_header_from_file(&mut self.log_file, position)
    }

    /// Offset 索引查找
    pub fn lookup_offset(&self, relative_offset: u32) -> Option<crate::index::OffsetIndexEntry> {
        self.offset_index.lookup(relative_offset)
    }

    /// Time 索引查找
    pub fn lookup_time(&self, timestamp: i64) -> Option<crate::index::TimeIndexEntry> {
        self.time_index.lookup(timestamp)
    }

    /// Seal 此 Segment (标记为只读)
    pub fn seal(&mut self) -> Result<()> {
        self.state = SegmentState::Sealed;
        self.offset_index.flush()?;
        self.time_index.flush()?;
        self.log_file.sync_all()?;
        Ok(())
    }

    /// 刷盘
    pub fn flush(&self) -> Result<()> {
        self.log_file.sync_all()?;
        self.offset_index.flush()?;
        self.time_index.flush()?;
        Ok(())
    }

    /// 获取当前写入位置
    pub fn write_position(&self) -> u64 {
        self.write_position
    }

    /// 获取 base_offset
    pub fn base_offset(&self) -> u64 {
        self.base_offset
    }

    /// 获取状态
    pub fn state(&self) -> SegmentState {
        self.state
    }

    /// 设置为 Active (用于恢复后)
    pub fn set_active(&mut self) {
        self.state = SegmentState::Active;
    }

    /// 获取最大时间戳
    pub fn max_timestamp(&self) -> i64 {
        self.max_timestamp
    }

    /// 获取 batch 数量
    pub fn batch_count(&self) -> u64 {
        self.batch_count
    }

    /// 获取最后一条消息的相对 offset
    pub fn last_relative_offset(&self) -> u32 {
        self.last_relative_offset
    }

    /// 删除 Segment 文件
    pub fn delete(self) -> Result<()> {
        let base_name = log_io::format_segment_base_name(self.base_offset);
        let log_path = self.dir_path.join(format!("{}.log", base_name));
        let index_path = self.dir_path.join(format!("{}.index", base_name));
        let timeindex_path = self.dir_path.join(format!("{}.timeindex", base_name));

        drop(self.log_file);
        if log_path.exists() { fs::remove_file(&log_path)?; }
        if index_path.exists() { fs::remove_file(&index_path)?; }
        if timeindex_path.exists() { fs::remove_file(&timeindex_path)?; }

        Ok(())
    }

    /// 扫描本 Segment 所有有效 batch (用于 recovery)
    pub fn scan_all_batches(&mut self) -> Result<Vec<(u64, Vec<u8>)>> {
        let file_size = self.log_file.metadata()?.len();
        let mut file = File::open(self.log_path())?;
        Ok(log_io::scan_batches_from_file(&mut file, 0, file_size))
    }

    /// .log 文件路径
    pub fn log_path(&self) -> PathBuf {
        let base_name = log_io::format_segment_base_name(self.base_offset);
        self.dir_path.join(format!("{}.log", base_name))
    }

    /// 重建索引 (用于 recovery)
    pub fn rebuild_indexes(&mut self) -> Result<u64> {
        // 清空现有索引
        self.offset_index.truncate(0);
        self.time_index.truncate(0);

        let file_size = self.log_file.metadata()?.len();
        let mut file = File::open(self.log_path())?;
        let batches = log_io::scan_batches_from_file(&mut file, 0, file_size);

        let mut max_ts = i64::MIN;
        for (pos, batch_bytes) in &batches {
            let mut reader = KafkaReader::new(batch_bytes);
            let hdr = decode_batch_header(&mut reader)?;

            let relative_offset = (hdr.base_offset as u64).wrapping_sub(self.base_offset) as u32;
            self.offset_index.append(relative_offset, *pos as u32)?;
            self.time_index.append(hdr.base_timestamp, relative_offset)?;

            if hdr.max_timestamp > max_ts {
                max_ts = hdr.max_timestamp;
            }
        }

        self.batch_count = batches.len() as u64;
        self.max_timestamp = max_ts;
        self.write_position = file_size;

        self.offset_index.flush()?;
        self.time_index.flush()?;

        Ok(file_size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::DEFAULT_MAX_INDEX_SIZE;
    use crate::log_io::build_batch_bytes;
    use tempfile::tempdir;

    fn make_test_batch(base_offset: i64, record_count: i32) -> Vec<u8> {
        let records = vec![0u8; record_count as usize * 10]; // fake records
        build_batch_bytes(
            base_offset, 1, 0, 1000, 2000, -1, -1, -1, &records, record_count,
        )
    }

    #[test]
    fn test_segment_create_and_append() {
        let dir = tempdir().unwrap();
        let mut seg = LogSegment::create(dir.path(), 0, DEFAULT_MAX_INDEX_SIZE).unwrap();

        assert_eq!(seg.state(), SegmentState::Active);
        assert_eq!(seg.base_offset(), 0);
        assert_eq!(seg.write_position(), 0);

        let batch = make_test_batch(0, 5);
        let offset = seg.append(&batch).unwrap();
        assert_eq!(offset, 0);
        assert!(seg.write_position() > 0);
        assert_eq!(seg.batch_count(), 1);
    }

    #[test]
    fn test_segment_multiple_appends() {
        let dir = tempdir().unwrap();
        let mut seg = LogSegment::create(dir.path(), 0, DEFAULT_MAX_INDEX_SIZE).unwrap();

        let batch1 = make_test_batch(0, 5);
        let batch2 = make_test_batch(5, 3);
        let batch3 = make_test_batch(8, 2);

        seg.append(&batch1).unwrap();
        seg.append(&batch2).unwrap();
        seg.append(&batch3).unwrap();

        assert_eq!(seg.batch_count(), 3);
        assert!(seg.write_position() > 0);
    }

    #[test]
    fn test_segment_read_batch() {
        let dir = tempdir().unwrap();
        let mut seg = LogSegment::create(dir.path(), 0, DEFAULT_MAX_INDEX_SIZE).unwrap();

        let batch = make_test_batch(0, 5);
        seg.append(&batch).unwrap();

        // 从位置 0 读取
        let (read_batch, size) = seg.read_batch(0).unwrap();
        assert_eq!(read_batch, batch);
        assert_eq!(size, batch.len() as u64);
    }

    #[test]
    fn test_segment_offset_lookup() {
        let dir = tempdir().unwrap();
        let mut seg = LogSegment::create(dir.path(), 0, DEFAULT_MAX_INDEX_SIZE).unwrap();

        let batch1 = make_test_batch(0, 5);
        let batch2 = make_test_batch(5, 3);
        let pos_after_batch1 = batch1.len() as u32;

        seg.append(&batch1).unwrap();
        seg.append(&batch2).unwrap();

        // 查找 offset 0 → 应该在位置 0
        let entry = seg.lookup_offset(0).unwrap();
        assert_eq!(entry.relative_offset, 0);
        assert_eq!(entry.physical_position, 0);

        // 查找 offset 5 → 应该在 batch1 之后
        let entry = seg.lookup_offset(5).unwrap();
        assert_eq!(entry.relative_offset, 5);
        assert_eq!(entry.physical_position, pos_after_batch1);

        // 查找 offset 3 → 应返回 ≤ 3 的最大条目 = offset 0
        let entry = seg.lookup_offset(3).unwrap();
        assert_eq!(entry.relative_offset, 0);
    }

    #[test]
    fn test_segment_seal() {
        let dir = tempdir().unwrap();
        let mut seg = LogSegment::create(dir.path(), 0, DEFAULT_MAX_INDEX_SIZE).unwrap();

        let batch = make_test_batch(0, 5);
        seg.append(&batch).unwrap();
        seg.seal().unwrap();

        assert_eq!(seg.state(), SegmentState::Sealed);

        // sealed 后不能再写入
        let result = seg.append(&batch);
        assert!(result.is_err());
    }

    #[test]
    fn test_segment_open_existing() {
        let dir = tempdir().unwrap();

        // 创建并写入
        {
            let mut seg = LogSegment::create(dir.path(), 0, DEFAULT_MAX_INDEX_SIZE).unwrap();
            seg.append(&make_test_batch(0, 5)).unwrap();
            seg.append(&make_test_batch(5, 3)).unwrap();
            seg.seal().unwrap();
        }

        // 重新打开
        let seg = LogSegment::open(dir.path(), 0, DEFAULT_MAX_INDEX_SIZE).unwrap();
        assert_eq!(seg.base_offset(), 0);
        assert_eq!(seg.batch_count(), 2);
        assert_eq!(seg.state(), SegmentState::Sealed);
    }

    #[test]
    fn test_segment_rebuild_indexes() {
        let dir = tempdir().unwrap();

        // 创建并写入
        {
            let mut seg = LogSegment::create(dir.path(), 0, DEFAULT_MAX_INDEX_SIZE).unwrap();
            seg.append(&make_test_batch(0, 5)).unwrap();
            seg.append(&make_test_batch(5, 3)).unwrap();
            seg.seal().unwrap();
        }

        // 重新打开并重建索引
        let mut seg = LogSegment::open(dir.path(), 0, DEFAULT_MAX_INDEX_SIZE).unwrap();
        seg.rebuild_indexes().unwrap();

        // 索引应该已重建
        let entry = seg.lookup_offset(0).unwrap();
        assert_eq!(entry.relative_offset, 0);
        assert_eq!(entry.physical_position, 0);

        let entry = seg.lookup_offset(5).unwrap();
        assert_eq!(entry.relative_offset, 5);
    }

    #[test]
    fn test_segment_with_nonzero_base_offset() {
        let dir = tempdir().unwrap();
        let mut seg = LogSegment::create(dir.path(), 1000, DEFAULT_MAX_INDEX_SIZE).unwrap();

        let batch = make_test_batch(1000, 5);
        let offset = seg.append(&batch).unwrap();
        assert_eq!(offset, 1000);

        let entry = seg.lookup_offset(0).unwrap(); // relative offset 0
        assert_eq!(entry.relative_offset, 0);
        assert_eq!(entry.physical_position, 0);
    }
}
