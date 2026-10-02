//! OffsetIndex / TimeIndex: 稀疏索引 (mmap)
//!
//! ## OffsetIndex
//!
//! `.index` 文件: 每条记录 8 字节 (relative_offset: u32, physical_position: u32)
//! - relative_offset: 相对于 Segment base_offset 的偏移
//! - physical_position: 在 `.log` 文件中的字节偏移
//!
//! 查找: 二分查找 O(log n)，找到 ≤ target 的最大条目，然后顺序扫描 .log
//!
//! ## TimeIndex
//!
//! `.timeindex` 文件: 每条记录 12 字节 (timestamp: i64, offset: u32)
//! - timestamp: 消息时间戳
//! - offset: 相对于 Segment base_offset 的偏移
//!
//! 查找: 二分查找最接近的 timestamp，获取 offset 后走 OffsetIndex 流程

use std::fs::OpenOptions;
use std::path::{Path, PathBuf};

use memmap2::MmapMut;
use rk_core::error::{RkError, Result};

// ─── 索引条目大小 ──────────────────────────────────────────────────

/// Offset 索引条目大小: relative_offset(u32) + physical_position(u32) = 8 bytes
pub const OFFSET_INDEX_ENTRY_SIZE: usize = 8;

/// Time 索引条目大小: timestamp(i64) + relative_offset(u32) = 12 bytes
pub const TIME_INDEX_ENTRY_SIZE: usize = 12;

/// 默认索引文件最大大小 (可容纳的条目数 × 条目大小)
/// 默认 10MB for offset index = 1,250,000 entries
/// 默认 10MB for time index  = ~833,333 entries
pub const DEFAULT_MAX_INDEX_SIZE: usize = 10 * 1024 * 1024;

// ─── OffsetIndex ────────────────────────────────────────────────────

/// 稀疏索引条目 (offset → 物理位置)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct OffsetIndexEntry {
    /// 相对于 Segment base_offset 的偏移
    pub relative_offset: u32,
    /// 在 .log 文件中的字节偏移
    pub physical_position: u32,
}

/// Offset 稀疏索引 (mmap)
///
/// 每 4KB 数据写入一条索引条目 (稀疏)，通过二分查找快速定位。
pub struct OffsetIndex {
    mmap: MmapMut,
    entry_count: usize,
    max_entries: usize,
    #[allow(dead_code)]
    path: PathBuf,
}

impl OffsetIndex {
    /// 创建或打开索引文件
    pub fn open(path: &Path, max_size: usize) -> Result<Self> {
        let max_entries = max_size / OFFSET_INDEX_ENTRY_SIZE;

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;

        let file_len = file.metadata()?.len() as usize;

        // 确保文件至少分配 max_size
        if file_len < max_size {
            file.set_len(max_size as u64)?;
        }

        let mmap = unsafe { MmapMut::map_mut(&file)? };

        // 扫描确定已有条目数 (找到第一个全零条目即停止)
        let existing_entries = Self::count_valid_entries(&mmap, max_entries);

        Ok(Self {
            mmap,
            entry_count: existing_entries,
            max_entries,
            path: path.to_path_buf(),
        })
    }

    /// 扫描 mmap 计算有效条目数 (遇到全零条目停止)
    fn count_valid_entries(mmap: &MmapMut, max_entries: usize) -> usize {
        for i in 0..max_entries {
            let offset = i * OFFSET_INDEX_ENTRY_SIZE;
            if offset + OFFSET_INDEX_ENTRY_SIZE > mmap.len() {
                return i;
            }
            // 检查是否全零 (relative_offset == 0 && physical_position == 0 视为空)
            let relative_offset = u32::from_be_bytes([
                mmap[offset], mmap[offset + 1], mmap[offset + 2], mmap[offset + 3],
            ]);
            let physical_position = u32::from_be_bytes([
                mmap[offset + 4], mmap[offset + 5], mmap[offset + 6], mmap[offset + 7],
            ]);
            // 第一条 entry 可以是 (0, 0)，需要特殊处理:
            // 如果 i == 0 且两个字段都是 0，可能是有效条目也可能是空
            // 使用启发式: 如果 i == 0 且后面有非零条目，则 (0,0) 有效
            // 简化: 仅当 relative_offset 和 physical_position 都为 0 且 i > 0 时停止
            if i > 0 && relative_offset == 0 && physical_position == 0 {
                return i;
            }
            // 对于 i == 0 且全零的情况，检查下一个条目
            if i == 0 && relative_offset == 0 && physical_position == 0 {
                // 可能是空索引，也可能是有效第一条
                // 检查第二个条目位置是否也是全零
                let next_offset = OFFSET_INDEX_ENTRY_SIZE;
                if next_offset + OFFSET_INDEX_ENTRY_SIZE <= mmap.len() {
                    let next_rel = u32::from_be_bytes([
                        mmap[next_offset], mmap[next_offset + 1],
                        mmap[next_offset + 2], mmap[next_offset + 3],
                    ]);
                    let next_pos = u32::from_be_bytes([
                        mmap[next_offset + 4], mmap[next_offset + 5],
                        mmap[next_offset + 6], mmap[next_offset + 7],
                    ]);
                    if next_rel == 0 && next_pos == 0 {
                        return 0; // 空索引
                    }
                } else {
                    return 1; // 只有一个条目
                }
            }
        }
        max_entries
    }

    /// 追加一条索引条目
    pub fn append(&mut self, relative_offset: u32, physical_position: u32) -> Result<()> {
        if self.entry_count >= self.max_entries {
            return Err(RkError::Storage("OffsetIndex is full".to_string()));
        }

        let offset = self.entry_count * OFFSET_INDEX_ENTRY_SIZE;
        self.mmap[offset..offset + 4].copy_from_slice(&relative_offset.to_be_bytes());
        self.mmap[offset + 4..offset + 8].copy_from_slice(&physical_position.to_be_bytes());
        self.entry_count += 1;

        Ok(())
    }

    /// 二分查找: 找到 ≤ target_relative_offset 的最大条目
    pub fn lookup(&self, target_relative_offset: u32) -> Option<OffsetIndexEntry> {
        if self.entry_count == 0 {
            return None;
        }

        let entries = self.entries();
        // partition_point 返回第一个 > target 的位置
        let idx = entries.partition_point(|e| e.relative_offset <= target_relative_offset);

        if idx == 0 {
            None
        } else {
            Some(entries[idx - 1])
        }
    }

    /// 获取所有已写入的条目
    pub fn entries(&self) -> Vec<OffsetIndexEntry> {
        let mut result = Vec::with_capacity(self.entry_count);
        for i in 0..self.entry_count {
            let offset = i * OFFSET_INDEX_ENTRY_SIZE;
            let relative_offset = u32::from_be_bytes([
                self.mmap[offset],
                self.mmap[offset + 1],
                self.mmap[offset + 2],
                self.mmap[offset + 3],
            ]);
            let physical_position = u32::from_be_bytes([
                self.mmap[offset + 4],
                self.mmap[offset + 5],
                self.mmap[offset + 6],
                self.mmap[offset + 7],
            ]);
            result.push(OffsetIndexEntry {
                relative_offset,
                physical_position,
            });
        }
        result
    }

    /// 条目数
    pub fn len(&self) -> usize {
        self.entry_count
    }

    pub fn is_empty(&self) -> bool {
        self.entry_count == 0
    }

    /// 刷到磁盘
    pub fn flush(&self) -> Result<()> {
        self.mmap.flush()?;
        Ok(())
    }

    /// 截断到指定条目数 (用于 recovery)
    pub fn truncate(&mut self, entry_count: usize) {
        if entry_count < self.entry_count {
            let start = entry_count * OFFSET_INDEX_ENTRY_SIZE;
            let end = self.entry_count * OFFSET_INDEX_ENTRY_SIZE;
            for byte in &mut self.mmap[start..end] {
                *byte = 0;
            }
            self.entry_count = entry_count;
        }
    }
}

// ─── TimeIndex ──────────────────────────────────────────────────────

/// 时间索引条目 (timestamp → offset)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct TimeIndexEntry {
    /// 消息时间戳 (毫秒)
    pub timestamp: i64,
    /// 相对于 Segment base_offset 的偏移
    pub relative_offset: u32,
}

/// Time 稀疏索引 (mmap)
pub struct TimeIndex {
    mmap: MmapMut,
    entry_count: usize,
    max_entries: usize,
    #[allow(dead_code)]
    path: PathBuf,
}

impl TimeIndex {
    /// 创建或打开时间索引文件
    pub fn open(path: &Path, max_size: usize) -> Result<Self> {
        let max_entries = max_size / TIME_INDEX_ENTRY_SIZE;

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;

        let file_len = file.metadata()?.len() as usize;

        if file_len < max_size {
            file.set_len(max_size as u64)?;
        }

        let mmap = unsafe { MmapMut::map_mut(&file)? };

        // 扫描确定已有条目数 (timestamp == 0 视为空)
        let existing_entries = Self::count_valid_entries(&mmap, max_entries);

        Ok(Self {
            mmap,
            entry_count: existing_entries,
            max_entries,
            path: path.to_path_buf(),
        })
    }

    /// 扫描 mmap 计算有效条目数 (timestamp == 0 视为空)
    fn count_valid_entries(mmap: &MmapMut, max_entries: usize) -> usize {
        for i in 0..max_entries {
            let offset = i * TIME_INDEX_ENTRY_SIZE;
            if offset + TIME_INDEX_ENTRY_SIZE > mmap.len() {
                return i;
            }
            let timestamp = i64::from_be_bytes([
                mmap[offset], mmap[offset + 1], mmap[offset + 2], mmap[offset + 3],
                mmap[offset + 4], mmap[offset + 5], mmap[offset + 6], mmap[offset + 7],
            ]);
            if timestamp == 0 {
                return i;
            }
        }
        max_entries
    }

    /// 追加一条时间索引条目
    pub fn append(&mut self, timestamp: i64, relative_offset: u32) -> Result<()> {
        if self.entry_count >= self.max_entries {
            return Err(RkError::Storage("TimeIndex is full".to_string()));
        }

        let offset = self.entry_count * TIME_INDEX_ENTRY_SIZE;
        self.mmap[offset..offset + 8].copy_from_slice(&timestamp.to_be_bytes());
        self.mmap[offset + 8..offset + 12].copy_from_slice(&relative_offset.to_be_bytes());
        self.entry_count += 1;

        Ok(())
    }

    /// 二分查找: 找到 ≤ target_timestamp 的最大条目
    pub fn lookup(&self, target_timestamp: i64) -> Option<TimeIndexEntry> {
        if self.entry_count == 0 {
            return None;
        }

        let entries = self.entries();
        let idx = entries.partition_point(|e| e.timestamp <= target_timestamp);

        if idx == 0 {
            None
        } else {
            Some(entries[idx - 1])
        }
    }

    /// 获取所有已写入的条目
    pub fn entries(&self) -> Vec<TimeIndexEntry> {
        let mut result = Vec::with_capacity(self.entry_count);
        for i in 0..self.entry_count {
            let offset = i * TIME_INDEX_ENTRY_SIZE;
            let timestamp = i64::from_be_bytes([
                self.mmap[offset],
                self.mmap[offset + 1],
                self.mmap[offset + 2],
                self.mmap[offset + 3],
                self.mmap[offset + 4],
                self.mmap[offset + 5],
                self.mmap[offset + 6],
                self.mmap[offset + 7],
            ]);
            let relative_offset = u32::from_be_bytes([
                self.mmap[offset + 8],
                self.mmap[offset + 9],
                self.mmap[offset + 10],
                self.mmap[offset + 11],
            ]);
            result.push(TimeIndexEntry {
                timestamp,
                relative_offset,
            });
        }
        result
    }

    pub fn len(&self) -> usize {
        self.entry_count
    }

    pub fn is_empty(&self) -> bool {
        self.entry_count == 0
    }

    pub fn flush(&self) -> Result<()> {
        self.mmap.flush()?;
        Ok(())
    }

    pub fn truncate(&mut self, entry_count: usize) {
        if entry_count < self.entry_count {
            let start = entry_count * TIME_INDEX_ENTRY_SIZE;
            let end = self.entry_count * TIME_INDEX_ENTRY_SIZE;
            for byte in &mut self.mmap[start..end] {
                *byte = 0;
            }
            self.entry_count = entry_count;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_offset_index_append_and_lookup() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.index");
        let mut idx = OffsetIndex::open(&path, DEFAULT_MAX_INDEX_SIZE).unwrap();

        assert!(idx.is_empty());

        // 写入稀疏索引条目: 每 4KB 一条
        idx.append(0, 0).unwrap();
        idx.append(10, 4096).unwrap();
        idx.append(20, 8192).unwrap();
        idx.append(30, 12288).unwrap();

        assert_eq!(idx.len(), 4);

        // 精确命中
        let entry = idx.lookup(10).unwrap();
        assert_eq!(entry.relative_offset, 10);
        assert_eq!(entry.physical_position, 4096);

        // 中间值: 应返回 ≤ target 的最大条目
        let entry = idx.lookup(15).unwrap();
        assert_eq!(entry.relative_offset, 10);
        assert_eq!(entry.physical_position, 4096);

        let entry = idx.lookup(25).unwrap();
        assert_eq!(entry.relative_offset, 20);
        assert_eq!(entry.physical_position, 8192);

        // 超过最大值
        let entry = idx.lookup(100).unwrap();
        assert_eq!(entry.relative_offset, 30);
        assert_eq!(entry.physical_position, 12288);

        // 小于最小值
        assert!(idx.lookup(0).is_some());
        let entry = idx.lookup(0).unwrap();
        assert_eq!(entry.relative_offset, 0);
    }

    #[test]
    fn test_offset_index_empty_lookup() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.index");
        let idx = OffsetIndex::open(&path, DEFAULT_MAX_INDEX_SIZE).unwrap();
        assert!(idx.lookup(100).is_none());
    }

    #[test]
    fn test_offset_index_flush_and_reopen() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.index");

        {
            let mut idx = OffsetIndex::open(&path, DEFAULT_MAX_INDEX_SIZE).unwrap();
            idx.append(0, 0).unwrap();
            idx.append(10, 4096).unwrap();
            idx.flush().unwrap();
        }

        // 重新打开应恢复已有条目
        let idx = OffsetIndex::open(&path, DEFAULT_MAX_INDEX_SIZE).unwrap();
        assert_eq!(idx.len(), 2);
        let entry = idx.lookup(5).unwrap();
        assert_eq!(entry.relative_offset, 0);
    }

    #[test]
    fn test_offset_index_truncate() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.index");
        let mut idx = OffsetIndex::open(&path, DEFAULT_MAX_INDEX_SIZE).unwrap();

        idx.append(0, 0).unwrap();
        idx.append(10, 4096).unwrap();
        idx.append(20, 8192).unwrap();
        assert_eq!(idx.len(), 3);

        idx.truncate(1);
        assert_eq!(idx.len(), 1);
        let entries = idx.entries();
        assert_eq!(entries[0].relative_offset, 0);
    }

    #[test]
    fn test_time_index_append_and_lookup() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.timeindex");
        let mut idx = TimeIndex::open(&path, DEFAULT_MAX_INDEX_SIZE).unwrap();

        idx.append(1000, 0).unwrap();
        idx.append(2000, 100).unwrap();
        idx.append(3000, 200).unwrap();

        assert_eq!(idx.len(), 3);

        // 精确命中
        let entry = idx.lookup(2000).unwrap();
        assert_eq!(entry.timestamp, 2000);
        assert_eq!(entry.relative_offset, 100);

        // 中间值
        let entry = idx.lookup(2500).unwrap();
        assert_eq!(entry.timestamp, 2000);
        assert_eq!(entry.relative_offset, 100);

        // 超过最大值
        let entry = idx.lookup(5000).unwrap();
        assert_eq!(entry.timestamp, 3000);

        // 小于最小值
        assert!(idx.lookup(500).is_none());
    }

    #[test]
    fn test_time_index_flush_and_reopen() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.timeindex");

        {
            let mut idx = TimeIndex::open(&path, DEFAULT_MAX_INDEX_SIZE).unwrap();
            idx.append(1000, 0).unwrap();
            idx.append(2000, 100).unwrap();
            idx.flush().unwrap();
        }

        let idx = TimeIndex::open(&path, DEFAULT_MAX_INDEX_SIZE).unwrap();
        assert_eq!(idx.len(), 2);
        let entry = idx.lookup(1500).unwrap();
        assert_eq!(entry.timestamp, 1000);
    }
}
