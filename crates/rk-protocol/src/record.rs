//! Kafka RecordBatch v2 编解码 (Wire Format)
//!
//! 磁盘格式与 Apache Kafka RecordBatch v2 **完全一致**。
//! 本模块处理线上协议级编解码 (wire format)，
//! 磁盘级 I/O 在 rk-storage/log_io.rs 中。
//!
//! RecordBatch 布局:
//! ```text
//! base_offset            (i64)   8 bytes
//! batch_length           (i32)   4 bytes
//! partition_leader_epoch (i32)   4 bytes
//! magic                  (i8)    1 byte   (= 2)
//! crc                    (i32)   4 bytes  (CRC32C from magic to end)
//! attributes             (i16)   2 bytes
//! last_offset_delta      (i32)   4 bytes
//! base_timestamp         (i64)   8 bytes
//! max_timestamp          (i64)   8 bytes
//! producer_id            (i64)   8 bytes
//! producer_epoch         (i16)   2 bytes
//! base_sequence          (i32)   4 bytes
//! records_count          (i32)   4 bytes
//! records                (...)   variable
//! ```

use crate::types::{KafkaReader, KafkaWriter};
use rk_core::error::Result;

/// RecordBatch magic 值 (v2)
pub const RECORDBATCH_MAGIC: i8 = 2;

/// CRC 起始偏移 (从 batch 开头: base_offset 8 + batch_length 4 + partition_leader_epoch 4 = 16)
pub const CRC_OFFSET: usize = 16;

/// RecordBatch 头部固定大小 (不含 records)
pub const HEADER_SIZE: usize = 61;

/// 压缩类型 (attributes bit 0-2)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i16)]
pub enum CompressionType {
    None = 0,
    Gzip = 1,
    Snappy = 2,
    Lz4 = 3,
    Zstd = 4,
}

impl CompressionType {
    pub fn from_attributes(attrs: i16) -> Self {
        match attrs & 0x07 {
            0 => Self::None,
            1 => Self::Gzip,
            2 => Self::Snappy,
            3 => Self::Lz4,
            4 => Self::Zstd,
            _ => Self::None,
        }
    }
}

/// 时间戳类型 (attributes bit 3)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i16)]
pub enum TimestampType {
    CreateTime = 0,
    LogAppendTime = 1,
}

impl TimestampType {
    pub fn from_attributes(attrs: i16) -> Self {
        if attrs & 0x08 != 0 {
            Self::LogAppendTime
        } else {
            Self::CreateTime
        }
    }
}

/// 解析后的 RecordBatch 头部
#[derive(Debug, Clone)]
pub struct RecordBatchHeader {
    pub base_offset: i64,
    pub batch_length: i32,
    pub partition_leader_epoch: i32,
    pub magic: i8,
    pub crc: i32,
    pub attributes: i16,
    pub last_offset_delta: i32,
    pub base_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub records_count: i32,
}

impl RecordBatchHeader {
    /// 压缩类型
    pub fn compression_type(&self) -> CompressionType {
        CompressionType::from_attributes(self.attributes)
    }

    /// 时间戳类型
    pub fn timestamp_type(&self) -> TimestampType {
        TimestampType::from_attributes(self.attributes)
    }

    /// 是否事务消息
    pub fn is_transactional(&self) -> bool {
        self.attributes & 0x10 != 0
    }

    /// 是否控制批次
    pub fn is_control_batch(&self) -> bool {
        self.attributes & 0x20 != 0
    }

    /// 是否有 deleteHorizonMs
    pub fn has_delete_horizon_ms(&self) -> bool {
        self.attributes & 0x40 != 0
    }

    /// 验证 CRC32C
    /// `batch_bytes` 为从 base_offset 开始的完整 batch 字节
    pub fn verify_crc(&self, batch_bytes: &[u8]) -> bool {
        let computed = crc32c::crc32c(&batch_bytes[CRC_OFFSET..]);
        computed == self.crc as u32
    }

    /// 计算 CRC32C (从 magic 字段开始)
    pub fn compute_crc(batch_bytes: &[u8]) -> u32 {
        crc32c::crc32c(&batch_bytes[CRC_OFFSET..])
    }
}

/// 从字节流读取 RecordBatch 头部
pub fn decode_batch_header(reader: &mut KafkaReader<'_>) -> Result<RecordBatchHeader> {
    Ok(RecordBatchHeader {
        base_offset: reader.read_i64()?,
        batch_length: reader.read_i32()?,
        partition_leader_epoch: reader.read_i32()?,
        magic: reader.read_i8()?,
        crc: reader.read_i32()?,
        attributes: reader.read_i16()?,
        last_offset_delta: reader.read_i32()?,
        base_timestamp: reader.read_i64()?,
        max_timestamp: reader.read_i64()?,
        producer_id: reader.read_i64()?,
        producer_epoch: reader.read_i16()?,
        base_sequence: reader.read_i32()?,
        records_count: reader.read_i32()?,
    })
}

/// 将 RecordBatch 头部写入字节流
pub fn encode_batch_header(writer: &mut KafkaWriter<'_>, hdr: &RecordBatchHeader) {
    writer.write_i64(hdr.base_offset);
    writer.write_i32(hdr.batch_length);
    writer.write_i32(hdr.partition_leader_epoch);
    writer.write_i8(hdr.magic);
    writer.write_i32(hdr.crc);
    writer.write_i16(hdr.attributes);
    writer.write_i32(hdr.last_offset_delta);
    writer.write_i64(hdr.base_timestamp);
    writer.write_i64(hdr.max_timestamp);
    writer.write_i64(hdr.producer_id);
    writer.write_i16(hdr.producer_epoch);
    writer.write_i32(hdr.base_sequence);
    writer.write_i32(hdr.records_count);
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_batch_header_roundtrip() {
        let hdr = RecordBatchHeader {
            base_offset: 0,
            batch_length: 100,
            partition_leader_epoch: 1,
            magic: RECORDBATCH_MAGIC,
            crc: 0,
            attributes: 0,
            last_offset_delta: 9,
            base_timestamp: 1000,
            max_timestamp: 2000,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records_count: 10,
        };

        let mut buf = BytesMut::with_capacity(HEADER_SIZE);
        let mut w = KafkaWriter::new(&mut buf);
        encode_batch_header(&mut w, &hdr);
        assert_eq!(buf.len(), HEADER_SIZE);

        let mut r = KafkaReader::new(&buf);
        let decoded = decode_batch_header(&mut r).unwrap();
        assert_eq!(decoded.base_offset, 0);
        assert_eq!(decoded.batch_length, 100);
        assert_eq!(decoded.magic, RECORDBATCH_MAGIC);
        assert_eq!(decoded.records_count, 10);
    }

    #[test]
    fn test_attributes_parsing() {
        // compression=snappy(2), timestamp_type=LogAppendTime(1), isTransactional=true
        let attrs: i16 = 2 | 0x08 | 0x10;
        assert_eq!(
            CompressionType::from_attributes(attrs),
            CompressionType::Snappy
        );
        assert_eq!(
            TimestampType::from_attributes(attrs),
            TimestampType::LogAppendTime
        );
        assert!(attrs & 0x10 != 0); // transactional
        assert!(attrs & 0x20 == 0); // not control batch
    }
}
