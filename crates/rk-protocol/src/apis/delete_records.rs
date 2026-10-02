//! DeleteRecords API (Key = 21)
//!
//! 删除指定 partition 中指定偏移量之前的记录 (日志截断)。
//! 实际上是将 log_start_offset 前移到指定位置。

use crate::codec::{KafkaRequestDecoder, KafkaResponseEncoder};
use crate::error_codes::KafkaErrorCode;
use crate::types::{KafkaReader, KafkaWriter};
use rk_core::error::Result;

// ─── Request ──────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct DeleteRecordsRequest {
    pub topics: Vec<DeleteRecordsRequestTopic>,
    pub timeout_ms: i32,
}

#[derive(Debug, Clone)]
pub struct DeleteRecordsRequestTopic {
    pub name: String,
    pub partitions: Vec<DeleteRecordsRequestPartition>,
}

#[derive(Debug, Clone)]
pub struct DeleteRecordsRequestPartition {
    pub index: i32,
    /// 删除此偏移量之前的所有记录
    pub offset: i64,
}

impl KafkaRequestDecoder for DeleteRecordsRequest {
    fn decode(reader: &mut KafkaReader<'_>, version: i16) -> Result<Self> {
        if version >= 2 {
            // Flexible v2+
            let topics = reader.read_compact_array(|r| {
                let name = r.read_compact_string()?;
                let partitions = r.read_compact_array(|r2| {
                    let index = r2.read_i32()?;
                    let offset = r2.read_i64()?;
                    let _tags = r2.read_tagged_fields()?;
                    Ok(DeleteRecordsRequestPartition { index, offset })
                })?;
                let _tags = r.read_tagged_fields()?;
                Ok(DeleteRecordsRequestTopic { name, partitions })
            })?;
            let timeout_ms = reader.read_i32()?;
            let _tags = reader.read_tagged_fields()?;
            Ok(Self { topics, timeout_ms })
        } else {
            // Legacy v0-v1
            let topics = reader.read_array(|r| {
                let name = r.read_string()?;
                let partitions = r.read_array(|r2| {
                    let index = r2.read_i32()?;
                    let offset = r2.read_i64()?;
                    Ok(DeleteRecordsRequestPartition { index, offset })
                })?;
                Ok(DeleteRecordsRequestTopic { name, partitions })
            })?;
            let timeout_ms = reader.read_i32()?;
            Ok(Self { topics, timeout_ms })
        }
    }
}

impl KafkaResponseEncoder for DeleteRecordsRequest {
    fn encode(&self, _writer: &mut KafkaWriter<'_>, _version: i16) -> Result<()> {
        Ok(()) // Request only
    }
}

// ─── Response ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct DeleteRecordsResponse {
    /// v1+: throttle_time_ms (i32)
    pub throttle_time_ms: i32,
    pub topics: Vec<DeleteRecordsResponseTopic>,
}

#[derive(Debug, Clone)]
pub struct DeleteRecordsResponseTopic {
    pub name: String,
    pub partitions: Vec<DeleteRecordsResponsePartition>,
}

#[derive(Debug, Clone)]
pub struct DeleteRecordsResponsePartition {
    pub index: i32,
    /// 新的 log_start_offset
    pub low_watermark: i64,
    pub error_code: KafkaErrorCode,
}

impl KafkaResponseEncoder for DeleteRecordsResponse {
    fn encode(&self, writer: &mut KafkaWriter<'_>, version: i16) -> Result<()> {
        if version >= 2 {
            // Flexible v2+
            writer.write_i32(self.throttle_time_ms);
            writer.write_compact_array(&self.topics, |w, topic| {
                w.write_compact_string(&topic.name);
                w.write_compact_array(&topic.partitions, |w2, part| {
                    w2.write_i32(part.index);
                    w2.write_i64(part.low_watermark);
                    w2.write_i16(part.error_code as i16);
                    w2.write_tagged_fields(&[]);
                });
                w.write_tagged_fields(&[]);
            });
            writer.write_tagged_fields(&[]);
        } else {
            // Legacy v0-v1
            writer.write_i32(self.throttle_time_ms);
            writer.write_array(&self.topics, |w, topic| {
                w.write_string(&topic.name);
                w.write_array(&topic.partitions, |w2, part| {
                    w2.write_i32(part.index);
                    w2.write_i64(part.low_watermark);
                    w2.write_i16(part.error_code as i16);
                });
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_delete_records_encode_v0() {
        let resp = DeleteRecordsResponse {
            throttle_time_ms: 0,
            topics: vec![DeleteRecordsResponseTopic {
                name: "test".to_string(),
                partitions: vec![DeleteRecordsResponsePartition {
                    index: 0,
                    low_watermark: 10,
                    error_code: KafkaErrorCode::None,
                }],
            }],
        };
        let mut buf = BytesMut::new();
        let mut writer = KafkaWriter::new(&mut buf);
        resp.encode(&mut writer, 0).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_delete_records_encode_v2() {
        let resp = DeleteRecordsResponse {
            throttle_time_ms: 100,
            topics: vec![DeleteRecordsResponseTopic {
                name: "test".to_string(),
                partitions: vec![DeleteRecordsResponsePartition {
                    index: 0,
                    low_watermark: 5,
                    error_code: KafkaErrorCode::None,
                }],
            }],
        };
        let mut buf = BytesMut::new();
        let mut writer = KafkaWriter::new(&mut buf);
        resp.encode(&mut writer, 2).unwrap();
        assert!(!buf.is_empty());
    }
}
