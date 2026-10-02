//! OffsetDelete API (Key = 47)
//!
//! 删除消费者组的已提交偏移量 (Admin 操作)。
//! KIP-496, v0+ 全部为 Flexible 格式。

use rk_core::error::Result;
use crate::codec::{KafkaRequestDecoder, KafkaResponseEncoder};
use crate::types::{KafkaReader, KafkaWriter};
use crate::error_codes::KafkaErrorCode;

// ─── Request ──────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct OffsetDeleteRequest {
    pub group_id: String,
    pub topics: Vec<OffsetDeleteRequestTopic>,
}

#[derive(Debug, Clone)]
pub struct OffsetDeleteRequestTopic {
    pub name: String,
    pub partitions: Vec<OffsetDeleteRequestPartition>,
}

#[derive(Debug, Clone)]
pub struct OffsetDeleteRequestPartition {
    pub index: i32,
}

impl KafkaRequestDecoder for OffsetDeleteRequest {
    fn decode(reader: &mut KafkaReader<'_>, _version: i16) -> Result<Self> {
        // v0+ is always flexible
        let group_id = reader.read_compact_string()?;
        let topics = reader.read_compact_array(|r| {
            let name = r.read_compact_string()?;
            let partitions = r.read_compact_array(|r2| {
                let index = r2.read_i32()?;
                let _tags = r2.read_tagged_fields();
                Ok(OffsetDeleteRequestPartition { index })
            })?;
            let _tags = r.read_tagged_fields();
            Ok(OffsetDeleteRequestTopic { name, partitions })
        })?;
        let _tags = reader.read_tagged_fields();
        Ok(Self { group_id, topics })
    }
}

impl KafkaResponseEncoder for OffsetDeleteRequest {
    fn encode(&self, _writer: &mut KafkaWriter<'_>, _version: i16) -> Result<()> {
        Ok(()) // Request only
    }
}

// ─── Response ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct OffsetDeleteResponse {
    pub throttle_time_ms: i32,
    pub error_code: KafkaErrorCode,
    pub topics: Vec<OffsetDeleteResponseTopic>,
}

#[derive(Debug, Clone)]
pub struct OffsetDeleteResponseTopic {
    pub name: String,
    pub partitions: Vec<OffsetDeleteResponsePartition>,
}

#[derive(Debug, Clone)]
pub struct OffsetDeleteResponsePartition {
    pub index: i32,
    pub error_code: KafkaErrorCode,
}

impl KafkaResponseEncoder for OffsetDeleteResponse {
    fn encode(&self, writer: &mut KafkaWriter<'_>, _version: i16) -> Result<()> {
        // Always flexible
        writer.write_i32(self.throttle_time_ms);
        writer.write_i16(self.error_code as i16);
        writer.write_compact_array(&self.topics, |w, topic| {
            w.write_compact_string(&topic.name);
            w.write_compact_array(&topic.partitions, |w2, part| {
                w2.write_i32(part.index);
                w2.write_i16(part.error_code as i16);
                w2.write_tagged_fields(&[]);
            });
            w.write_tagged_fields(&[]);
        });
        writer.write_tagged_fields(&[]);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_offset_delete_encode() {
        let resp = OffsetDeleteResponse {
            throttle_time_ms: 0,
            error_code: KafkaErrorCode::None,
            topics: vec![OffsetDeleteResponseTopic {
                name: "test".to_string(),
                partitions: vec![OffsetDeleteResponsePartition {
                    index: 0,
                    error_code: KafkaErrorCode::None,
                }],
            }],
        };
        let mut buf = BytesMut::new();
        let mut writer = KafkaWriter::new(&mut buf);
        resp.encode(&mut writer, 0).unwrap();
        assert!(!buf.is_empty());
    }
}
