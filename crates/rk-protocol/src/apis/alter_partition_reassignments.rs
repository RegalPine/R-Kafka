//! AlterPartitionReassignments API (Key = 45)
//!
//! 修改 partition 的副本分配。
//! KIP-455, v0+ 全部为 Flexible 格式。

use rk_core::error::Result;
use crate::codec::{KafkaRequestDecoder, KafkaResponseEncoder};
use crate::types::{KafkaReader, KafkaWriter};
use crate::error_codes::KafkaErrorCode;

// ─── Request ──────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct AlterPartitionReassignmentsRequest {
    pub timeout_ms: i32,
    pub topics: Vec<AlterPartitionReassignmentsRequestTopic>,
}

#[derive(Debug, Clone)]
pub struct AlterPartitionReassignmentsRequestTopic {
    pub name: String,
    pub partitions: Vec<AlterPartitionReassignmentsRequestPartition>,
}

#[derive(Debug, Clone)]
pub struct AlterPartitionReassignmentsRequestPartition {
    pub index: i32,
    /// null 表示取消重新分配
    pub replicas: Option<Vec<i32>>,
}

impl KafkaRequestDecoder for AlterPartitionReassignmentsRequest {
    fn decode(reader: &mut KafkaReader<'_>, _version: i16) -> Result<Self> {
        // v0+ is always flexible
        let timeout_ms = reader.read_i32()?;
        let topics = reader.read_compact_array(|r| {
            let name = r.read_compact_string()?;
            let partitions = r.read_compact_array(|r2| {
                let index = r2.read_i32()?;
                let replicas = r2.read_compact_nullable_array(|r3| r3.read_i32())?;
                let _tags = r2.read_tagged_fields();
                Ok(AlterPartitionReassignmentsRequestPartition { index, replicas })
            })?;
            let _tags = r.read_tagged_fields();
            Ok(AlterPartitionReassignmentsRequestTopic { name, partitions })
        })?;
        let _tags = reader.read_tagged_fields();
        Ok(Self { timeout_ms, topics })
    }
}

impl KafkaResponseEncoder for AlterPartitionReassignmentsRequest {
    fn encode(&self, _writer: &mut KafkaWriter<'_>, _version: i16) -> Result<()> {
        Ok(()) // Request only
    }
}

// ─── Response ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct AlterPartitionReassignmentsResponse {
    pub throttle_time_ms: i32,
    pub error_code: KafkaErrorCode,
    pub error_message: Option<String>,
    pub topics: Vec<AlterPartitionReassignmentsResponseTopic>,
}

#[derive(Debug, Clone)]
pub struct AlterPartitionReassignmentsResponseTopic {
    pub name: String,
    pub partitions: Vec<AlterPartitionReassignmentsResponsePartition>,
}

#[derive(Debug, Clone)]
pub struct AlterPartitionReassignmentsResponsePartition {
    pub index: i32,
    pub error_code: KafkaErrorCode,
    pub error_message: Option<String>,
}

impl KafkaResponseEncoder for AlterPartitionReassignmentsResponse {
    fn encode(&self, writer: &mut KafkaWriter<'_>, _version: i16) -> Result<()> {
        // Always flexible
        writer.write_i32(self.throttle_time_ms);
        writer.write_i16(self.error_code as i16);
        writer.write_compact_nullable_string(self.error_message.as_deref());
        writer.write_compact_array(&self.topics, |w, topic| {
            w.write_compact_string(&topic.name);
            w.write_compact_array(&topic.partitions, |w2, part| {
                w2.write_i32(part.index);
                w2.write_i16(part.error_code as i16);
                w2.write_compact_nullable_string(part.error_message.as_deref());
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
    fn test_alter_partition_reassignments_encode() {
        let resp = AlterPartitionReassignmentsResponse {
            throttle_time_ms: 0,
            error_code: KafkaErrorCode::None,
            error_message: None,
            topics: vec![AlterPartitionReassignmentsResponseTopic {
                name: "test".to_string(),
                partitions: vec![AlterPartitionReassignmentsResponsePartition {
                    index: 0,
                    error_code: KafkaErrorCode::None,
                    error_message: None,
                }],
            }],
        };
        let mut buf = BytesMut::new();
        let mut writer = KafkaWriter::new(&mut buf);
        resp.encode(&mut writer, 0).unwrap();
        assert!(!buf.is_empty());
    }
}
