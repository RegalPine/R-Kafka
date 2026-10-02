//! ListPartitionReassignments API (Key = 46)
//!
//! 列出当前正在进行的 partition 重新分配。
//! KIP-455, v0+ 全部为 Flexible 格式。

use rk_core::error::Result;
use crate::codec::{KafkaRequestDecoder, KafkaResponseEncoder};
use crate::types::{KafkaReader, KafkaWriter};
use crate::error_codes::KafkaErrorCode;

// ─── Request ──────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ListPartitionReassignmentsRequest {
    pub timeout_ms: i32,
    /// null 表示列出所有 partition
    pub topics: Option<Vec<ListPartitionReassignmentsRequestTopic>>,
}

#[derive(Debug, Clone)]
pub struct ListPartitionReassignmentsRequestTopic {
    pub name: String,
    pub partitions: Vec<i32>,
}

impl KafkaRequestDecoder for ListPartitionReassignmentsRequest {
    fn decode(reader: &mut KafkaReader<'_>, _version: i16) -> Result<Self> {
        // v0+ is always flexible
        let timeout_ms = reader.read_i32()?;
        let topics = reader.read_compact_nullable_array(|r| {
            let name = r.read_compact_string()?;
            let partitions = r.read_compact_array(|r2| r2.read_i32())?;
            let _tags = r.read_tagged_fields();
            Ok(ListPartitionReassignmentsRequestTopic { name, partitions })
        })?;
        let _tags = reader.read_tagged_fields();
        Ok(Self { timeout_ms, topics })
    }
}

impl KafkaResponseEncoder for ListPartitionReassignmentsRequest {
    fn encode(&self, _writer: &mut KafkaWriter<'_>, _version: i16) -> Result<()> {
        Ok(()) // Request only
    }
}

// ─── Response ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ListPartitionReassignmentsResponse {
    pub throttle_time_ms: i32,
    pub error_code: KafkaErrorCode,
    pub error_message: Option<String>,
    pub topics: Vec<ListPartitionReassignmentsResponseTopic>,
}

#[derive(Debug, Clone)]
pub struct ListPartitionReassignmentsResponseTopic {
    pub name: String,
    pub partitions: Vec<ListPartitionReassignmentsResponsePartition>,
}

#[derive(Debug, Clone)]
pub struct ListPartitionReassignmentsResponsePartition {
    pub index: i32,
    pub replicas: Vec<i32>,
    pub adding_replicas: Vec<i32>,
    pub removing_replicas: Vec<i32>,
}

impl KafkaResponseEncoder for ListPartitionReassignmentsResponse {
    fn encode(&self, writer: &mut KafkaWriter<'_>, _version: i16) -> Result<()> {
        // Always flexible
        writer.write_i32(self.throttle_time_ms);
        writer.write_i16(self.error_code as i16);
        writer.write_compact_nullable_string(self.error_message.as_deref());
        writer.write_compact_array(&self.topics, |w, topic| {
            w.write_compact_string(&topic.name);
            w.write_compact_array(&topic.partitions, |w2, part| {
                w2.write_i32(part.index);
                w2.write_compact_array(&part.replicas, |w3, r| w3.write_i32(*r));
                w2.write_compact_array(&part.adding_replicas, |w3, r| w3.write_i32(*r));
                w2.write_compact_array(&part.removing_replicas, |w3, r| w3.write_i32(*r));
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
    fn test_list_partition_reassignments_encode() {
        let resp = ListPartitionReassignmentsResponse {
            throttle_time_ms: 0,
            error_code: KafkaErrorCode::None,
            error_message: None,
            topics: vec![ListPartitionReassignmentsResponseTopic {
                name: "test".to_string(),
                partitions: vec![ListPartitionReassignmentsResponsePartition {
                    index: 0,
                    replicas: vec![1, 2, 3],
                    adding_replicas: vec![],
                    removing_replicas: vec![],
                }],
            }],
        };
        let mut buf = BytesMut::new();
        let mut writer = KafkaWriter::new(&mut buf);
        resp.encode(&mut writer, 0).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_list_partition_reassignments_empty() {
        let resp = ListPartitionReassignmentsResponse {
            throttle_time_ms: 0,
            error_code: KafkaErrorCode::None,
            error_message: None,
            topics: vec![],
        };
        let mut buf = BytesMut::new();
        let mut writer = KafkaWriter::new(&mut buf);
        resp.encode(&mut writer, 0).unwrap();
        assert!(!buf.is_empty());
    }
}
