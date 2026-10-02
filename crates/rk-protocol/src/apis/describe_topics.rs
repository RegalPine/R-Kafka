//! DescribeTopics API (Key = 70)
//!
//! 描述 Topic 的详细信息 (新版 API, KIP-951)。
//! v0+ 全部为 Flexible 格式。

use crate::codec::{KafkaRequestDecoder, KafkaResponseEncoder};
use crate::error_codes::KafkaErrorCode;
use crate::types::{KafkaReader, KafkaWriter};
use rk_core::error::Result;

// ─── Request ──────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct DescribeTopicsRequest {
    /// 要描述的 topic (null = 所有)
    pub topics: Option<Vec<DescribeTopicsRequestTopic>>,
}

#[derive(Debug, Clone)]
pub struct DescribeTopicsRequestTopic {
    pub name: String,
}

impl KafkaRequestDecoder for DescribeTopicsRequest {
    fn decode(reader: &mut KafkaReader<'_>, _version: i16) -> Result<Self> {
        // v0+ is always flexible
        let topics = reader.read_compact_nullable_array(|r| {
            let name = r.read_compact_string()?;
            let _tags = r.read_tagged_fields();
            Ok(DescribeTopicsRequestTopic { name })
        })?;
        let _tags = reader.read_tagged_fields();
        Ok(Self { topics })
    }
}

impl KafkaResponseEncoder for DescribeTopicsRequest {
    fn encode(&self, _writer: &mut KafkaWriter<'_>, _version: i16) -> Result<()> {
        Ok(()) // Request only
    }
}

// ─── Response ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct DescribeTopicsResponse {
    pub throttle_time_ms: i32,
    pub topics: Vec<DescribeTopicsResponseTopic>,
}

#[derive(Debug, Clone)]
pub struct DescribeTopicsResponseTopic {
    pub name: String,
    pub topic_id: [u8; 16],
    pub is_internal: bool,
    pub partitions: Vec<DescribeTopicsResponsePartition>,
    pub topic_authorized_operations: i32,
    pub error_code: KafkaErrorCode,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DescribeTopicsResponsePartition {
    pub partition_index: i32,
    pub leader_id: i32,
    pub leader_epoch: i32,
    pub replica_nodes: Vec<i32>,
    pub isr_nodes: Vec<i32>,
    pub offline_replicas: Vec<i32>,
    pub error_code: KafkaErrorCode,
    pub error_message: Option<String>,
}

impl KafkaResponseEncoder for DescribeTopicsResponse {
    fn encode(&self, writer: &mut KafkaWriter<'_>, _version: i16) -> Result<()> {
        // Always flexible
        writer.write_i32(self.throttle_time_ms);
        writer.write_compact_array(&self.topics, |w, topic| {
            w.write_compact_string(&topic.name);
            w.write_bytes(&topic.topic_id);
            w.write_bool(topic.is_internal);
            w.write_compact_array(&topic.partitions, |w2, p| {
                w2.write_i32(p.partition_index);
                w2.write_i32(p.leader_id);
                w2.write_i32(p.leader_epoch);
                w2.write_compact_array(&p.replica_nodes, |w3, &n| w3.write_i32(n));
                w2.write_compact_array(&p.isr_nodes, |w3, &n| w3.write_i32(n));
                w2.write_compact_array(&p.offline_replicas, |w3, &n| w3.write_i32(n));
                w2.write_i16(p.error_code as i16);
                w2.write_compact_nullable_string(p.error_message.as_deref());
                w2.write_tagged_fields(&[]);
            });
            w.write_i32(topic.topic_authorized_operations);
            w.write_i16(topic.error_code as i16);
            w.write_compact_nullable_string(topic.error_message.as_deref());
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
    fn test_describe_topics_response_encode() {
        let resp = DescribeTopicsResponse {
            throttle_time_ms: 0,
            topics: vec![DescribeTopicsResponseTopic {
                name: "test-topic".to_string(),
                topic_id: [0u8; 16],
                is_internal: false,
                partitions: vec![DescribeTopicsResponsePartition {
                    partition_index: 0,
                    leader_id: 1,
                    leader_epoch: 0,
                    replica_nodes: vec![1],
                    isr_nodes: vec![1],
                    offline_replicas: vec![],
                    error_code: KafkaErrorCode::None,
                    error_message: None,
                }],
                topic_authorized_operations: 0,
                error_code: KafkaErrorCode::None,
                error_message: None,
            }],
        };
        let mut buf = BytesMut::new();
        let mut writer = KafkaWriter::new(&mut buf);
        resp.encode(&mut writer, 0).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_describe_topics_request_decode() {
        // Build a simple request buffer
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        // topics: compact nullable array with 1 element
        w.write_compact_array(
            &[DescribeTopicsRequestTopic {
                name: "t1".to_string(),
            }],
            |w, t| {
                w.write_compact_string(&t.name);
                w.write_tagged_fields(&[]);
            },
        );
        w.write_tagged_fields(&[]);

        let mut reader = KafkaReader::new(&buf);
        let req = DescribeTopicsRequest::decode(&mut reader, 0).unwrap();
        assert!(req.topics.is_some());
        let topics = req.topics.unwrap();
        assert_eq!(topics.len(), 1);
        assert_eq!(topics[0].name, "t1");
    }
}
