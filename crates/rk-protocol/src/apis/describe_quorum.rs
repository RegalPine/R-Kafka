//! DescribeQuorum API (Key = 56)
//!
//! 查询 Quorum 元数据信息 (KRaft 模式)。
//! KIP-595, v0-v1 为 Flexible 格式。
//! Phase 1: 单 Broker，返回当前 Broker 作为唯一 leader 的信息。

use rk_core::error::Result;
use crate::codec::{KafkaRequestDecoder, KafkaResponseEncoder};
use crate::types::{KafkaReader, KafkaWriter};
use crate::error_codes::KafkaErrorCode;

// ─── Request ──────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct DescribeQuorumRequest {
    pub topics: Vec<DescribeQuorumRequestTopic>,
}

#[derive(Debug, Clone)]
pub struct DescribeQuorumRequestTopic {
    pub topic_name: String,
    pub partitions: Vec<DescribeQuorumRequestPartition>,
}

#[derive(Debug, Clone)]
pub struct DescribeQuorumRequestPartition {
    pub partition_index: i32,
}

impl KafkaRequestDecoder for DescribeQuorumRequest {
    fn decode(reader: &mut KafkaReader<'_>, _version: i16) -> Result<Self> {
        // v0+ is always flexible
        let topics = reader.read_compact_array(|r| {
            let topic_name = r.read_compact_string()?;
            let partitions = r.read_compact_array(|r2| {
                let partition_index = r2.read_i32()?;
                let _tags = r2.read_tagged_fields();
                Ok(DescribeQuorumRequestPartition { partition_index })
            })?;
            let _tags = r.read_tagged_fields();
            Ok(DescribeQuorumRequestTopic { topic_name, partitions })
        })?;
        Ok(Self { topics })
    }
}

impl KafkaResponseEncoder for DescribeQuorumRequest {
    fn encode(&self, _writer: &mut KafkaWriter<'_>, _version: i16) -> Result<()> {
        Ok(()) // Request only
    }
}

// ─── Response ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct DescribeQuorumResponse {
    pub throttle_time_ms: i32,
    pub error_code: KafkaErrorCode,
    pub error_message: Option<String>,
    pub topics: Vec<DescribeQuorumResponseTopic>,
}

#[derive(Debug, Clone)]
pub struct DescribeQuorumResponseTopic {
    pub topic_name: String,
    pub partitions: Vec<DescribeQuorumResponsePartition>,
}

#[derive(Debug, Clone)]
pub struct DescribeQuorumResponsePartition {
    pub partition_index: i32,
    pub error_code: KafkaErrorCode,
    pub error_message: Option<String>,
    pub leader_id: i32,
    pub leader_epoch: i32,
    pub high_watermark: i64,
    pub current_voters: Vec<DescribeQuorumResponseVoter>,
    pub observers: Vec<DescribeQuorumResponseVoter>,
}

#[derive(Debug, Clone)]
pub struct DescribeQuorumResponseVoter {
    pub voter_id: i32,
    pub log_end_offset: i64,
}

impl KafkaResponseEncoder for DescribeQuorumResponse {
    fn encode(&self, _writer: &mut KafkaWriter<'_>, _version: i16) -> Result<()> {
        // Always flexible
        _writer.write_i32(self.throttle_time_ms);
        _writer.write_i16(self.error_code as i16);
        _writer.write_compact_nullable_string(self.error_message.as_deref());
        _writer.write_compact_array(&self.topics, |w, topic| {
            w.write_compact_string(&topic.topic_name);
            w.write_compact_array(&topic.partitions, |w2, part| {
                w2.write_i32(part.partition_index);
                w2.write_i16(part.error_code as i16);
                w2.write_compact_nullable_string(part.error_message.as_deref());
                w2.write_i32(part.leader_id);
                w2.write_i32(part.leader_epoch);
                w2.write_i64(part.high_watermark);
                w2.write_compact_array(&part.current_voters, |w3, voter| {
                    w3.write_i32(voter.voter_id);
                    w3.write_i64(voter.log_end_offset);
                    w3.write_tagged_fields(&[]);
                });
                w2.write_compact_array(&part.observers, |w3, obs| {
                    w3.write_i32(obs.voter_id);
                    w3.write_i64(obs.log_end_offset);
                    w3.write_tagged_fields(&[]);
                });
                w2.write_tagged_fields(&[]);
            });
            w.write_tagged_fields(&[]);
        });
        _writer.write_tagged_fields(&[]);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_describe_quorum_response_encode() {
        let resp = DescribeQuorumResponse {
            throttle_time_ms: 0,
            error_code: KafkaErrorCode::None,
            error_message: None,
            topics: vec![DescribeQuorumResponseTopic {
                topic_name: "test".to_string(),
                partitions: vec![DescribeQuorumResponsePartition {
                    partition_index: 0,
                    error_code: KafkaErrorCode::None,
                    error_message: None,
                    leader_id: 1,
                    leader_epoch: 0,
                    high_watermark: 100,
                    current_voters: vec![DescribeQuorumResponseVoter {
                        voter_id: 1,
                        log_end_offset: 100,
                    }],
                    observers: vec![],
                }],
            }],
        };
        let mut buf = BytesMut::new();
        let mut writer = KafkaWriter::new(&mut buf);
        resp.encode(&mut writer, 0).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_describe_quorum_request_decode() {
        // Manually encode a request body
        let mut buf = BytesMut::new();
        let mut writer = KafkaWriter::new(&mut buf);
        // topics: compact array with 1 topic
        writer.write_compact_array(&["test".to_string()], |w, name| {
            w.write_compact_string(name);
            // partitions: compact array with 1 partition
            w.write_compact_array(&[0i32], |w2, idx| {
                w2.write_i32(*idx);
                w2.write_tagged_fields(&[]);
            });
            w.write_tagged_fields(&[]);
        });

        let mut reader = KafkaReader::new(&buf);
        let req = DescribeQuorumRequest::decode(&mut reader, 0).unwrap();
        assert_eq!(req.topics.len(), 1);
        assert_eq!(req.topics[0].topic_name, "test");
        assert_eq!(req.topics[0].partitions.len(), 1);
        assert_eq!(req.topics[0].partitions[0].partition_index, 0);
    }
}
