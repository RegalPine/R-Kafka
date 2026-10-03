//! TxnOffsetCommit API (Key = 27)
//!
//! 事务性消费者偏移量提交。将消费者组偏移量作为事务的一部分提交，
//! 与 AddOffsetsToTxn (25) 配合使用，实现 consume-transform-produce 模式。
//! v0-v2: 传统格式
//! v3+: Flexible 格式

use crate::codec::{KafkaRequestDecoder, KafkaResponseEncoder};
use crate::error_codes::KafkaErrorCode;
use crate::types::{KafkaReader, KafkaWriter};
use rk_core::error::Result;

// ─── Request ──────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct TxnOffsetCommitRequest {
    /// 事务 ID
    pub transactional_id: String,
    /// 消费者组 ID
    pub group_id: String,
    /// 生产者 ID
    pub producer_id: i64,
    /// 生产者 Epoch
    pub producer_epoch: i16,
    /// v3+: generation_id (i32)
    pub generation_id: i32,
    /// v3+: member_id (string)
    pub member_id: String,
    /// v3+: group_instance_id (nullable string)
    pub group_instance_id: Option<String>,
    /// 要提交的 topic 偏移量列表
    pub topics: Vec<TxnOffsetCommitRequestTopic>,
}

#[derive(Debug, Clone)]
pub struct TxnOffsetCommitRequestTopic {
    pub name: String,
    pub partitions: Vec<TxnOffsetCommitRequestPartition>,
}

#[derive(Debug, Clone)]
pub struct TxnOffsetCommitRequestPartition {
    pub partition_index: i32,
    pub committed_offset: i64,
    /// v2+: committed_leader_epoch (i32)
    pub committed_leader_epoch: i32,
    /// nullable metadata string
    pub committed_metadata: Option<String>,
}

impl KafkaRequestDecoder for TxnOffsetCommitRequest {
    fn decode(reader: &mut KafkaReader<'_>, version: i16) -> Result<Self> {
        if version >= 3 {
            // Flexible v3+
            let transactional_id = reader.read_compact_string()?;
            let group_id = reader.read_compact_string()?;
            let producer_id = reader.read_i64()?;
            let producer_epoch = reader.read_i16()?;
            let generation_id = reader.read_i32()?;
            let member_id = reader.read_compact_string()?;
            let group_instance_id = reader.read_compact_nullable_string()?;
            let topics = reader.read_compact_array(|r| {
                let name = r.read_compact_string()?;
                let partitions = r.read_compact_array(|r2| {
                    let partition_index = r2.read_i32()?;
                    let committed_offset = r2.read_i64()?;
                    let committed_leader_epoch = r2.read_i32()?;
                    let committed_metadata = r2.read_compact_nullable_string()?;
                    let _tags = r2.read_tagged_fields()?;
                    Ok(TxnOffsetCommitRequestPartition {
                        partition_index,
                        committed_offset,
                        committed_leader_epoch,
                        committed_metadata,
                    })
                })?;
                let _tags = r.read_tagged_fields()?;
                Ok(TxnOffsetCommitRequestTopic { name, partitions })
            })?;
            let _tags = reader.read_tagged_fields()?;
            Ok(Self {
                transactional_id,
                group_id,
                producer_id,
                producer_epoch,
                generation_id,
                member_id,
                group_instance_id,
                topics,
            })
        } else {
            // Legacy v0-v2
            let transactional_id = reader.read_string()?;
            let group_id = reader.read_string()?;
            let producer_id = reader.read_i64()?;
            let producer_epoch = reader.read_i16()?;
            let topics = reader.read_array(|r| {
                let name = r.read_string()?;
                let partitions = r.read_array(|r2| {
                    let partition_index = r2.read_i32()?;
                    let committed_offset = r2.read_i64()?;
                    let committed_leader_epoch = if version >= 2 { r2.read_i32()? } else { -1 };
                    let committed_metadata = r2.read_nullable_string()?;
                    Ok(TxnOffsetCommitRequestPartition {
                        partition_index,
                        committed_offset,
                        committed_leader_epoch,
                        committed_metadata,
                    })
                })?;
                Ok(TxnOffsetCommitRequestTopic { name, partitions })
            })?;
            Ok(Self {
                transactional_id,
                group_id,
                producer_id,
                producer_epoch,
                generation_id: -1,
                member_id: String::new(),
                group_instance_id: None,
                topics,
            })
        }
    }
}

// ─── Response ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct TxnOffsetCommitResponse {
    pub throttle_time_ms: i32,
    pub topics: Vec<TxnOffsetCommitResponseTopic>,
}

#[derive(Debug, Clone)]
pub struct TxnOffsetCommitResponseTopic {
    pub name: String,
    pub partitions: Vec<TxnOffsetCommitResponsePartition>,
}

#[derive(Debug, Clone)]
pub struct TxnOffsetCommitResponsePartition {
    pub partition_index: i32,
    pub error_code: KafkaErrorCode,
}

impl KafkaResponseEncoder for TxnOffsetCommitResponse {
    fn encode(&self, writer: &mut KafkaWriter<'_>, version: i16) -> Result<()> {
        if version >= 3 {
            // Flexible v3+
            writer.write_i32(self.throttle_time_ms);
            writer.write_compact_array(&self.topics, |w, topic| {
                w.write_compact_string(&topic.name);
                w.write_compact_array(&topic.partitions, |w2, part| {
                    w2.write_i32(part.partition_index);
                    w2.write_i16(part.error_code.as_i16());
                    w2.write_tagged_fields(&[]);
                });
                w.write_tagged_fields(&[]);
            });
            writer.write_tagged_fields(&[]);
        } else {
            // Legacy v0-v2
            writer.write_i32(self.throttle_time_ms);
            writer.write_array(&self.topics, |w, topic| {
                w.write_string(&topic.name);
                w.write_array(&topic.partitions, |w2, part| {
                    w2.write_i32(part.partition_index);
                    w2.write_i16(part.error_code.as_i16());
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
    fn test_txn_offset_commit_response_encode_v0() {
        let resp = TxnOffsetCommitResponse {
            throttle_time_ms: 0,
            topics: vec![TxnOffsetCommitResponseTopic {
                name: "test".to_string(),
                partitions: vec![TxnOffsetCommitResponsePartition {
                    partition_index: 0,
                    error_code: KafkaErrorCode::None,
                }],
            }],
        };
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        resp.encode(&mut w, 0).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_txn_offset_commit_response_encode_v3_flexible() {
        let resp = TxnOffsetCommitResponse {
            throttle_time_ms: 0,
            topics: vec![TxnOffsetCommitResponseTopic {
                name: "test".to_string(),
                partitions: vec![TxnOffsetCommitResponsePartition {
                    partition_index: 0,
                    error_code: KafkaErrorCode::None,
                }],
            }],
        };
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        resp.encode(&mut w, 3).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_txn_offset_commit_request_decode_v0() {
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        w.write_string("txn-1");
        w.write_string("my-group");
        w.write_i64(1000);
        w.write_i16(0);
        // topics array: 1 topic, 1 partition
        w.write_i32(1); // array length
        w.write_string("test-topic");
        w.write_i32(1); // partitions array length
        w.write_i32(0); // partition_index
        w.write_i64(42); // committed_offset
        w.write_nullable_string(None); // metadata

        let mut reader = KafkaReader::new(&buf);
        let req = TxnOffsetCommitRequest::decode(&mut reader, 0).unwrap();
        assert_eq!(req.transactional_id, "txn-1");
        assert_eq!(req.group_id, "my-group");
        assert_eq!(req.producer_id, 1000);
        assert_eq!(req.topics.len(), 1);
        assert_eq!(req.topics[0].partitions[0].committed_offset, 42);
    }
}
