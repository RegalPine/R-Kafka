//! OffsetCommit API (Key = 8)
//!
//! 消费者提交偏移量到 Broker。
//! Phase 1: 内存存储 + 可选持久化到 __consumer_offsets 内部 topic。

use rk_core::error::Result;
use crate::codec::{KafkaRequestDecoder, KafkaResponseEncoder};
use crate::types::{KafkaReader, KafkaWriter};
use crate::error_codes::KafkaErrorCode;

// ─── Request ──────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct OffsetCommitRequest {
    pub group_id: String,
    /// v1-7: generation_id (i32); v8+: removed
    pub generation_id: i32,
    /// v1-7: member_id (string); v8+: removed
    pub member_id: String,
    /// v7+: group_instance_id (nullable_string)
    pub group_instance_id: Option<String>,
    /// v1-4: retention_time_ms (i64); v5+: removed
    pub retention_time_ms: i64,
    pub topics: Vec<OffsetCommitRequestTopic>,
}

#[derive(Debug, Clone)]
pub struct OffsetCommitRequestTopic {
    pub name: String,
    pub partitions: Vec<OffsetCommitRequestPartition>,
}

#[derive(Debug, Clone)]
pub struct OffsetCommitRequestPartition {
    pub index: i32,
    pub committed_offset: i64,
    /// v1-7: committed_leader_epoch (i32); v8+: removed
    pub committed_leader_epoch: i32,
    /// v1+: commit_timestamp (i64); v2-v4: removed; v5+: re-added
    pub commit_timestamp: i64,
    /// nullable string
    pub metadata: Option<String>,
}

impl KafkaRequestDecoder for OffsetCommitRequest {
    fn decode(reader: &mut KafkaReader<'_>, version: i16) -> Result<Self> {
        if version >= 8 {
            // Flexible
            let group_id = reader.read_compact_string()?;
            let topics = reader.read_compact_array(|r| {
                let name = r.read_compact_string()?;
                let partitions = r.read_compact_array(|r2| {
                    let index = r2.read_i32()?;
                    let offset = r2.read_i64()?;
                    let _tags = r2.read_tagged_fields()?;
                    Ok(OffsetCommitRequestPartition {
                        index,
                        committed_offset: offset,
                        committed_leader_epoch: -1,
                        commit_timestamp: -1,
                        metadata: None,
                    })
                })?;
                let _tags = r.read_tagged_fields()?;
                Ok(OffsetCommitRequestTopic { name, partitions })
            })?;
            let _tags = reader.read_tagged_fields()?;
            Ok(Self {
                group_id,
                generation_id: -1,
                member_id: String::new(),
                group_instance_id: None,
                retention_time_ms: -1,
                topics,
            })
        } else {
            // Legacy v0-v7
            let group_id = reader.read_string()?;

            let generation_id = if version >= 1 {
                reader.read_i32()?
            } else {
                -1
            };

            let member_id = if version >= 1 {
                reader.read_string()?
            } else {
                String::new()
            };

            let group_instance_id = if version >= 7 {
                reader.read_nullable_string()?
            } else {
                None
            };

            // v1-v4: retention_time_ms
            let retention_time_ms = if version >= 1 && version <= 4 {
                reader.read_i64()?
            } else {
                -1
            };

            let topics = reader.read_array(|r| {
                let name = r.read_string()?;
                let partitions = r.read_array(|r2| {
                    let index = r2.read_i32()?;
                    let offset = r2.read_i64()?;

                    let committed_leader_epoch = if version >= 6 {
                        r2.read_i32()?
                    } else {
                        -1
                    };

                    // v1: timestamp; v2-v4: removed; v5+: re-added
                    let commit_timestamp = if version == 1 || version >= 5 {
                        r2.read_i64()?
                    } else {
                        -1
                    };

                    let metadata = if version >= 1 {
                        r2.read_nullable_string()?
                    } else {
                        None
                    };

                    Ok(OffsetCommitRequestPartition {
                        index,
                        committed_offset: offset,
                        committed_leader_epoch,
                        commit_timestamp,
                        metadata,
                    })
                })?;
                Ok(OffsetCommitRequestTopic { name, partitions })
            })?;

            Ok(Self {
                group_id,
                generation_id,
                member_id,
                group_instance_id,
                retention_time_ms,
                topics,
            })
        }
    }
}

// ─── Response ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct OffsetCommitResponse {
    /// v3+: throttle_time_ms (i32)
    pub throttle_time_ms: i32,
    pub topics: Vec<OffsetCommitResponseTopic>,
}

#[derive(Debug, Clone)]
pub struct OffsetCommitResponseTopic {
    pub name: String,
    pub partitions: Vec<OffsetCommitResponsePartition>,
}

#[derive(Debug, Clone)]
pub struct OffsetCommitResponsePartition {
    pub index: i32,
    pub error_code: KafkaErrorCode,
}

impl KafkaResponseEncoder for OffsetCommitResponse {
    fn encode(&self, writer: &mut KafkaWriter<'_>, version: i16) -> Result<()> {
        if version >= 3 {
            writer.write_i32(self.throttle_time_ms);
        }

        if version >= 8 {
            writer.write_compact_array(&self.topics, |w, t| {
                w.write_compact_string(&t.name);
                w.write_compact_array(&t.partitions, |w2, p| {
                    w2.write_i32(p.index);
                    w2.write_i16(p.error_code.as_i16());
                    w2.write_tagged_fields(&[]);
                });
                w.write_tagged_fields(&[]);
            });
            writer.write_tagged_fields(&[]);
        } else {
            writer.write_array(&self.topics, |w, t| {
                w.write_string(&t.name);
                w.write_array(&t.partitions, |w2, p| {
                    w2.write_i32(p.index);
                    w2.write_i16(p.error_code.as_i16());
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
    fn test_offset_commit_response_v0() {
        let resp = OffsetCommitResponse {
            throttle_time_ms: 0,
            topics: vec![OffsetCommitResponseTopic {
                name: "test".to_string(),
                partitions: vec![OffsetCommitResponsePartition {
                    index: 0,
                    error_code: KafkaErrorCode::None,
                }],
            }],
        };
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        resp.encode(&mut w, 0).unwrap();
        assert!(buf.len() > 5);
    }

    #[test]
    fn test_offset_commit_response_v8_flexible() {
        let resp = OffsetCommitResponse {
            throttle_time_ms: 0,
            topics: vec![OffsetCommitResponseTopic {
                name: "test".to_string(),
                partitions: vec![OffsetCommitResponsePartition {
                    index: 0,
                    error_code: KafkaErrorCode::None,
                }],
            }],
        };
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        resp.encode(&mut w, 8).unwrap();
        assert!(buf.len() > 5);
    }
}
