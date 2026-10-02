//! OffsetForLeaderEpoch API (Key = 23)
//!
//! 查询指定 leader epoch 对应的最后偏移量。
//! 用于副本同步和消费者偏移量验证。

use rk_core::error::Result;
use crate::codec::{KafkaRequestDecoder, KafkaResponseEncoder};
use crate::types::{KafkaReader, KafkaWriter};
use crate::error_codes::KafkaErrorCode;

// ─── Request ──────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct OffsetForLeaderEpochRequest {
    /// v0+: replica_id (i32)
    pub replica_id: i32,
    pub topics: Vec<OffsetForLeaderEpochRequestTopic>,
}

#[derive(Debug, Clone)]
pub struct OffsetForLeaderEpochRequestTopic {
    pub topic: String,
    pub partitions: Vec<OffsetForLeaderEpochRequestPartition>,
}

#[derive(Debug, Clone)]
pub struct OffsetForLeaderEpochRequestPartition {
    /// v3+: current_leader_epoch (i32)
    pub current_leader_epoch: i32,
    /// v0+: leader_epoch (i32)
    pub leader_epoch: i32,
    pub partition: i32,
}

impl KafkaRequestDecoder for OffsetForLeaderEpochRequest {
    fn decode(reader: &mut KafkaReader<'_>, version: i16) -> Result<Self> {
        if version >= 3 {
            // Flexible
            let replica_id = reader.read_i32()?;
            let topics = reader.read_compact_array(|r| {
                let topic = r.read_compact_string()?;
                let partitions = r.read_compact_array(|r| {
                    let current_leader_epoch = r.read_i32()?;
                    let leader_epoch = r.read_i32()?;
                    let partition = r.read_i32()?;
                    let _tags = r.read_tagged_fields()?;
                    Ok(OffsetForLeaderEpochRequestPartition {
                        current_leader_epoch,
                        leader_epoch,
                        partition,
                    })
                })?;
                let _tags = r.read_tagged_fields()?;
                Ok(OffsetForLeaderEpochRequestTopic { topic, partitions })
            })?;
            let _tags = reader.read_tagged_fields()?;
            Ok(Self { replica_id, topics })
        } else {
            // Legacy v0-v2
            let replica_id = reader.read_i32()?;
            let topics = reader.read_array(|r| {
                let topic = r.read_string()?;
                let partitions = r.read_array(|r| {
                    let current_leader_epoch = if version >= 3 { r.read_i32()? } else { -1 };
                    let leader_epoch = r.read_i32()?;
                    let partition = r.read_i32()?;
                    Ok(OffsetForLeaderEpochRequestPartition {
                        current_leader_epoch,
                        leader_epoch,
                        partition,
                    })
                })?;
                Ok(OffsetForLeaderEpochRequestTopic { topic, partitions })
            })?;
            Ok(Self { replica_id, topics })
        }
    }
}

// ─── Response ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct OffsetForLeaderEpochResponse {
    /// v0+: throttle_time_ms (i32) — only v3+
    pub throttle_time_ms: i32,
    pub topics: Vec<OffsetForLeaderEpochResponseTopic>,
}

#[derive(Debug, Clone)]
pub struct OffsetForLeaderEpochResponseTopic {
    pub topic: String,
    pub partitions: Vec<OffsetForLeaderEpochResponsePartition>,
}

#[derive(Debug, Clone)]
pub struct OffsetForLeaderEpochResponsePartition {
    pub error_code: KafkaErrorCode,
    /// v4+: leader_epoch (i32)
    pub leader_epoch: i32,
    pub end_offset: i64,
}

impl KafkaResponseEncoder for OffsetForLeaderEpochResponse {
    fn encode(&self, writer: &mut KafkaWriter<'_>, version: i16) -> Result<()> {
        if version >= 3 {
            writer.write_i32(self.throttle_time_ms);
        }
        if version >= 3 {
            writer.write_compact_array(&self.topics, |w, t| {
                w.write_compact_string(&t.topic);
                w.write_compact_array(&t.partitions, |w, p| {
                    w.write_i16(p.error_code.as_i16());
                    if version >= 4 {
                        w.write_i32(p.leader_epoch);
                    }
                    w.write_i64(p.end_offset);
                    w.write_tagged_fields(&[]);
                });
                w.write_tagged_fields(&[]);
            });
            writer.write_tagged_fields(&[]);
        } else {
            writer.write_array(&self.topics, |w, t| {
                w.write_string(&t.topic);
                w.write_array(&t.partitions, |w, p| {
                    w.write_i16(p.error_code.as_i16());
                    if version >= 4 {
                        w.write_i32(p.leader_epoch);
                    }
                    w.write_i64(p.end_offset);
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
    fn test_offset_for_leader_epoch_response_encode_v0() {
        let resp = OffsetForLeaderEpochResponse {
            throttle_time_ms: 0,
            topics: vec![OffsetForLeaderEpochResponseTopic {
                topic: "test".to_string(),
                partitions: vec![OffsetForLeaderEpochResponsePartition {
                    error_code: KafkaErrorCode::None,
                    leader_epoch: 0,
                    end_offset: 100,
                }],
            }],
        };
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        resp.encode(&mut w, 0).unwrap();
        assert!(!buf.is_empty());
    }
}
