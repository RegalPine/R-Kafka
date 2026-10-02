//! ElectLeaders API (Key = 43)
//!
//! 触发 partition leader 选举。
//! v0: KIP-460, v1: KIP-700 (preferred only + timeout)

use rk_core::error::Result;
use crate::codec::{KafkaRequestDecoder, KafkaResponseEncoder};
use crate::types::{KafkaReader, KafkaWriter};
use crate::error_codes::KafkaErrorCode;

// ─── Request ──────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ElectLeadersRequest {
    /// 0 = Preferred, 1 = Unclean
    pub election_type: i32,
    /// null = 所有 partition
    pub topic_partitions: Option<Vec<ElectLeadersRequestTopic>>,
    pub timeout_ms: i32,
}

#[derive(Debug, Clone)]
pub struct ElectLeadersRequestTopic {
    pub topic: String,
    pub partitions: Vec<i32>,
}

impl KafkaRequestDecoder for ElectLeadersRequest {
    fn decode(reader: &mut KafkaReader<'_>, version: i16) -> Result<Self> {
        if version >= 2 {
            // Flexible v2+
            let election_type = reader.read_i32()?;
            let topic_partitions = reader.read_compact_nullable_array(|r| {
                let topic = r.read_compact_string()?;
                let partitions = r.read_compact_array(|r2| r2.read_i32())?;
                let _tags = r.read_tagged_fields()?;
                Ok(ElectLeadersRequestTopic { topic, partitions })
            })?;
            let timeout_ms = reader.read_i32()?;
            let _tags = reader.read_tagged_fields()?;
            Ok(Self { election_type, topic_partitions, timeout_ms })
        } else {
            // Legacy v0-v1
            let election_type = reader.read_i32()?;
            let topic_partitions = reader.read_nullable_array(|r| {
                let topic = r.read_string()?;
                let partitions = r.read_array(|r2| r2.read_i32())?;
                Ok(ElectLeadersRequestTopic { topic, partitions })
            })?;
            let timeout_ms = reader.read_i32()?;
            Ok(Self { election_type, topic_partitions, timeout_ms })
        }
    }
}

impl KafkaResponseEncoder for ElectLeadersRequest {
    fn encode(&self, _writer: &mut KafkaWriter<'_>, _version: i16) -> Result<()> {
        Ok(()) // Request only
    }
}

// ─── Response ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ElectLeadersResponse {
    /// v1+: throttle_time_ms (i32)
    pub throttle_time_ms: i32,
    /// v0: 每个 partition 的结果
    pub results: Vec<ElectLeadersResponseTopic>,
    /// v1+: error_code (top-level)
    pub error_code: KafkaErrorCode,
}

#[derive(Debug, Clone)]
pub struct ElectLeadersResponseTopic {
    pub topic: String,
    pub partitions: Vec<ElectLeadersResponsePartition>,
}

#[derive(Debug, Clone)]
pub struct ElectLeadersResponsePartition {
    pub partition: i32,
    pub error_code: KafkaErrorCode,
}

impl KafkaResponseEncoder for ElectLeadersResponse {
    fn encode(&self, writer: &mut KafkaWriter<'_>, version: i16) -> Result<()> {
        if version >= 2 {
            // Flexible v2+
            writer.write_i32(self.throttle_time_ms);
            writer.write_compact_array(&self.results, |w, topic| {
                w.write_compact_string(&topic.topic);
                w.write_compact_array(&topic.partitions, |w2, part| {
                    w2.write_i32(part.partition);
                    w2.write_i16(part.error_code as i16);
                    w2.write_tagged_fields(&[]);
                });
                w.write_tagged_fields(&[]);
            });
            writer.write_i16(self.error_code as i16);
            writer.write_tagged_fields(&[]);
        } else {
            // Legacy v0-v1
            writer.write_i32(self.throttle_time_ms);
            writer.write_array(&self.results, |w, topic| {
                w.write_string(&topic.topic);
                w.write_array(&topic.partitions, |w2, part| {
                    w2.write_i32(part.partition);
                    w2.write_i16(part.error_code as i16);
                });
            });
            if version >= 1 {
                writer.write_i16(self.error_code as i16);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_elect_leaders_encode_v0() {
        let resp = ElectLeadersResponse {
            throttle_time_ms: 0,
            results: vec![ElectLeadersResponseTopic {
                topic: "test".to_string(),
                partitions: vec![ElectLeadersResponsePartition {
                    partition: 0,
                    error_code: KafkaErrorCode::None,
                }],
            }],
            error_code: KafkaErrorCode::None,
        };
        let mut buf = BytesMut::new();
        let mut writer = KafkaWriter::new(&mut buf);
        resp.encode(&mut writer, 0).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_elect_leaders_encode_v2() {
        let resp = ElectLeadersResponse {
            throttle_time_ms: 50,
            results: vec![],
            error_code: KafkaErrorCode::None,
        };
        let mut buf = BytesMut::new();
        let mut writer = KafkaWriter::new(&mut buf);
        resp.encode(&mut writer, 2).unwrap();
        assert!(!buf.is_empty());
    }
}
