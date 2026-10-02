//! CreatePartitions API (Key = 37)
//!
//! 为已有 Topic 增加 Partition 数量。

use crate::codec::{KafkaRequestDecoder, KafkaResponseEncoder};
use crate::error_codes::KafkaErrorCode;
use crate::types::{KafkaReader, KafkaWriter};
use rk_core::error::Result;

// ─── Request ──────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct CreatePartitionsRequest {
    pub topics: Vec<CreatePartitionsRequestTopic>,
    pub timeout_ms: i32,
    /// v1+: validate_only (bool)
    pub validate_only: bool,
}

#[derive(Debug, Clone)]
pub struct CreatePartitionsRequestTopic {
    pub name: String,
    pub new_partitions_count: i32,
    /// nullable array: 新分区到 Broker 的分配方案
    pub assignments: Option<Vec<Vec<i32>>>,
}

impl KafkaRequestDecoder for CreatePartitionsRequest {
    fn decode(reader: &mut KafkaReader<'_>, version: i16) -> Result<Self> {
        if version >= 2 {
            // Flexible
            let topics = reader.read_compact_array(|r| {
                let name = r.read_compact_string()?;
                let new_partitions_count = r.read_i32()?;
                let assignments =
                    r.read_compact_nullable_array(|r| r.read_compact_array(|r| r.read_i32()))?;
                let _tags = r.read_tagged_fields()?;
                Ok(CreatePartitionsRequestTopic {
                    name,
                    new_partitions_count,
                    assignments,
                })
            })?;
            let timeout_ms = reader.read_i32()?;
            let validate_only = reader.read_bool()?;
            let _tags = reader.read_tagged_fields()?;
            Ok(Self {
                topics,
                timeout_ms,
                validate_only,
            })
        } else {
            // Legacy v0-v1
            let topics = reader.read_array(|r| {
                let name = r.read_string()?;
                let new_partitions_count = r.read_i32()?;
                let assignments = r.read_nullable_array(|r| r.read_array(|r| r.read_i32()))?;
                Ok(CreatePartitionsRequestTopic {
                    name,
                    new_partitions_count,
                    assignments,
                })
            })?;
            let timeout_ms = reader.read_i32()?;
            let validate_only = if version >= 1 {
                reader.read_bool()?
            } else {
                false
            };
            Ok(Self {
                topics,
                timeout_ms,
                validate_only,
            })
        }
    }
}

// ─── Response ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct CreatePartitionsResponse {
    pub throttle_time_ms: i32,
    pub results: Vec<CreatePartitionsResponseResult>,
}

#[derive(Debug, Clone)]
pub struct CreatePartitionsResponseResult {
    pub name: String,
    pub error_code: KafkaErrorCode,
    pub error_message: Option<String>,
}

impl KafkaResponseEncoder for CreatePartitionsResponse {
    fn encode(&self, writer: &mut KafkaWriter<'_>, version: i16) -> Result<()> {
        writer.write_i32(self.throttle_time_ms);
        if version >= 2 {
            writer.write_compact_array(&self.results, |w, r| {
                w.write_compact_string(&r.name);
                w.write_i16(r.error_code.as_i16());
                w.write_compact_nullable_string(r.error_message.as_deref());
                w.write_tagged_fields(&[]);
            });
            writer.write_tagged_fields(&[]);
        } else {
            writer.write_array(&self.results, |w, r| {
                w.write_string(&r.name);
                w.write_i16(r.error_code.as_i16());
                w.write_nullable_string(r.error_message.as_deref());
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
    fn test_create_partitions_response_encode_v0() {
        let resp = CreatePartitionsResponse {
            throttle_time_ms: 0,
            results: vec![CreatePartitionsResponseResult {
                name: "test".to_string(),
                error_code: KafkaErrorCode::None,
                error_message: None,
            }],
        };
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        resp.encode(&mut w, 0).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_create_partitions_request_decode_v0() {
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        // topics array: 1 topic
        w.write_i32(1);
        w.write_string("my-topic");
        w.write_i32(6); // new_partitions_count
        w.write_i32(-1); // assignments: null
        w.write_i32(30000); // timeout_ms
                            // v0: no validate_only
        let mut r = KafkaReader::new(&buf);
        let req = CreatePartitionsRequest::decode(&mut r, 0).unwrap();
        assert_eq!(req.topics.len(), 1);
        assert_eq!(req.topics[0].name, "my-topic");
        assert_eq!(req.topics[0].new_partitions_count, 6);
        assert_eq!(req.timeout_ms, 30000);
        assert!(!req.validate_only);
    }
}
