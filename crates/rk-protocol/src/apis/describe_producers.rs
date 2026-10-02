//! DescribeProducers API (Key = 61)
//!
//! 查询 partition 上活跃的幂等生产者信息。
//! KIP-664, v0+ 全部为 Flexible 格式。

use crate::codec::{KafkaRequestDecoder, KafkaResponseEncoder};
use crate::error_codes::KafkaErrorCode;
use crate::types::{KafkaReader, KafkaWriter};
use rk_core::error::Result;

// ─── Request ──────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct DescribeProducersRequest {
    pub topics: Vec<DescribeProducersRequestTopic>,
}

#[derive(Debug, Clone)]
pub struct DescribeProducersRequestTopic {
    pub name: String,
    pub partitions: Vec<i32>,
}

impl KafkaRequestDecoder for DescribeProducersRequest {
    fn decode(reader: &mut KafkaReader<'_>, _version: i16) -> Result<Self> {
        // v0+ is always flexible
        let topics = reader.read_compact_array(|r| {
            let name = r.read_compact_string()?;
            let partitions = r.read_compact_array(|r2| r2.read_i32())?;
            let _tags = r.read_tagged_fields();
            Ok(DescribeProducersRequestTopic { name, partitions })
        })?;
        let _tags = reader.read_tagged_fields();
        Ok(Self { topics })
    }
}

impl KafkaResponseEncoder for DescribeProducersRequest {
    fn encode(&self, _writer: &mut KafkaWriter<'_>, _version: i16) -> Result<()> {
        Ok(()) // Request only
    }
}

// ─── Response ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct DescribeProducersResponse {
    pub throttle_time_ms: i32,
    pub topics: Vec<DescribeProducersResponseTopic>,
}

#[derive(Debug, Clone)]
pub struct DescribeProducersResponseTopic {
    pub name: String,
    pub partitions: Vec<DescribeProducersResponsePartition>,
}

#[derive(Debug, Clone)]
pub struct DescribeProducersResponsePartition {
    pub index: i32,
    pub error_code: KafkaErrorCode,
    pub error_message: Option<String>,
    pub active_producers: Vec<DescribeProducersResponseProducer>,
}

#[derive(Debug, Clone)]
pub struct DescribeProducersResponseProducer {
    pub producer_id: i64,
    pub producer_epoch: i32,
    pub last_sequence: i32,
    pub last_timestamp: i64,
    pub current_txn_start_offset: i64,
}

impl KafkaResponseEncoder for DescribeProducersResponse {
    fn encode(&self, writer: &mut KafkaWriter<'_>, _version: i16) -> Result<()> {
        // Always flexible
        writer.write_i32(self.throttle_time_ms);
        writer.write_compact_array(&self.topics, |w, topic| {
            w.write_compact_string(&topic.name);
            w.write_compact_array(&topic.partitions, |w2, part| {
                w2.write_i32(part.index);
                w2.write_i16(part.error_code as i16);
                w2.write_compact_nullable_string(part.error_message.as_deref());
                w2.write_compact_array(&part.active_producers, |w3, prod| {
                    w3.write_i64(prod.producer_id);
                    w3.write_i32(prod.producer_epoch);
                    w3.write_i32(prod.last_sequence);
                    w3.write_i64(prod.last_timestamp);
                    w3.write_i64(prod.current_txn_start_offset);
                    w3.write_tagged_fields(&[]);
                });
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
    fn test_describe_producers_encode() {
        let resp = DescribeProducersResponse {
            throttle_time_ms: 0,
            topics: vec![DescribeProducersResponseTopic {
                name: "test".to_string(),
                partitions: vec![DescribeProducersResponsePartition {
                    index: 0,
                    error_code: KafkaErrorCode::None,
                    error_message: None,
                    active_producers: vec![DescribeProducersResponseProducer {
                        producer_id: 1000,
                        producer_epoch: 1,
                        last_sequence: 5,
                        last_timestamp: 1234567890,
                        current_txn_start_offset: -1,
                    }],
                }],
            }],
        };
        let mut buf = BytesMut::new();
        let mut writer = KafkaWriter::new(&mut buf);
        resp.encode(&mut writer, 0).unwrap();
        assert!(!buf.is_empty());
    }
}
