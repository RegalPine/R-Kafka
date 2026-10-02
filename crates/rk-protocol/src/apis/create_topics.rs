//! CreateTopics API (Key = 19)
//!
//! 创建 Topic: 指定分区数、副本因子和配置。
//! Phase 1: 单副本，仅支持 partition_count + replication_factor=1。

use rk_core::error::Result;
use crate::codec::{KafkaRequestDecoder, KafkaResponseEncoder};
use crate::types::{KafkaReader, KafkaWriter};
use crate::error_codes::KafkaErrorCode;

// ─── Request ──────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct CreateTopicsRequest {
    pub topics: Vec<CreateTopicsRequestTopic>,
    /// v1+: timeout_ms (i32)
    pub timeout_ms: i32,
    /// v2+: validate_only (bool)
    pub validate_only: bool,
}

#[derive(Debug, Clone)]
pub struct CreateTopicsRequestTopic {
    pub name: String,
    /// v0+: num_partitions (i32), -1 = auto
    pub num_partitions: i32,
    /// v0+: replication_factor (i16), -1 = auto
    pub replication_factor: i16,
    /// v0+: assignments (array of partition -> replicas)
    pub assignments: Vec<CreateTopicsRequestAssignment>,
    /// v0+: configs (array of key-value)
    pub configs: Vec<CreateTopicsRequestConfig>,
}

#[derive(Debug, Clone)]
pub struct CreateTopicsRequestAssignment {
    pub partition_id: i32,
    pub broker_ids: Vec<i32>,
}

#[derive(Debug, Clone)]
pub struct CreateTopicsRequestConfig {
    pub key: String,
    pub value: Option<String>,
}

impl KafkaRequestDecoder for CreateTopicsRequest {
    fn decode(reader: &mut KafkaReader<'_>, version: i16) -> Result<Self> {
        let topics = if version >= 2 {
            reader.read_compact_array(|r| {
                let name = r.read_compact_string()?;
                let num_partitions = r.read_i32()?;
                let replication_factor = r.read_i16()?;
                let assignments = r.read_compact_array(|r| {
                    let partition_id = r.read_i32()?;
                    let broker_ids = r.read_compact_array(|r| r.read_i32())?;
                    Ok(CreateTopicsRequestAssignment { partition_id, broker_ids })
                })?;
                let configs = r.read_compact_array(|r| {
                    let key = r.read_compact_string()?;
                    let value = r.read_compact_nullable_string()?;
                    Ok(CreateTopicsRequestConfig { key, value })
                })?;
                let _tags = r.read_tagged_fields()?;
                Ok(CreateTopicsRequestTopic {
                    name, num_partitions, replication_factor, assignments, configs,
                })
            })?
        } else {
            reader.read_array(|r| {
                let name = r.read_string()?;
                let num_partitions = r.read_i32()?;
                let replication_factor = r.read_i16()?;
                let assignments = r.read_array(|r| {
                    let partition_id = r.read_i32()?;
                    let broker_ids = r.read_array(|r| r.read_i32())?;
                    Ok(CreateTopicsRequestAssignment { partition_id, broker_ids })
                })?;
                let configs = r.read_array(|r| {
                    let key = r.read_string()?;
                    let value = r.read_nullable_string()?;
                    Ok(CreateTopicsRequestConfig { key, value })
                })?;
                Ok(CreateTopicsRequestTopic {
                    name, num_partitions, replication_factor, assignments, configs,
                })
            })?
        };

        let timeout_ms = reader.read_i32()?;

        let validate_only = if version >= 2 {
            reader.read_bool()?
        } else {
            false
        };

        if version >= 2 {
            let _tags = reader.read_tagged_fields()?;
        }

        Ok(Self { topics, timeout_ms, validate_only })
    }
}

// ─── Response ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct CreateTopicsResponse {
    /// v1+: throttle_time_ms (i32)
    pub throttle_time_ms: i32,
    pub topics: Vec<CreateTopicsResponseTopic>,
}

#[derive(Debug, Clone)]
pub struct CreateTopicsResponseTopic {
    pub name: String,
    pub error_code: KafkaErrorCode,
    /// v1+: error_message (nullable_string)
    pub error_message: Option<String>,
    // v5+: num_partitions, replication_factor, configs (TopicConfigInfo)
    // Phase 1 不实现 v5+
}

impl CreateTopicsResponse {
    pub fn success(topics: Vec<CreateTopicsResponseTopic>) -> Self {
        Self {
            throttle_time_ms: 0,
            topics,
        }
    }
}

impl KafkaResponseEncoder for CreateTopicsResponse {
    fn encode(&self, writer: &mut KafkaWriter<'_>, version: i16) -> Result<()> {
        if version >= 1 {
            writer.write_i32(self.throttle_time_ms);
        }

        if version >= 2 {
            writer.write_compact_array(&self.topics, |w, t| {
                w.write_compact_string(&t.name);
                w.write_i16(t.error_code.as_i16());
                if version >= 1 {
                    w.write_compact_nullable_string(t.error_message.as_deref());
                }
                w.write_tagged_fields(&[]);
            });
            writer.write_tagged_fields(&[]);
        } else {
            writer.write_array(&self.topics, |w, t| {
                w.write_string(&t.name);
                w.write_i16(t.error_code.as_i16());
                if version >= 1 {
                    w.write_nullable_string(t.error_message.as_deref());
                }
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
    fn test_create_topics_response_encode_v0() {
        let resp = CreateTopicsResponse::success(vec![
            CreateTopicsResponseTopic {
                name: "test".to_string(),
                error_code: KafkaErrorCode::None,
                error_message: None,
            },
        ]);
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        resp.encode(&mut w, 0).unwrap();
        assert!(buf.len() > 4);
    }

    #[test]
    fn test_create_topics_response_encode_v1() {
        let resp = CreateTopicsResponse::success(vec![
            CreateTopicsResponseTopic {
                name: "test".to_string(),
                error_code: KafkaErrorCode::TopicAlreadyExists,
                error_message: Some("already exists".to_string()),
            },
        ]);
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        resp.encode(&mut w, 1).unwrap();
        // v1: throttle_time_ms(4) + array_len(4) + name + error_code(2) + error_message
        assert!(buf.len() > 10);
    }
}
