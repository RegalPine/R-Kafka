//! DeleteTopics API (Key = 20)
//!
//! 删除 Topic: 按名称删除。
//! Phase 1: 仅支持按 topic_name 删除 (v6+ 支持 topic_id)。

use crate::codec::{KafkaRequestDecoder, KafkaResponseEncoder};
use crate::error_codes::KafkaErrorCode;
use crate::types::{KafkaReader, KafkaWriter};
use rk_core::error::Result;

// ─── Request ──────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct DeleteTopicsRequest {
    /// v0+: topic_names (array of string)
    pub topic_names: Vec<String>,
    /// v0+: timeout_ms (i32)
    pub timeout_ms: i32,
}

impl KafkaRequestDecoder for DeleteTopicsRequest {
    fn decode(reader: &mut KafkaReader<'_>, version: i16) -> Result<Self> {
        let topic_names = if version >= 4 {
            // v4+: compact_array of compact_string
            reader.read_compact_array(|r| r.read_compact_string())?
        } else {
            reader.read_array(|r| r.read_string())?
        };

        let timeout_ms = reader.read_i32()?;

        if version >= 4 {
            let _tags = reader.read_tagged_fields()?;
        }

        Ok(Self {
            topic_names,
            timeout_ms,
        })
    }
}

// ─── Response ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct DeleteTopicsResponse {
    /// v1+: throttle_time_ms (i32)
    pub throttle_time_ms: i32,
    pub topics: Vec<DeleteTopicsResponseTopic>,
}

#[derive(Debug, Clone)]
pub struct DeleteTopicsResponseTopic {
    pub name: String,
    pub error_code: KafkaErrorCode,
    /// v1+: error_message (nullable_string)
    pub error_message: Option<String>,
}

impl DeleteTopicsResponse {
    pub fn success(topics: Vec<DeleteTopicsResponseTopic>) -> Self {
        Self {
            throttle_time_ms: 0,
            topics,
        }
    }
}

impl KafkaResponseEncoder for DeleteTopicsResponse {
    fn encode(&self, writer: &mut KafkaWriter<'_>, version: i16) -> Result<()> {
        if version >= 1 {
            writer.write_i32(self.throttle_time_ms);
        }

        if version >= 4 {
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
    fn test_delete_topics_response_encode_v0() {
        let resp = DeleteTopicsResponse::success(vec![DeleteTopicsResponseTopic {
            name: "test".to_string(),
            error_code: KafkaErrorCode::None,
            error_message: None,
        }]);
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        resp.encode(&mut w, 0).unwrap();
        assert!(buf.len() > 4);
    }

    #[test]
    fn test_delete_topics_response_encode_v1() {
        let resp = DeleteTopicsResponse::success(vec![DeleteTopicsResponseTopic {
            name: "test".to_string(),
            error_code: KafkaErrorCode::UnknownTopicOrPartition,
            error_message: Some("not found".to_string()),
        }]);
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        resp.encode(&mut w, 1).unwrap();
        assert!(buf.len() > 10);
    }
}
