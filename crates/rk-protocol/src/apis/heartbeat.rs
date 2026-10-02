//! Heartbeat API (Key = 12)
//!
//! 消费者发送心跳保持活跃。
//! Phase 1: 简化实现。

use rk_core::error::Result;
use crate::codec::{KafkaRequestDecoder, KafkaResponseEncoder};
use crate::types::{KafkaReader, KafkaWriter};
use crate::error_codes::KafkaErrorCode;

// ─── Request ──────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct HeartbeatRequest {
    pub group_id: String,
    pub generation_id: i32,
    pub member_id: String,
    /// v3+: group_instance_id (nullable_string)
    pub group_instance_id: Option<String>,
}

impl KafkaRequestDecoder for HeartbeatRequest {
    fn decode(reader: &mut KafkaReader<'_>, version: i16) -> Result<Self> {
        if version >= 4 {
            // Flexible
            let group_id = reader.read_compact_string()?;
            let generation_id = reader.read_i32()?;
            let member_id = reader.read_compact_string()?;
            let group_instance_id = reader.read_compact_nullable_string()?;
            let _tags = reader.read_tagged_fields()?;
            Ok(Self {
                group_id,
                generation_id,
                member_id,
                group_instance_id,
            })
        } else {
            // Legacy v0-v3
            let group_id = reader.read_string()?;
            let generation_id = reader.read_i32()?;
            let member_id = reader.read_string()?;
            let group_instance_id = if version >= 3 {
                reader.read_nullable_string()?
            } else {
                None
            };
            Ok(Self {
                group_id,
                generation_id,
                member_id,
                group_instance_id,
            })
        }
    }
}

// ─── Response ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct HeartbeatResponse {
    /// v1+: throttle_time_ms (i32)
    pub throttle_time_ms: i32,
    pub error_code: KafkaErrorCode,
}

impl KafkaResponseEncoder for HeartbeatResponse {
    fn encode(&self, writer: &mut KafkaWriter<'_>, version: i16) -> Result<()> {
        if version >= 1 {
            writer.write_i32(self.throttle_time_ms);
        }

        if version >= 4 {
            // Flexible
            writer.write_i16(self.error_code.as_i16());
            writer.write_tagged_fields(&[]);
        } else {
            writer.write_i16(self.error_code.as_i16());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_heartbeat_response_v0() {
        let resp = HeartbeatResponse {
            throttle_time_ms: 0,
            error_code: KafkaErrorCode::None,
        };
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        resp.encode(&mut w, 0).unwrap();
        assert_eq!(buf.len(), 2); // just error_code
    }

    #[test]
    fn test_heartbeat_response_v4_flexible() {
        let resp = HeartbeatResponse {
            throttle_time_ms: 0,
            error_code: KafkaErrorCode::None,
        };
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        resp.encode(&mut w, 4).unwrap();
        assert!(buf.len() >= 3); // error_code + tagged_fields
    }
}
