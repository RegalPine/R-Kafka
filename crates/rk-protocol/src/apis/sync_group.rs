//! SyncGroup API (Key = 14)
//!
//! 消费者同步分区分配结果。
//! Phase 1: 简化实现，Leader 分配后广播。

use crate::codec::{KafkaRequestDecoder, KafkaResponseEncoder};
use crate::error_codes::KafkaErrorCode;
use crate::types::{KafkaReader, KafkaWriter};
use rk_core::error::Result;

// ─── Request ──────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct SyncGroupRequest {
    pub group_id: String,
    pub generation_id: i32,
    pub member_id: String,
    /// v3+: group_instance_id (nullable_string)
    pub group_instance_id: Option<String>,
    /// v5+: protocol_type (nullable_string)
    pub protocol_type: Option<String>,
    /// v5+: protocol_name (nullable_string)
    pub protocol_name: Option<String>,
    /// Leader 提交的分区分配 (仅 leader 发送时非空)
    pub assignments: Vec<SyncGroupRequestAssignment>,
}

#[derive(Debug, Clone)]
pub struct SyncGroupRequestAssignment {
    pub member_id: String,
    pub assignment: Vec<u8>,
}

impl KafkaRequestDecoder for SyncGroupRequest {
    fn decode(reader: &mut KafkaReader<'_>, version: i16) -> Result<Self> {
        if version >= 6 {
            // Flexible
            let group_id = reader.read_compact_string()?;
            let generation_id = reader.read_i32()?;
            let member_id = reader.read_compact_string()?;
            let group_instance_id = reader.read_compact_nullable_string()?;
            let protocol_type = reader.read_compact_nullable_string()?;
            let protocol_name = reader.read_compact_nullable_string()?;
            let assignments = reader.read_compact_array(|r| {
                let member_id = r.read_compact_string()?;
                let assignment = r.read_compact_bytes()?;
                let _tags = r.read_tagged_fields()?;
                Ok(SyncGroupRequestAssignment {
                    member_id,
                    assignment,
                })
            })?;
            let _tags = reader.read_tagged_fields()?;
            Ok(Self {
                group_id,
                generation_id,
                member_id,
                group_instance_id,
                protocol_type,
                protocol_name,
                assignments,
            })
        } else {
            // Legacy v0-v5
            let group_id = reader.read_string()?;
            let generation_id = reader.read_i32()?;
            let member_id = reader.read_string()?;
            let group_instance_id = if version >= 3 {
                reader.read_nullable_string()?
            } else {
                None
            };
            let protocol_type = if version >= 5 {
                reader.read_nullable_string()?
            } else {
                None
            };
            let protocol_name = if version >= 5 {
                reader.read_nullable_string()?
            } else {
                None
            };
            let assignments = reader.read_array(|r| {
                let member_id = r.read_string()?;
                let assignment = r.read_bytes()?;
                Ok(SyncGroupRequestAssignment {
                    member_id,
                    assignment,
                })
            })?;
            Ok(Self {
                group_id,
                generation_id,
                member_id,
                group_instance_id,
                protocol_type,
                protocol_name,
                assignments,
            })
        }
    }
}

// ─── Response ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct SyncGroupResponse {
    /// v1+: throttle_time_ms (i32)
    pub throttle_time_ms: i32,
    pub error_code: KafkaErrorCode,
    /// v5+: protocol_type (nullable_string)
    pub protocol_type: Option<String>,
    /// v5+: protocol_name (nullable_string)
    pub protocol_name: Option<String>,
    /// 该 member 的分区分配
    pub assignment: Vec<u8>,
}

impl KafkaResponseEncoder for SyncGroupResponse {
    fn encode(&self, writer: &mut KafkaWriter<'_>, version: i16) -> Result<()> {
        if version >= 1 {
            writer.write_i32(self.throttle_time_ms);
        }

        if version >= 6 {
            // Flexible
            writer.write_i16(self.error_code.as_i16());
            writer.write_compact_nullable_string(self.protocol_type.as_deref());
            writer.write_compact_nullable_string(self.protocol_name.as_deref());
            writer.write_compact_bytes(&self.assignment);
            writer.write_tagged_fields(&[]);
        } else {
            writer.write_i16(self.error_code.as_i16());
            if version >= 5 {
                writer.write_nullable_string(self.protocol_type.as_deref());
                writer.write_nullable_string(self.protocol_name.as_deref());
            }
            writer.write_bytes(&self.assignment);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_sync_group_response_v0() {
        let resp = SyncGroupResponse {
            throttle_time_ms: 0,
            error_code: KafkaErrorCode::None,
            protocol_type: None,
            protocol_name: None,
            assignment: vec![10, 20, 30],
        };
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        resp.encode(&mut w, 0).unwrap();
        assert!(buf.len() > 5);
    }

    #[test]
    fn test_sync_group_response_v6_flexible() {
        let resp = SyncGroupResponse {
            throttle_time_ms: 0,
            error_code: KafkaErrorCode::None,
            protocol_type: Some("consumer".to_string()),
            protocol_name: Some("range".to_string()),
            assignment: vec![10, 20, 30],
        };
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        resp.encode(&mut w, 6).unwrap();
        assert!(buf.len() > 5);
    }
}
