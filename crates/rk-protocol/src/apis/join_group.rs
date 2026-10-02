//! JoinGroup API (Key = 11)
//!
//! 消费者加入消费者组。
//! Phase 1: 简化实现，单 Broker 协调。

use rk_core::error::Result;
use crate::codec::{KafkaRequestDecoder, KafkaResponseEncoder};
use crate::types::{KafkaReader, KafkaWriter};
use crate::error_codes::KafkaErrorCode;

// ─── Request ──────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct JoinGroupRequest {
    pub group_id: String,
    /// v1+: session_timeout_ms (i32)
    pub session_timeout_ms: i32,
    /// v4+: rebalance_timeout_ms (i32)
    pub rebalance_timeout_ms: i32,
    /// v1+: member_id (string); v5+: 可为空表示新成员
    pub member_id: String,
    /// v5+: group_instance_id (nullable_string)
    pub group_instance_id: Option<String>,
    pub protocol_type: String,
    pub protocols: Vec<JoinGroupRequestProtocol>,
    /// v9+: reason (nullable_string)
    pub reason: Option<String>,
}

#[derive(Debug, Clone)]
pub struct JoinGroupRequestProtocol {
    pub name: String,
    pub metadata: Vec<u8>,
}

impl KafkaRequestDecoder for JoinGroupRequest {
    fn decode(reader: &mut KafkaReader<'_>, version: i16) -> Result<Self> {
        if version >= 9 {
            // Flexible
            let group_id = reader.read_compact_string()?;
            let session_timeout_ms = reader.read_i32()?;
            let rebalance_timeout_ms = reader.read_i32()?;
            let member_id = reader.read_compact_string()?;
            let group_instance_id = reader.read_compact_nullable_string()?;
            let protocol_type = reader.read_compact_string()?;
            let protocols = reader.read_compact_array(|r| {
                let name = r.read_compact_string()?;
                let metadata = r.read_compact_bytes()?;
                let _tags = r.read_tagged_fields()?;
                Ok(JoinGroupRequestProtocol { name, metadata })
            })?;
            let reason = reader.read_compact_nullable_string()?;
            let _tags = reader.read_tagged_fields()?;
            Ok(Self {
                group_id,
                session_timeout_ms,
                rebalance_timeout_ms,
                member_id,
                group_instance_id,
                protocol_type,
                protocols,
                reason,
            })
        } else {
            // Legacy v0-v8
            let group_id = reader.read_string()?;
            let session_timeout_ms = if version >= 1 {
                reader.read_i32()?
            } else {
                30000
            };
            let rebalance_timeout_ms = if version >= 4 {
                reader.read_i32()?
            } else {
                session_timeout_ms
            };
            let member_id = reader.read_string()?;
            let group_instance_id = if version >= 5 {
                reader.read_nullable_string()?
            } else {
                None
            };
            let protocol_type = reader.read_string()?;
            let protocols = reader.read_array(|r| {
                let name = r.read_string()?;
                let metadata = r.read_bytes()?;
                Ok(JoinGroupRequestProtocol { name, metadata })
            })?;
            let reason = if version >= 9 {
                reader.read_nullable_string()?
            } else {
                None
            };
            Ok(Self {
                group_id,
                session_timeout_ms,
                rebalance_timeout_ms,
                member_id,
                group_instance_id,
                protocol_type,
                protocols,
                reason,
            })
        }
    }
}

// ─── Response ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct JoinGroupResponse {
    /// v2+: throttle_time_ms (i32)
    pub throttle_time_ms: i32,
    pub error_code: KafkaErrorCode,
    pub generation_id: i32,
    /// v7+: protocol_type (nullable_string)
    pub protocol_type: Option<String>,
    /// 协商后的协议名称
    pub protocol_name: Option<String>,
    pub leader: String,
    /// v9+: skip_assignment (bool)
    pub skip_assignment: bool,
    pub member_id: String,
    pub members: Vec<JoinGroupResponseMember>,
}

#[derive(Debug, Clone)]
pub struct JoinGroupResponseMember {
    pub member_id: String,
    /// v5+: group_instance_id (nullable_string)
    pub group_instance_id: Option<String>,
    pub metadata: Vec<u8>,
}

impl KafkaResponseEncoder for JoinGroupResponse {
    fn encode(&self, writer: &mut KafkaWriter<'_>, version: i16) -> Result<()> {
        if version >= 2 {
            writer.write_i32(self.throttle_time_ms);
        }

        if version >= 9 {
            // Flexible
            writer.write_i16(self.error_code.as_i16());
            writer.write_i32(self.generation_id);
            writer.write_compact_nullable_string(self.protocol_type.as_deref());
            writer.write_compact_nullable_string(self.protocol_name.as_deref());
            writer.write_compact_string(&self.leader);
            writer.write_bool(self.skip_assignment);
            writer.write_compact_string(&self.member_id);
            writer.write_compact_array(&self.members, |w, m| {
                w.write_compact_string(&m.member_id);
                w.write_compact_nullable_string(m.group_instance_id.as_deref());
                w.write_compact_bytes(&m.metadata);
                w.write_tagged_fields(&[]);
            });
            writer.write_tagged_fields(&[]);
        } else {
            writer.write_i16(self.error_code.as_i16());
            writer.write_i32(self.generation_id);
            if version >= 7 {
                writer.write_nullable_string(self.protocol_type.as_deref());
            }
            writer.write_nullable_string(self.protocol_name.as_deref());
            writer.write_string(&self.leader);
            writer.write_string(&self.member_id);
            writer.write_array(&self.members, |w, m| {
                w.write_string(&m.member_id);
                if version >= 5 {
                    w.write_nullable_string(m.group_instance_id.as_deref());
                }
                w.write_bytes(&m.metadata);
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
    fn test_join_group_response_v0() {
        let resp = JoinGroupResponse {
            throttle_time_ms: 0,
            error_code: KafkaErrorCode::None,
            generation_id: 1,
            protocol_type: None,
            protocol_name: Some("range".to_string()),
            leader: "member-1".to_string(),
            skip_assignment: false,
            member_id: "member-1".to_string(),
            members: vec![JoinGroupResponseMember {
                member_id: "member-1".to_string(),
                group_instance_id: None,
                metadata: vec![1, 2, 3],
            }],
        };
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        resp.encode(&mut w, 0).unwrap();
        assert!(buf.len() > 20);
    }

    #[test]
    fn test_join_group_response_v9_flexible() {
        let resp = JoinGroupResponse {
            throttle_time_ms: 0,
            error_code: KafkaErrorCode::None,
            generation_id: 1,
            protocol_type: Some("consumer".to_string()),
            protocol_name: Some("range".to_string()),
            leader: "member-1".to_string(),
            skip_assignment: false,
            member_id: "member-1".to_string(),
            members: vec![JoinGroupResponseMember {
                member_id: "member-1".to_string(),
                group_instance_id: None,
                metadata: vec![1, 2, 3],
            }],
        };
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        resp.encode(&mut w, 9).unwrap();
        assert!(buf.len() > 20);
    }
}
