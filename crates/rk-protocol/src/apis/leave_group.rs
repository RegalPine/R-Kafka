//! LeaveGroup API (Key = 13)
//!
//! 消费者离开消费者组。
//! Phase 1: 简化实现。

use rk_core::error::Result;
use crate::codec::{KafkaRequestDecoder, KafkaResponseEncoder};
use crate::types::{KafkaReader, KafkaWriter};
use crate::error_codes::KafkaErrorCode;

// ─── Request ──────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct LeaveGroupRequest {
    pub group_id: String,
    /// v0-v2: 单个 member_id
    pub member_id: String,
    /// v3+: 批量成员
    pub members: Option<Vec<LeaveGroupRequestMember>>,
    /// v3+: reason (nullable_string)
    pub reason: Option<String>,
}

#[derive(Debug, Clone)]
pub struct LeaveGroupRequestMember {
    pub member_id: String,
    /// v3+: group_instance_id (nullable_string)
    pub group_instance_id: Option<String>,
    /// v5+: reason (nullable_string)
    pub reason: Option<String>,
}

impl KafkaRequestDecoder for LeaveGroupRequest {
    fn decode(reader: &mut KafkaReader<'_>, version: i16) -> Result<Self> {
        if version >= 5 {
            // Flexible
            let group_id = reader.read_compact_string()?;
            let members = reader.read_compact_array(|r| {
                let member_id = r.read_compact_string()?;
                let group_instance_id = r.read_compact_nullable_string()?;
                let reason = r.read_compact_nullable_string()?;
                let _tags = r.read_tagged_fields()?;
                Ok(LeaveGroupRequestMember {
                    member_id,
                    group_instance_id,
                    reason,
                })
            })?;
            let _tags = reader.read_tagged_fields()?;
            Ok(Self {
                group_id,
                member_id: String::new(),
                members: Some(members),
                reason: None,
            })
        } else if version >= 3 {
            // v3-v4: batch with legacy format
            let group_id = reader.read_string()?;
            let members = reader.read_array(|r| {
                let member_id = r.read_string()?;
                let group_instance_id = r.read_nullable_string()?;
                Ok(LeaveGroupRequestMember {
                    member_id,
                    group_instance_id,
                    reason: None,
                })
            })?;
            Ok(Self {
                group_id,
                member_id: String::new(),
                members: Some(members),
                reason: None,
            })
        } else {
            // v0-v2: single member_id
            let group_id = reader.read_string()?;
            let member_id = reader.read_string()?;
            Ok(Self {
                group_id,
                member_id,
                members: None,
                reason: None,
            })
        }
    }
}

// ─── Response ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct LeaveGroupResponse {
    /// v1+: throttle_time_ms (i32)
    pub throttle_time_ms: i32,
    pub error_code: KafkaErrorCode,
    /// v3+: members
    pub members: Vec<LeaveGroupResponseMember>,
}

#[derive(Debug, Clone)]
pub struct LeaveGroupResponseMember {
    pub member_id: String,
    /// v3+: group_instance_id (nullable_string)
    pub group_instance_id: Option<String>,
    pub error_code: KafkaErrorCode,
}

impl KafkaResponseEncoder for LeaveGroupResponse {
    fn encode(&self, writer: &mut KafkaWriter<'_>, version: i16) -> Result<()> {
        if version >= 1 {
            writer.write_i32(self.throttle_time_ms);
        }

        if version >= 5 {
            // Flexible
            writer.write_i16(self.error_code.as_i16());
            writer.write_compact_array(&self.members, |w, m| {
                w.write_compact_string(&m.member_id);
                w.write_compact_nullable_string(m.group_instance_id.as_deref());
                w.write_i16(m.error_code.as_i16());
                w.write_tagged_fields(&[]);
            });
            writer.write_tagged_fields(&[]);
        } else if version >= 3 {
            writer.write_i16(self.error_code.as_i16());
            writer.write_array(&self.members, |w, m| {
                w.write_string(&m.member_id);
                w.write_nullable_string(m.group_instance_id.as_deref());
                w.write_i16(m.error_code.as_i16());
            });
        } else {
            // v0-v2: just error_code
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
    fn test_leave_group_response_v0() {
        let resp = LeaveGroupResponse {
            throttle_time_ms: 0,
            error_code: KafkaErrorCode::None,
            members: vec![],
        };
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        resp.encode(&mut w, 0).unwrap();
        assert_eq!(buf.len(), 2); // just error_code
    }

    #[test]
    fn test_leave_group_response_v3() {
        let resp = LeaveGroupResponse {
            throttle_time_ms: 0,
            error_code: KafkaErrorCode::None,
            members: vec![LeaveGroupResponseMember {
                member_id: "member-1".to_string(),
                group_instance_id: None,
                error_code: KafkaErrorCode::None,
            }],
        };
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        resp.encode(&mut w, 3).unwrap();
        assert!(buf.len() > 5);
    }

    #[test]
    fn test_leave_group_response_v5_flexible() {
        let resp = LeaveGroupResponse {
            throttle_time_ms: 0,
            error_code: KafkaErrorCode::None,
            members: vec![LeaveGroupResponseMember {
                member_id: "member-1".to_string(),
                group_instance_id: None,
                error_code: KafkaErrorCode::None,
            }],
        };
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        resp.encode(&mut w, 5).unwrap();
        assert!(buf.len() > 5);
    }
}
