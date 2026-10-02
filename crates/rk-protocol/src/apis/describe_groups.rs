//! DescribeGroups API (Key = 15)
//!
//! 查询消费者组详情。

use rk_core::error::Result;
use crate::codec::{KafkaRequestDecoder, KafkaResponseEncoder};
use crate::types::{KafkaReader, KafkaWriter};
use crate::error_codes::KafkaErrorCode;

// ─── Request ──────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct DescribeGroupsRequest {
    pub groups: Vec<String>,
    /// v1+: include_authorized_operations (bool)
    pub include_authorized_operations: bool,
}

impl KafkaRequestDecoder for DescribeGroupsRequest {
    fn decode(reader: &mut KafkaReader<'_>, version: i16) -> Result<Self> {
        if version >= 5 {
            // Flexible
            let groups = reader.read_compact_array(|r| r.read_compact_string())?;
            let include_authorized_operations = if version >= 1 {
                reader.read_bool()?
            } else {
                false
            };
            let _tags = reader.read_tagged_fields()?;
            Ok(Self {
                groups,
                include_authorized_operations,
            })
        } else {
            let groups = reader.read_array(|r| r.read_string())?;
            let include_authorized_operations = if version >= 1 {
                reader.read_bool()?
            } else {
                false
            };
            Ok(Self {
                groups,
                include_authorized_operations,
            })
        }
    }
}

// ─── Response ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct DescribeGroupsResponse {
    /// v1+: throttle_time_ms (i32)
    pub throttle_time_ms: i32,
    pub groups: Vec<DescribeGroupsResponseGroup>,
}

#[derive(Debug, Clone)]
pub struct DescribeGroupsResponseGroup {
    pub error_code: KafkaErrorCode,
    pub group_id: String,
    pub group_state: String,
    pub protocol_type: String,
    pub protocol_data: String,
    pub members: Vec<DescribeGroupsResponseMember>,
    /// v1+: authorized_operations (i32)
    pub authorized_operations: i32,
}

#[derive(Debug, Clone)]
pub struct DescribeGroupsResponseMember {
    pub member_id: String,
    /// v3+: group_instance_id (nullable_string)
    pub group_instance_id: Option<String>,
    pub client_id: String,
    pub client_host: String,
    pub member_metadata: Vec<u8>,
    pub member_assignment: Vec<u8>,
}

impl KafkaResponseEncoder for DescribeGroupsResponse {
    fn encode(&self, writer: &mut KafkaWriter<'_>, version: i16) -> Result<()> {
        if version >= 1 {
            writer.write_i32(self.throttle_time_ms);
        }

        if version >= 5 {
            // Flexible
            writer.write_compact_array(&self.groups, |w, g| {
                w.write_i16(g.error_code.as_i16());
                w.write_compact_string(&g.group_id);
                w.write_compact_string(&g.group_state);
                w.write_compact_string(&g.protocol_type);
                w.write_compact_string(&g.protocol_data);
                w.write_compact_array(&g.members, |w2, m| {
                    w2.write_compact_string(&m.member_id);
                    w2.write_compact_nullable_string(m.group_instance_id.as_deref());
                    w2.write_compact_string(&m.client_id);
                    w2.write_compact_string(&m.client_host);
                    w2.write_compact_bytes(&m.member_metadata);
                    w2.write_compact_bytes(&m.member_assignment);
                    w2.write_tagged_fields(&[]);
                });
                if version >= 1 {
                    w.write_i32(g.authorized_operations);
                }
                w.write_tagged_fields(&[]);
            });
            writer.write_tagged_fields(&[]);
        } else {
            writer.write_array(&self.groups, |w, g| {
                w.write_i16(g.error_code.as_i16());
                w.write_string(&g.group_id);
                w.write_string(&g.group_state);
                w.write_string(&g.protocol_type);
                w.write_string(&g.protocol_data);
                w.write_array(&g.members, |w2, m| {
                    w2.write_string(&m.member_id);
                    if version >= 3 {
                        w2.write_nullable_string(m.group_instance_id.as_deref());
                    }
                    w2.write_string(&m.client_id);
                    w2.write_string(&m.client_host);
                    w2.write_bytes(&m.member_metadata);
                    w2.write_bytes(&m.member_assignment);
                });
                if version >= 1 {
                    w.write_i32(g.authorized_operations);
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
    fn test_describe_groups_response_v0() {
        let resp = DescribeGroupsResponse {
            throttle_time_ms: 0,
            groups: vec![DescribeGroupsResponseGroup {
                error_code: KafkaErrorCode::None,
                group_id: "test-group".to_string(),
                group_state: "Stable".to_string(),
                protocol_type: "consumer".to_string(),
                protocol_data: "range".to_string(),
                members: vec![DescribeGroupsResponseMember {
                    member_id: "member-1".to_string(),
                    group_instance_id: None,
                    client_id: "client-1".to_string(),
                    client_host: "/127.0.0.1".to_string(),
                    member_metadata: vec![1, 2, 3],
                    member_assignment: vec![4, 5, 6],
                }],
                authorized_operations: 0,
            }],
        };
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        resp.encode(&mut w, 0).unwrap();
        assert!(buf.len() > 50);
    }

    #[test]
    fn test_describe_groups_response_v5_flexible() {
        let resp = DescribeGroupsResponse {
            throttle_time_ms: 0,
            groups: vec![DescribeGroupsResponseGroup {
                error_code: KafkaErrorCode::None,
                group_id: "test-group".to_string(),
                group_state: "Stable".to_string(),
                protocol_type: "consumer".to_string(),
                protocol_data: "range".to_string(),
                members: vec![],
                authorized_operations: 0,
            }],
        };
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        resp.encode(&mut w, 5).unwrap();
        assert!(buf.len() > 20);
    }
}
