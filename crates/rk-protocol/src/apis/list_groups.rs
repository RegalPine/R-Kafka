//! ListGroups API (Key = 16)
//!
//! 列出所有消费者组。

use crate::codec::{KafkaRequestDecoder, KafkaResponseEncoder};
use crate::error_codes::KafkaErrorCode;
use crate::types::{KafkaReader, KafkaWriter};
use rk_core::error::Result;

// ─── Request ──────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ListGroupsRequest {
    /// v4+: states_filter (compact_array of string)
    pub states_filter: Option<Vec<String>>,
}

impl KafkaRequestDecoder for ListGroupsRequest {
    fn decode(reader: &mut KafkaReader<'_>, version: i16) -> Result<Self> {
        if version >= 4 {
            // Flexible
            let states_filter = reader.read_compact_array(|r| r.read_compact_string())?;
            let _tags = reader.read_tagged_fields()?;
            Ok(Self {
                states_filter: if states_filter.is_empty() {
                    None
                } else {
                    Some(states_filter)
                },
            })
        } else {
            Ok(Self {
                states_filter: None,
            })
        }
    }
}

// ─── Response ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ListGroupsResponse {
    /// v1+: throttle_time_ms (i32)
    pub throttle_time_ms: i32,
    pub error_code: KafkaErrorCode,
    pub groups: Vec<ListGroupsResponseGroup>,
}

#[derive(Debug, Clone)]
pub struct ListGroupsResponseGroup {
    pub group_id: String,
    pub protocol_type: String,
    /// v4+: group_state (string)
    pub group_state: Option<String>,
}

impl KafkaResponseEncoder for ListGroupsResponse {
    fn encode(&self, writer: &mut KafkaWriter<'_>, version: i16) -> Result<()> {
        if version >= 1 {
            writer.write_i32(self.throttle_time_ms);
        }

        if version >= 4 {
            // Flexible
            writer.write_i16(self.error_code.as_i16());
            writer.write_compact_array(&self.groups, |w, g| {
                w.write_compact_string(&g.group_id);
                w.write_compact_string(&g.protocol_type);
                w.write_compact_string(g.group_state.as_deref().unwrap_or(""));
                w.write_tagged_fields(&[]);
            });
            writer.write_tagged_fields(&[]);
        } else {
            writer.write_i16(self.error_code.as_i16());
            writer.write_array(&self.groups, |w, g| {
                w.write_string(&g.group_id);
                w.write_string(&g.protocol_type);
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
    fn test_list_groups_response_v0() {
        let resp = ListGroupsResponse {
            throttle_time_ms: 0,
            error_code: KafkaErrorCode::None,
            groups: vec![ListGroupsResponseGroup {
                group_id: "test-group".to_string(),
                protocol_type: "consumer".to_string(),
                group_state: None,
            }],
        };
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        resp.encode(&mut w, 0).unwrap();
        assert!(buf.len() > 10);
    }

    #[test]
    fn test_list_groups_response_v4_flexible() {
        let resp = ListGroupsResponse {
            throttle_time_ms: 0,
            error_code: KafkaErrorCode::None,
            groups: vec![ListGroupsResponseGroup {
                group_id: "test-group".to_string(),
                protocol_type: "consumer".to_string(),
                group_state: Some("Stable".to_string()),
            }],
        };
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        resp.encode(&mut w, 4).unwrap();
        assert!(buf.len() > 10);
    }
}
