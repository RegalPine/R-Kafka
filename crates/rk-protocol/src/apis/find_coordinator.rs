//! FindCoordinator API (Key = 10)
//!
//! 客户端通过此 API 查找 Consumer Group Coordinator。
//! Phase 1: 单 Broker 模式，自身即 Coordinator。

use rk_core::error::Result;
use crate::codec::{KafkaRequestDecoder, KafkaResponseEncoder};
use crate::types::{KafkaReader, KafkaWriter};
use crate::error_codes::KafkaErrorCode;

// ─── Request ──────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct FindCoordinatorRequest {
    /// v0-v3: 单个 key (group_id)
    pub key: String,
    /// v1+: key_type (i8) 0=group, 1=transaction
    pub key_type: i8,
    /// v4+: coordinator_keys (compact_array of compact_string)
    pub coordinator_keys: Option<Vec<String>>,
}

impl KafkaRequestDecoder for FindCoordinatorRequest {
    fn decode(reader: &mut KafkaReader<'_>, version: i16) -> Result<Self> {
        if version >= 4 {
            // Flexible
            let key = reader.read_compact_string()?;
            let key_type = reader.read_i8()?;
            let coordinator_keys = reader.read_compact_array(|r| r.read_compact_string())?;
            let _tags = reader.read_tagged_fields()?;
            Ok(Self {
                key,
                key_type,
                coordinator_keys: Some(coordinator_keys),
            })
        } else {
            // Legacy
            let key = reader.read_string()?;
            let key_type = if version >= 1 {
                reader.read_i8()?
            } else {
                0 // group
            };
            Ok(Self {
                key,
                key_type,
                coordinator_keys: None,
            })
        }
    }
}

// ─── Response ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct FindCoordinatorResponse {
    /// v1+: throttle_time_ms (i32)
    pub throttle_time_ms: i32,
    /// v0-v3: 单个 coordinator
    pub error_code: KafkaErrorCode,
    pub error_message: Option<String>,
    pub node_id: i32,
    pub host: String,
    pub port: i32,
    /// v4+: coordinators (compact_array)
    pub coordinators: Option<Vec<FindCoordinatorResponseCoordinator>>,
}

#[derive(Debug, Clone)]
pub struct FindCoordinatorResponseCoordinator {
    pub key: String,
    pub node_id: i32,
    pub host: String,
    pub port: i32,
    pub error_code: KafkaErrorCode,
    pub error_message: Option<String>,
}

impl FindCoordinatorResponse {
    /// Phase 1: 单 Broker 模式，自身即 Coordinator
    pub fn self_coordinator(broker_id: i32, host: &str, port: i32) -> Self {
        Self {
            throttle_time_ms: 0,
            error_code: KafkaErrorCode::None,
            error_message: None,
            node_id: broker_id,
            host: host.to_string(),
            port,
            coordinators: None,
        }
    }
}

impl KafkaResponseEncoder for FindCoordinatorResponse {
    fn encode(&self, writer: &mut KafkaWriter<'_>, version: i16) -> Result<()> {
        if version >= 1 {
            writer.write_i32(self.throttle_time_ms);
        }

        if version >= 4 {
            // Flexible: v4+ uses array of coordinators
            if let Some(coordinators) = &self.coordinators {
                writer.write_compact_array(coordinators, |w, c| {
                    w.write_compact_string(&c.key);
                    w.write_i32(c.node_id);
                    w.write_compact_string(&c.host);
                    w.write_i32(c.port);
                    w.write_i16(c.error_code.as_i16());
                    w.write_compact_nullable_string(c.error_message.as_deref());
                    w.write_tagged_fields(&[]);
                });
            } else {
                // Single coordinator as compact_array with 1 element
                writer.write_unsigned_varint(2); // length+1 = 2
                writer.write_compact_string(&""); // key
                writer.write_i32(self.node_id);
                writer.write_compact_string(&self.host);
                writer.write_i32(self.port);
                writer.write_i16(self.error_code.as_i16());
                writer.write_compact_nullable_string(self.error_message.as_deref());
                writer.write_tagged_fields(&[]);
            }
            writer.write_tagged_fields(&[]);
        } else {
            // v0-v3: single coordinator
            writer.write_i16(self.error_code.as_i16());
            if version >= 1 {
                writer.write_nullable_string(self.error_message.as_deref());
            }
            writer.write_i32(self.node_id);
            writer.write_string(&self.host);
            writer.write_i32(self.port);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_find_coordinator_response_v0() {
        let resp = FindCoordinatorResponse::self_coordinator(1, "localhost", 9092);
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        resp.encode(&mut w, 0).unwrap();
        // error_code(2) + node_id(4) + host_len(2) + "localhost"(9) + port(4) = 21
        assert_eq!(buf.len(), 21);
    }

    #[test]
    fn test_find_coordinator_response_v1() {
        let resp = FindCoordinatorResponse::self_coordinator(1, "localhost", 9092);
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        resp.encode(&mut w, 1).unwrap();
        // throttle(4) + error_code(2) + error_msg(4=null) + node_id(4) + host(4+9) + port(4) = 35
        assert!(buf.len() > 20);
    }

    #[test]
    fn test_find_coordinator_response_v4_flexible() {
        let resp = FindCoordinatorResponse {
            throttle_time_ms: 0,
            error_code: KafkaErrorCode::None,
            error_message: None,
            node_id: 1,
            host: "localhost".to_string(),
            port: 9092,
            coordinators: Some(vec![FindCoordinatorResponseCoordinator {
                key: "my-group".to_string(),
                node_id: 1,
                host: "localhost".to_string(),
                port: 9092,
                error_code: KafkaErrorCode::None,
                error_message: None,
            }]),
        };
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        resp.encode(&mut w, 4).unwrap();
        assert!(buf.len() > 10);
    }
}
