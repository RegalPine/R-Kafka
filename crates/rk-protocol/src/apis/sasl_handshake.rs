//! SaslHandshake API (Key = 17)
//!
//! SASL 握手: 客户端请求认证机制列表。
//! v0: legacy, v1: flexible (KIP-482)

use rk_core::error::Result;
use crate::codec::{KafkaRequestDecoder, KafkaResponseEncoder};
use crate::types::{KafkaReader, KafkaWriter};
use crate::error_codes::KafkaErrorCode;

// ─── Request ──────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct SaslHandshakeRequest {
    /// 客户端请求的 SASL 机制 (如 "PLAIN", "SCRAM-SHA-256")
    pub mechanism: String,
}

impl KafkaRequestDecoder for SaslHandshakeRequest {
    fn decode(reader: &mut KafkaReader<'_>, version: i16) -> Result<Self> {
        if version >= 1 {
            // Flexible
            let mechanism = reader.read_compact_string()?;
            let _tags = reader.read_tagged_fields()?;
            Ok(Self { mechanism })
        } else {
            // Legacy v0
            let mechanism = reader.read_string()?;
            Ok(Self { mechanism })
        }
    }
}

// ─── Response ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct SaslHandshakeResponse {
    pub error_code: KafkaErrorCode,
    /// Broker 支持的 SASL 机制列表
    pub mechanisms: Vec<String>,
}

impl SaslHandshakeResponse {
    /// 创建成功响应: 支持的机制列表
    pub fn supported(mechanisms: Vec<String>) -> Self {
        Self {
            error_code: KafkaErrorCode::None,
            mechanisms,
        }
    }

    /// 创建不支持的机制响应
    pub fn unsupported_mechanism(supported: Vec<String>) -> Self {
        Self {
            error_code: KafkaErrorCode::UnsupportedSaslMechanism,
            mechanisms: supported,
        }
    }
}

impl KafkaResponseEncoder for SaslHandshakeResponse {
    fn encode(&self, writer: &mut KafkaWriter<'_>, version: i16) -> Result<()> {
        writer.write_i16(self.error_code.as_i16());

        if version >= 1 {
            // Flexible: compact_array + compact_string
            writer.write_compact_array(&self.mechanisms, |w, m| {
                w.write_compact_string(m);
            });
            writer.write_tagged_fields(&[]);
        } else {
            // Legacy v0
            writer.write_array(&self.mechanisms, |w, m| {
                w.write_string(m);
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
    fn test_sasl_handshake_request_v0_roundtrip() {
        let req = SaslHandshakeRequest {
            mechanism: "PLAIN".to_string(),
        };
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        w.write_string(&req.mechanism);
        drop(w);

        let mut reader = KafkaReader::new(&buf);
        let decoded = SaslHandshakeRequest::decode(&mut reader, 0).unwrap();
        assert_eq!(decoded.mechanism, "PLAIN");
    }

    #[test]
    fn test_sasl_handshake_request_v1_roundtrip() {
        let req = SaslHandshakeRequest {
            mechanism: "SCRAM-SHA-256".to_string(),
        };
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        w.write_compact_string(&req.mechanism);
        w.write_tagged_fields(&[]); // empty tagged fields
        drop(w);

        let mut reader = KafkaReader::new(&buf);
        let decoded = SaslHandshakeRequest::decode(&mut reader, 1).unwrap();
        assert_eq!(decoded.mechanism, "SCRAM-SHA-256");
    }

    #[test]
    fn test_sasl_handshake_response_v0_encode() {
        let resp = SaslHandshakeResponse::supported(vec![
            "PLAIN".to_string(),
            "SCRAM-SHA-256".to_string(),
        ]);
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        resp.encode(&mut w, 0).unwrap();
        drop(w);

        // error_code(2) + array_len(4) + "PLAIN"(2+5) + "SCRAM-SHA-256"(2+13) = 28
        assert_eq!(buf.len(), 28);
    }

    #[test]
    fn test_sasl_handshake_response_v1_encode() {
        let resp = SaslHandshakeResponse::supported(vec![
            "PLAIN".to_string(),
        ]);
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        resp.encode(&mut w, 1).unwrap();
        drop(w);

        // error_code(2) + compact_array_len(1=2-1) + compact_string(1+5=6) + tagged_fields(1)
        assert_eq!(buf.len(), 10);
    }

    #[test]
    fn test_sasl_handshake_unsupported_mechanism() {
        let resp = SaslHandshakeResponse::unsupported_mechanism(vec!["PLAIN".to_string()]);
        assert_eq!(resp.error_code, KafkaErrorCode::UnsupportedSaslMechanism);
        assert_eq!(resp.mechanisms.len(), 1);
    }
}
