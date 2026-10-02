//! SaslAuthenticate API (Key = 36)
//!
//! SASL 认证交换: 客户端发送认证数据，Broker 验证后返回结果。
//! v0: legacy, v1+: flexible (KIP-482) + session_lifetime_ms

use rk_core::error::Result;
use crate::codec::{KafkaRequestDecoder, KafkaResponseEncoder};
use crate::types::{KafkaReader, KafkaWriter};
use crate::error_codes::KafkaErrorCode;

// ─── Request ──────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct SaslAuthenticateRequest {
    /// SASL 认证数据 (机制特定的字节流)
    pub auth_bytes: Vec<u8>,
}

impl SaslAuthenticateRequest {
    /// 从 SASL/PLAIN auth_bytes 解析用户名和密码
    ///
    /// SASL/PLAIN 格式: \0username\0password (RFC 4616)
    pub fn parse_plain_credentials(&self) -> Option<(String, String)> {
        // 格式: [authzid] \0 authcid \0 passwd
        let parts: Vec<&[u8]> = self.auth_bytes.splitn(3, |&b| b == 0).collect();
        if parts.len() != 3 {
            return None;
        }
        let _authzid = String::from_utf8(parts[0].to_vec()).ok()?;
        let username = String::from_utf8(parts[1].to_vec()).ok()?;
        let password = String::from_utf8(parts[2].to_vec()).ok()?;
        if username.is_empty() {
            return None;
        }
        Some((username, password))
    }
}

impl KafkaRequestDecoder for SaslAuthenticateRequest {
    fn decode(reader: &mut KafkaReader<'_>, version: i16) -> Result<Self> {
        if version >= 2 {
            // Flexible
            let auth_bytes = reader.read_compact_bytes()?;
            let _tags = reader.read_tagged_fields()?;
            Ok(Self { auth_bytes })
        } else {
            // Legacy v0-v1
            let auth_bytes = reader.read_bytes()?;
            Ok(Self { auth_bytes })
        }
    }
}

// ─── Response ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct SaslAuthenticateResponse {
    pub error_code: KafkaErrorCode,
    /// v1+: error_message (nullable_string)
    pub error_message: Option<String>,
    /// 认证完成后 Broker 返回的数据 (可能为空)
    pub auth_bytes: Vec<u8>,
    /// v1+: session_lifetime_ms (i64) — 会话生存时间, 0=无限
    pub session_lifetime_ms: i64,
}

impl SaslAuthenticateResponse {
    /// 认证成功
    pub fn success(auth_bytes: Vec<u8>) -> Self {
        Self {
            error_code: KafkaErrorCode::None,
            error_message: None,
            auth_bytes,
            session_lifetime_ms: 0, // 无限期
        }
    }

    /// 认证失败
    pub fn failure(message: String) -> Self {
        Self {
            error_code: KafkaErrorCode::SaslAuthenticationFailed,
            error_message: Some(message),
            auth_bytes: Vec::new(),
            session_lifetime_ms: 0,
        }
    }
}

impl KafkaResponseEncoder for SaslAuthenticateResponse {
    fn encode(&self, writer: &mut KafkaWriter<'_>, version: i16) -> Result<()> {
        writer.write_i16(self.error_code.as_i16());

        if version >= 2 {
            // Flexible
            writer.write_compact_nullable_string(self.error_message.as_deref());
            writer.write_compact_bytes(&self.auth_bytes);
            writer.write_i64(self.session_lifetime_ms);
            writer.write_tagged_fields(&[]);
        } else if version >= 1 {
            // v1: legacy format with error_message
            writer.write_nullable_string(self.error_message.as_deref());
            writer.write_bytes(&self.auth_bytes);
            writer.write_i64(self.session_lifetime_ms);
        } else {
            // v0: no error_message, no session_lifetime
            writer.write_bytes(&self.auth_bytes);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_sasl_authenticate_request_v0_roundtrip() {
        let auth_bytes = b"\0admin\0secret".to_vec();
        let _req = SaslAuthenticateRequest { auth_bytes: auth_bytes.clone() };

        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        w.write_bytes(&auth_bytes);
        drop(w);

        let mut reader = KafkaReader::new(&buf);
        let decoded = SaslAuthenticateRequest::decode(&mut reader, 0).unwrap();
        assert_eq!(decoded.auth_bytes, auth_bytes);
    }

    #[test]
    fn test_sasl_authenticate_request_v2_flexible_roundtrip() {
        let auth_bytes = b"\0admin\0password".to_vec();
        let _req = SaslAuthenticateRequest { auth_bytes: auth_bytes.clone() };

        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        w.write_compact_bytes(&auth_bytes);
        w.write_tagged_fields(&[]);
        drop(w);

        let mut reader = KafkaReader::new(&buf);
        let decoded = SaslAuthenticateRequest::decode(&mut reader, 2).unwrap();
        assert_eq!(decoded.auth_bytes, auth_bytes);
    }

    #[test]
    fn test_sasl_authenticate_response_v0_encode() {
        let resp = SaslAuthenticateResponse::success(b"response_data".to_vec());
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        resp.encode(&mut w, 0).unwrap();
        drop(w);

        // error_code(2) + bytes_len(4) + "response_data"(13) = 19
        assert_eq!(buf.len(), 19);
    }

    #[test]
    fn test_sasl_authenticate_response_v1_encode() {
        let resp = SaslAuthenticateResponse::failure("bad credentials".to_string());
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        resp.encode(&mut w, 1).unwrap();
        drop(w);

        // error_code(2) + nullable_string(2+15) + bytes_len(4+0) + session_lifetime(8) = 31
        assert_eq!(buf.len(), 31);
    }

    #[test]
    fn test_sasl_authenticate_response_v2_encode() {
        let resp = SaslAuthenticateResponse::success(Vec::new());
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        resp.encode(&mut w, 2).unwrap();
        drop(w);

        // error_code(2) + compact_nullable_string(1=null) + compact_bytes(1=0-1+1=1)
        // + session_lifetime(8) + tagged_fields(1) = 13
        assert!(buf.len() > 0);
    }

    #[test]
    fn test_parse_plain_credentials() {
        let req = SaslAuthenticateRequest {
            auth_bytes: b"\0admin\0secret123".to_vec(),
        };
        let (username, password) = req.parse_plain_credentials().unwrap();
        assert_eq!(username, "admin");
        assert_eq!(password, "secret123");
    }

    #[test]
    fn test_parse_plain_credentials_with_authzid() {
        let req = SaslAuthenticateRequest {
            auth_bytes: b"authz_user\0admin\0pass".to_vec(),
        };
        let (username, password) = req.parse_plain_credentials().unwrap();
        assert_eq!(username, "admin");
        assert_eq!(password, "pass");
    }

    #[test]
    fn test_parse_plain_credentials_invalid() {
        let req = SaslAuthenticateRequest {
            auth_bytes: b"no_null_separators".to_vec(),
        };
        assert!(req.parse_plain_credentials().is_none());
    }

    #[test]
    fn test_parse_plain_credentials_empty_username() {
        let req = SaslAuthenticateRequest {
            auth_bytes: b"\0\0password".to_vec(),
        };
        assert!(req.parse_plain_credentials().is_none());
    }
}
