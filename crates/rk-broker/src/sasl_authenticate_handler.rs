//! SaslAuthenticate Handler
//!
//! 处理 SaslAuthenticate 请求 (API Key = 36)。
//! Phase 1: SASL/PLAIN 认证。

use std::sync::Arc;

use rk_core::error::Result;
use rk_protocol::apis::sasl_authenticate::*;
use tracing::debug;

use crate::sasl_authenticator::{AuthResult, SaslAuthenticator};

/// SaslAuthenticate 请求处理器
pub struct SaslAuthenticateHandler {
    authenticator: Arc<SaslAuthenticator>,
}

impl SaslAuthenticateHandler {
    pub fn new(authenticator: Arc<SaslAuthenticator>) -> Self {
        Self { authenticator }
    }

    /// 处理 SaslAuthenticate 请求
    pub fn handle(
        &self,
        request: SaslAuthenticateRequest,
        version: i16,
    ) -> Result<SaslAuthenticateResponse> {
        debug!(
            auth_bytes_len = request.auth_bytes.len(),
            version = version,
            "SaslAuthenticate request"
        );

        // Phase 1: 仅支持 SASL/PLAIN
        let result = self.authenticator.authenticate_plain(&request.auth_bytes);

        match result {
            AuthResult::Success(username) => {
                debug!(username = %username, "SASL authentication successful");
                Ok(SaslAuthenticateResponse::success(Vec::new()))
            }
            AuthResult::Failure(reason) => {
                debug!(reason = %reason, "SASL authentication failed");
                Ok(SaslAuthenticateResponse::failure(reason))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rk_protocol::error_codes::KafkaErrorCode;
    use std::collections::HashMap;

    fn make_handler_with_creds(creds: HashMap<String, String>) -> SaslAuthenticateHandler {
        SaslAuthenticateHandler::new(Arc::new(SaslAuthenticator::with_auth(creds)))
    }

    fn make_handler_dev() -> SaslAuthenticateHandler {
        SaslAuthenticateHandler::new(Arc::new(SaslAuthenticator::new()))
    }

    #[test]
    fn test_authenticate_success() {
        let mut creds = HashMap::new();
        creds.insert("admin".to_string(), "secret123".to_string());
        let handler = make_handler_with_creds(creds);

        let req = SaslAuthenticateRequest {
            auth_bytes: b"\0admin\0secret123".to_vec(),
        };
        let resp = handler.handle(req, 0).unwrap();
        assert_eq!(resp.error_code, KafkaErrorCode::None);
        assert!(resp.error_message.is_none());
    }

    #[test]
    fn test_authenticate_wrong_password() {
        let mut creds = HashMap::new();
        creds.insert("admin".to_string(), "secret123".to_string());
        let handler = make_handler_with_creds(creds);

        let req = SaslAuthenticateRequest {
            auth_bytes: b"\0admin\0wrong".to_vec(),
        };
        let resp = handler.handle(req, 0).unwrap();
        assert_eq!(resp.error_code, KafkaErrorCode::SaslAuthenticationFailed);
        assert!(resp.error_message.is_some());
    }

    #[test]
    fn test_authenticate_dev_mode() {
        let handler = make_handler_dev();

        let req = SaslAuthenticateRequest {
            auth_bytes: b"\0anyuser\0anypass".to_vec(),
        };
        let resp = handler.handle(req, 0).unwrap();
        assert_eq!(resp.error_code, KafkaErrorCode::None);
    }

    #[test]
    fn test_authenticate_invalid_format() {
        let handler = make_handler_dev();

        let req = SaslAuthenticateRequest {
            auth_bytes: b"invalid_data".to_vec(),
        };
        let resp = handler.handle(req, 0).unwrap();
        assert_eq!(resp.error_code, KafkaErrorCode::SaslAuthenticationFailed);
    }

    #[test]
    fn test_authenticate_v1_response() {
        let mut creds = HashMap::new();
        creds.insert("user1".to_string(), "pass1".to_string());
        let handler = make_handler_with_creds(creds);

        let req = SaslAuthenticateRequest {
            auth_bytes: b"\0user1\0pass1".to_vec(),
        };
        let resp = handler.handle(req, 1).unwrap();
        assert_eq!(resp.error_code, KafkaErrorCode::None);
        // v1 response includes session_lifetime_ms
        assert_eq!(resp.session_lifetime_ms, 0);
    }

    #[test]
    fn test_authenticate_v2_flexible_response() {
        let handler = make_handler_dev();

        let req = SaslAuthenticateRequest {
            auth_bytes: b"\0testuser\0testpass".to_vec(),
        };
        let resp = handler.handle(req, 2).unwrap();
        assert_eq!(resp.error_code, KafkaErrorCode::None);
    }
}
