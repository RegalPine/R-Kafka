//! SaslHandshake Handler
//!
//! 处理 SaslHandshake 请求 (API Key = 17)。
//! 客户端通过此 API 发现 Broker 支持的 SASL 机制。

use std::sync::Arc;

use rk_core::error::Result;
use rk_protocol::apis::sasl_handshake::*;
use tracing::debug;

use crate::sasl_authenticator::SaslAuthenticator;

/// SaslHandshake 请求处理器
pub struct SaslHandshakeHandler {
    authenticator: Arc<SaslAuthenticator>,
}

impl SaslHandshakeHandler {
    pub fn new(authenticator: Arc<SaslAuthenticator>) -> Self {
        Self { authenticator }
    }

    /// 处理 SaslHandshake 请求
    pub fn handle(
        &self,
        request: SaslHandshakeRequest,
        _version: i16,
    ) -> Result<SaslHandshakeResponse> {
        debug!(
            mechanism = %request.mechanism,
            "SaslHandshake request"
        );

        let supported = self.authenticator.supported_mechanism_names();

        if self.authenticator.is_mechanism_supported(&request.mechanism) {
            Ok(SaslHandshakeResponse::supported(supported))
        } else {
            Ok(SaslHandshakeResponse::unsupported_mechanism(supported))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rk_protocol::error_codes::KafkaErrorCode;

    fn make_handler() -> SaslHandshakeHandler {
        SaslHandshakeHandler::new(Arc::new(SaslAuthenticator::new()))
    }

    #[test]
    fn test_sasl_handshake_supported() {
        let handler = make_handler();
        let req = SaslHandshakeRequest {
            mechanism: "PLAIN".to_string(),
        };
        let resp = handler.handle(req, 0).unwrap();
        assert_eq!(resp.error_code, KafkaErrorCode::None);
        assert!(resp.mechanisms.contains(&"PLAIN".to_string()));
    }

    #[test]
    fn test_sasl_handshake_unsupported() {
        let handler = make_handler();
        let req = SaslHandshakeRequest {
            mechanism: "GSSAPI".to_string(),
        };
        let resp = handler.handle(req, 0).unwrap();
        assert_eq!(resp.error_code, KafkaErrorCode::UnsupportedSaslMechanism);
        assert!(resp.mechanisms.contains(&"PLAIN".to_string()));
    }

    #[test]
    fn test_sasl_handshake_v1() {
        let handler = make_handler();
        let req = SaslHandshakeRequest {
            mechanism: "PLAIN".to_string(),
        };
        let resp = handler.handle(req, 1).unwrap();
        assert_eq!(resp.error_code, KafkaErrorCode::None);
    }

    #[test]
    fn test_sasl_handshake_with_scram_authenticator() {
        use std::collections::HashMap;
        let auth = SaslAuthenticator::with_auth(HashMap::new());
        let handler = SaslHandshakeHandler::new(Arc::new(auth));

        // SCRAM-SHA-256 not supported in Phase 1
        let req = SaslHandshakeRequest {
            mechanism: "SCRAM-SHA-256".to_string(),
        };
        let resp = handler.handle(req, 0).unwrap();
        assert_eq!(resp.error_code, KafkaErrorCode::UnsupportedSaslMechanism);
    }
}
