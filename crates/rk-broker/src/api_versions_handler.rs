//! ApiVersions Handler
//!
//! 处理 ApiVersions 请求 (API Key = 18)。
//! 返回 R-Kafka 支持的全部 API 版本范围。

use rk_core::error::Result;
use rk_protocol::api_versions::{ApiVersionsRequest, ApiVersionsResponse};

/// ApiVersions 请求处理器
pub struct ApiVersionsHandler;

impl ApiVersionsHandler {
    pub fn new() -> Self {
        Self
    }

    /// 处理 ApiVersions 请求
    ///
    /// Phase 1: 直接返回 SUPPORTED_API_VERSIONS 表。
    /// 不检查 client_software_name/version (仅用于信息收集)。
    pub fn handle(
        &self,
        _request: ApiVersionsRequest,
        _version: i16,
    ) -> Result<ApiVersionsResponse> {
        Ok(ApiVersionsResponse::supported())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_api_versions_handler_supported() {
        let handler = ApiVersionsHandler::new();
        let request = ApiVersionsRequest {
            client_software_name: None,
            client_software_version: None,
        };
        let response = handler.handle(request, 0).unwrap();
        assert_eq!(response.error_code, rk_protocol::error_codes::KafkaErrorCode::None);
        assert!(!response.api_versions.is_empty());

        // 验证关键 API 存在
        let api_keys: Vec<i16> = response.api_versions.iter().map(|av| av.api_key).collect();
        assert!(api_keys.contains(&0));  // Produce
        assert!(api_keys.contains(&1));  // Fetch
        assert!(api_keys.contains(&2));  // ListOffsets
        assert!(api_keys.contains(&3));  // Metadata
        assert!(api_keys.contains(&18)); // ApiVersions
        assert!(api_keys.contains(&19)); // CreateTopics
        assert!(api_keys.contains(&20)); // DeleteTopics
    }

    #[test]
    fn test_api_versions_handler_v3_with_software_info() {
        let handler = ApiVersionsHandler::new();
        let request = ApiVersionsRequest {
            client_software_name: Some("test-client".to_string()),
            client_software_version: Some("1.0.0".to_string()),
        };
        let response = handler.handle(request, 3).unwrap();
        assert_eq!(response.error_code, rk_protocol::error_codes::KafkaErrorCode::None);
        assert!(!response.api_versions.is_empty());
    }
}
