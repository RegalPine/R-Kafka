//! FindCoordinator Handler
//!
//! 处理 FindCoordinator 请求 (API Key = 10)。
//! Phase 1: 单 Broker 模式，自身即 Coordinator。

use rk_core::error::Result;
use rk_protocol::apis::find_coordinator::*;
use rk_protocol::error_codes::KafkaErrorCode;
use tracing::debug;

/// FindCoordinator 请求处理器
pub struct FindCoordinatorHandler {
    broker_id: i32,
    host: String,
    port: i32,
}

impl FindCoordinatorHandler {
    pub fn new(broker_id: i32, host: String, port: i32) -> Self {
        Self { broker_id, host, port }
    }

    /// 处理 FindCoordinator 请求
    ///
    /// Phase 1: 单 Broker，自身即 Coordinator。
    /// 所有 group/transaction coordinator 请求都返回自身。
    pub fn handle(
        &self,
        request: FindCoordinatorRequest,
        version: i16,
    ) -> Result<FindCoordinatorResponse> {
        debug!(
            key = %request.key,
            key_type = request.key_type,
            "FindCoordinator request"
        );

        if version >= 4 {
            // v4+: 返回多个 coordinator
            let coordinator_keys = request.coordinator_keys.unwrap_or_default();
            let coordinators = coordinator_keys
                .iter()
                .map(|key| FindCoordinatorResponseCoordinator {
                    key: key.clone(),
                    node_id: self.broker_id,
                    host: self.host.clone(),
                    port: self.port,
                    error_code: KafkaErrorCode::None,
                    error_message: None,
                })
                .collect();

            Ok(FindCoordinatorResponse {
                throttle_time_ms: 0,
                error_code: KafkaErrorCode::None,
                error_message: None,
                node_id: self.broker_id,
                host: self.host.clone(),
                port: self.port,
                coordinators: Some(coordinators),
            })
        } else {
            // v0-v3: 返回单个 coordinator
            Ok(FindCoordinatorResponse::self_coordinator(
                self.broker_id,
                &self.host,
                self.port,
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_handler() -> FindCoordinatorHandler {
        FindCoordinatorHandler::new(1, "localhost".to_string(), 9092)
    }

    #[test]
    fn test_find_coordinator_v0() {
        let handler = make_handler();
        let request = FindCoordinatorRequest {
            key: "my-group".to_string(),
            key_type: 0,
            coordinator_keys: None,
        };
        let response = handler.handle(request, 0).unwrap();
        assert_eq!(response.error_code, KafkaErrorCode::None);
        assert_eq!(response.node_id, 1);
        assert_eq!(response.host, "localhost");
        assert_eq!(response.port, 9092);
    }

    #[test]
    fn test_find_coordinator_v1() {
        let handler = make_handler();
        let request = FindCoordinatorRequest {
            key: "my-group".to_string(),
            key_type: 0, // group
            coordinator_keys: None,
        };
        let response = handler.handle(request, 1).unwrap();
        assert_eq!(response.error_code, KafkaErrorCode::None);
        assert_eq!(response.node_id, 1);
    }

    #[test]
    fn test_find_coordinator_v4_multiple() {
        let handler = make_handler();
        let request = FindCoordinatorRequest {
            key: "my-group".to_string(),
            key_type: 0,
            coordinator_keys: Some(vec![
                "group-a".to_string(),
                "group-b".to_string(),
            ]),
        };
        let response = handler.handle(request, 4).unwrap();
        let coordinators = response.coordinators.unwrap();
        assert_eq!(coordinators.len(), 2);
        assert_eq!(coordinators[0].key, "group-a");
        assert_eq!(coordinators[1].key, "group-b");
        assert_eq!(coordinators[0].node_id, 1);
    }

    #[test]
    fn test_find_coordinator_transaction() {
        let handler = make_handler();
        let request = FindCoordinatorRequest {
            key: "my-txn-id".to_string(),
            key_type: 1, // transaction
            coordinator_keys: None,
        };
        let response = handler.handle(request, 1).unwrap();
        assert_eq!(response.error_code, KafkaErrorCode::None);
        assert_eq!(response.node_id, 1);
    }
}
