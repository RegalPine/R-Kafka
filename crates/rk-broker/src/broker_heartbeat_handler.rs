//! BrokerHeartbeat Handler — Broker 心跳 (API 55)

use rk_core::error::Result;
use rk_protocol::apis::broker_heartbeat::*;
use rk_protocol::error_codes::KafkaErrorCode;
use tracing::debug;

pub struct BrokerHeartbeatHandler { _broker_id: i32 }

impl BrokerHeartbeatHandler {
    pub fn new(broker_id: i32) -> Self { Self { _broker_id: broker_id } }

    pub fn handle(&self, request: BrokerHeartbeatRequest, _version: i16) -> Result<BrokerHeartbeatResponse> {
        debug!(
            broker_id = request.broker_id,
            broker_epoch = request.broker_epoch,
            want_fence = request.want_fence,
            want_shut_down = request.want_shut_down,
            "BrokerHeartbeat request received"
        );
        // Phase 3: 返回基础心跳响应
        Ok(BrokerHeartbeatResponse {
            throttle_time_ms: 0,
            error_code: KafkaErrorCode::None,
            leader_id: -1,
            leader_epoch: -1,
            is_controller: false,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_broker_heartbeat_handler() {
        let handler = BrokerHeartbeatHandler::new(0);
        let req = BrokerHeartbeatRequest {
            broker_id: 1,
            broker_epoch: 42,
            want_fence: false,
            want_shut_down: false,
            current_metadata_offset: 1000,
        };
        let resp = handler.handle(req, 0).unwrap();
        assert_eq!(resp.error_code, KafkaErrorCode::None);
        assert_eq!(resp.leader_id, -1);
    }

    #[test]
    fn test_broker_heartbeat_shutdown() {
        let handler = BrokerHeartbeatHandler::new(0);
        let req = BrokerHeartbeatRequest {
            broker_id: 1,
            broker_epoch: 42,
            want_fence: false,
            want_shut_down: true,
            current_metadata_offset: 500,
        };
        let resp = handler.handle(req, 0).unwrap();
        assert_eq!(resp.error_code, KafkaErrorCode::None);
    }
}
