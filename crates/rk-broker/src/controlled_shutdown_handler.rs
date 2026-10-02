//! ControlledShutdown Handler — 优雅关闭 (API 7)

use rk_core::error::Result;
use rk_protocol::apis::controlled_shutdown::*;
use rk_protocol::error_codes::KafkaErrorCode;
use tracing::debug;

pub struct ControlledShutdownHandler { _broker_id: i32 }

impl ControlledShutdownHandler {
    pub fn new(broker_id: i32) -> Self { Self { _broker_id: broker_id } }

    pub fn handle(&self, request: ControlledShutdownRequest, _version: i16) -> Result<ControlledShutdownResponse> {
        debug!(broker_id = request.broker_id, broker_epoch = request.broker_epoch,
            "ControlledShutdown request received");
        // Phase 3: 返回空列表 (无剩余 partition)
        Ok(ControlledShutdownResponse {
            throttle_time_ms: 0, error_code: KafkaErrorCode::None,
            remaining_partitions: vec![],
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_controlled_shutdown_handler() {
        let handler = ControlledShutdownHandler::new(1);
        let req = ControlledShutdownRequest { broker_id: 1, broker_epoch: 42 };
        let resp = handler.handle(req, 0).unwrap();
        assert_eq!(resp.error_code, KafkaErrorCode::None);
        assert!(resp.remaining_partitions.is_empty());
    }
}
