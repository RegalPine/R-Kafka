//! UpdateMetadata Handler — 接收 Metadata 广播 (API 6)

use rk_core::error::Result;
use rk_protocol::apis::update_metadata::*;
use rk_protocol::error_codes::KafkaErrorCode;
use tracing::debug;

pub struct UpdateMetadataHandler {
    _broker_id: i32,
}

impl UpdateMetadataHandler {
    pub fn new(broker_id: i32) -> Self {
        Self {
            _broker_id: broker_id,
        }
    }

    pub fn handle(
        &self,
        request: UpdateMetadataRequest,
        _version: i16,
    ) -> Result<UpdateMetadataResponse> {
        debug!(
            controller_id = request.controller_id,
            brokers = request.brokers.len(),
            "UpdateMetadata request received"
        );
        Ok(UpdateMetadataResponse {
            throttle_time_ms: 0,
            error_code: KafkaErrorCode::None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_update_metadata_handler() {
        let handler = UpdateMetadataHandler::new(1);
        let req = UpdateMetadataRequest {
            controller_id: 0,
            controller_epoch: 1,
            brokers: vec![UpdateMetadataBroker {
                broker_id: 1,
                host: "h".to_string(),
                port: 9092,
            }],
        };
        let resp = handler.handle(req, 0).unwrap();
        assert_eq!(resp.error_code, KafkaErrorCode::None);
    }
}
