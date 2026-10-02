//! BrokerRegistration Handler — Broker 注册 (API 54)

use rk_core::error::Result;
use rk_protocol::apis::broker_registration::*;
use rk_protocol::error_codes::KafkaErrorCode;
use tracing::info;

pub struct BrokerRegistrationHandler { _broker_id: i32 }

impl BrokerRegistrationHandler {
    pub fn new(broker_id: i32) -> Self { Self { _broker_id: broker_id } }

    pub fn handle(&self, request: BrokerRegistrationRequest, _version: i16) -> Result<BrokerRegistrationResponse> {
        info!(
            broker_id = request.broker_id,
            cluster_id = %request.cluster_id,
            host = %request.host,
            port = request.port,
            broker_epoch = request.broker_epoch,
            rack = ?request.rack,
            "BrokerRegistration request received"
        );
        // Phase 3: 返回注册的 broker_epoch 确认
        Ok(BrokerRegistrationResponse {
            throttle_time_ms: 0,
            error_code: KafkaErrorCode::None,
            broker_epoch: request.broker_epoch,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_broker_registration_handler() {
        let handler = BrokerRegistrationHandler::new(0);
        let req = BrokerRegistrationRequest {
            broker_id: 1,
            cluster_id: "cluster1".to_string(),
            features: vec![],
            rack: Some("rack1".to_string()),
            host: "host1".to_string(),
            port: 9092,
            broker_epoch: 42,
            endpoints: vec![],
        };
        let resp = handler.handle(req, 0).unwrap();
        assert_eq!(resp.error_code, KafkaErrorCode::None);
        assert_eq!(resp.broker_epoch, 42);
    }
}
