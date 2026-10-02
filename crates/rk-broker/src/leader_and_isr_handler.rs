//! LeaderAndIsr Handler — 处理 Controller 发来的 Leader/ISR 更新 (API 4)

use rk_core::error::Result;
use rk_protocol::apis::leader_and_isr::*;
use rk_protocol::error_codes::KafkaErrorCode;
use tracing::debug;

pub struct LeaderAndIsrHandler {
    _broker_id: i32,
}

impl LeaderAndIsrHandler {
    pub fn new(broker_id: i32) -> Self {
        Self {
            _broker_id: broker_id,
        }
    }

    pub fn handle(
        &self,
        request: LeaderAndIsrRequest,
        _version: i16,
    ) -> Result<LeaderAndIsrResponse> {
        debug!(
            controller_id = request.controller_id,
            controller_epoch = request.controller_epoch,
            partitions = request.partition_states.len(),
            "LeaderAndIsr request received"
        );

        let partition_errors = request
            .partition_states
            .iter()
            .map(|ps| LeaderAndIsrPartitionError {
                topic_name: ps.topic_name.clone(),
                partition_index: ps.partition_index,
                error_code: KafkaErrorCode::None,
            })
            .collect();

        Ok(LeaderAndIsrResponse {
            throttle_time_ms: 0,
            error_code: KafkaErrorCode::None,
            partition_errors,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_leader_and_isr_handler() {
        let handler = LeaderAndIsrHandler::new(1);
        let req = LeaderAndIsrRequest {
            controller_id: 0,
            controller_epoch: 1,
            partition_states: vec![LeaderAndIsrPartitionState {
                topic_name: "test".to_string(),
                partition_index: 0,
                controller_epoch: 1,
                leader: 1,
                leader_epoch: 1,
                isr: vec![1, 2],
                partition_epoch: 0,
                replicas: vec![1, 2],
                adding_replicas: vec![],
                removing_replicas: vec![],
            }],
        };
        let resp = handler.handle(req, 0).unwrap();
        assert_eq!(resp.error_code, KafkaErrorCode::None);
        assert_eq!(resp.partition_errors.len(), 1);
    }
}
