//! StopReplica Handler — 停止指定副本 (API 5)

use rk_core::error::Result;
use rk_protocol::apis::stop_replica::*;
use rk_protocol::error_codes::KafkaErrorCode;
use tracing::debug;

pub struct StopReplicaHandler {
    _broker_id: i32,
}

impl StopReplicaHandler {
    pub fn new(broker_id: i32) -> Self {
        Self {
            _broker_id: broker_id,
        }
    }

    pub fn handle(
        &self,
        request: StopReplicaRequest,
        _version: i16,
    ) -> Result<StopReplicaResponse> {
        debug!(
            controller_id = request.controller_id,
            partitions = request.partitions.len(),
            delete = request.delete_partitions,
            "StopReplica request received"
        );
        let partition_errors = request
            .partitions
            .iter()
            .map(|p| StopReplicaPartitionError {
                topic_name: p.topic_name.clone(),
                partition_index: p.partition_index,
                error_code: KafkaErrorCode::None,
            })
            .collect();
        Ok(StopReplicaResponse {
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
    fn test_stop_replica_handler() {
        let handler = StopReplicaHandler::new(1);
        let req = StopReplicaRequest {
            controller_id: 0,
            controller_epoch: 1,
            delete_partitions: true,
            partitions: vec![StopReplicaPartitionInfo {
                topic_name: "t".to_string(),
                partition_index: 0,
            }],
        };
        let resp = handler.handle(req, 0).unwrap();
        assert_eq!(resp.error_code, KafkaErrorCode::None);
    }
}
