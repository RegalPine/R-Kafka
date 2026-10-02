//! BeginQuorumEpoch Handler
//!
//! 处理 BeginQuorumEpoch 请求 (API Key = 52)。
//! Leader 通知 Follower 新纪元开始。

use rk_core::error::Result;
use rk_protocol::apis::begin_quorum_epoch::*;
use rk_protocol::error_codes::KafkaErrorCode;
use tracing::debug;

/// BeginQuorumEpoch 请求处理器
pub struct BeginQuorumEpochHandler {
    broker_id: i32,
}

impl BeginQuorumEpochHandler {
    pub fn new(broker_id: i32) -> Self {
        Self { broker_id }
    }

    /// 处理 BeginQuorumEpoch 请求
    pub fn handle(
        &self,
        request: BeginQuorumEpochRequest,
        _version: i16,
    ) -> Result<BeginQuorumEpochResponse> {
        debug!(
            leader_id = request.leader_id,
            leader_epoch = request.leader_epoch,
            last_offset = request.last_offset,
            "BeginQuorumEpoch request received"
        );

        // Phase 3 基础: 接受新纪元
        Ok(BeginQuorumEpochResponse {
            throttle_time_ms: 0,
            error_code: KafkaErrorCode::None,
            voter_id: self.broker_id,
            voter_epoch: request.leader_epoch,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_begin_quorum_handler() {
        let handler = BeginQuorumEpochHandler::new(1);
        let req = BeginQuorumEpochRequest {
            cluster_id: "cluster-1".to_string(),
            leader_id: 2,
            leader_epoch: 10,
            last_offset: 500,
        };
        let resp = handler.handle(req, 0).unwrap();
        assert_eq!(resp.error_code, KafkaErrorCode::None);
        assert_eq!(resp.voter_id, 1);
        assert_eq!(resp.voter_epoch, 10);
    }
}
