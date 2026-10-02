//! EndQuorumEpoch Handler
//!
//! 处理 EndQuorumEpoch 请求 (API Key = 53)。
//! Leader 通知 Follower 当前纪元结束。

use rk_core::error::Result;
use rk_protocol::apis::end_quorum_epoch::*;
use rk_protocol::error_codes::KafkaErrorCode;
use tracing::debug;

/// EndQuorumEpoch 请求处理器
pub struct EndQuorumEpochHandler {
    broker_id: i32,
}

impl EndQuorumEpochHandler {
    pub fn new(broker_id: i32) -> Self {
        Self { broker_id }
    }

    /// 处理 EndQuorumEpoch 请求
    pub fn handle(
        &self,
        request: EndQuorumEpochRequest,
        _version: i16,
    ) -> Result<EndQuorumEpochResponse> {
        debug!(
            leader_id = request.leader_id,
            leader_epoch = request.leader_epoch,
            "EndQuorumEpoch request received"
        );

        // Phase 3 基础: 确认纪元结束
        Ok(EndQuorumEpochResponse {
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
    fn test_end_quorum_handler() {
        let handler = EndQuorumEpochHandler::new(1);
        let req = EndQuorumEpochRequest {
            cluster_id: "cluster-1".to_string(),
            leader_id: 2,
            leader_epoch: 10,
        };
        let resp = handler.handle(req, 0).unwrap();
        assert_eq!(resp.error_code, KafkaErrorCode::None);
        assert_eq!(resp.voter_id, 1);
        assert_eq!(resp.voter_epoch, 10);
    }
}
