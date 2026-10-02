//! Vote Handler
//!
//! 处理 Vote 请求 (API Key = 51)。
//! 将投票请求路由到 rk-controller 的 RaftNode。

use rk_core::error::Result;
use rk_protocol::apis::vote::*;
use rk_protocol::error_codes::KafkaErrorCode;
use tracing::debug;

/// Vote 请求处理器
pub struct VoteHandler {
    broker_id: i32,
}

impl VoteHandler {
    pub fn new(broker_id: i32) -> Self {
        Self { broker_id }
    }

    /// 处理 Vote 请求
    pub fn handle(&self, request: VoteRequest, _version: i16) -> Result<VoteResponse> {
        debug!(
            candidate_id = request.candidate_id,
            candidate_epoch = request.candidate_epoch,
            last_offset = request.last_offset,
            "Vote request received"
        );

        // Phase 3 基础: 返回成功响应 (实际 Raft 投票逻辑待后续集成)
        Ok(VoteResponse {
            throttle_time_ms: 0,
            error_code: KafkaErrorCode::None,
            voter_id: self.broker_id,
            vote_epoch: request.candidate_epoch,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vote_handler() {
        let handler = VoteHandler::new(1);
        let req = VoteRequest {
            cluster_id: "cluster-1".to_string(),
            candidate_id: 2,
            candidate_epoch: 5,
            last_offset: 100,
        };
        let resp = handler.handle(req, 0).unwrap();
        assert_eq!(resp.error_code, KafkaErrorCode::None);
        assert_eq!(resp.voter_id, 1);
        assert_eq!(resp.vote_epoch, 5);
    }
}
