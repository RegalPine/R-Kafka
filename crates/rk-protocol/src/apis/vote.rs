//! Vote API (Key = 51)
//!
//! KRaft 投票请求: 候选人请求其他节点投票。

use rk_core::error::Result;
use crate::codec::{KafkaRequestDecoder, KafkaResponseEncoder};
use crate::types::{KafkaReader, KafkaWriter};
use crate::error_codes::KafkaErrorCode;

// ─── Request ──────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct VoteRequest {
    /// 候选人的集群 ID
    pub cluster_id: String,
    /// 候选人 Broker ID
    pub candidate_id: i32,
    /// 候选人的纪元
    pub candidate_epoch: i32,
    /// 候选人最后已知的日志偏移
    pub last_offset: i64,
}

impl KafkaRequestDecoder for VoteRequest {
    fn decode(reader: &mut KafkaReader<'_>, version: i16) -> Result<Self> {
        if version >= 0 {
            // Flexible v0+ (所有版本都是 flexible)
            let cluster_id = reader.read_compact_string()?;
            let candidate_id = reader.read_i32()?;
            let candidate_epoch = reader.read_i32()?;
            let last_offset = reader.read_i64()?;
            let _tags = reader.read_tagged_fields()?;
            Ok(Self { cluster_id, candidate_id, candidate_epoch, last_offset })
        } else {
            Err(rk_core::error::RkError::Protocol(
                format!("Unsupported Vote API version: {}", version),
            ))
        }
    }
}

// ─── Response ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct VoteResponse {
    pub throttle_time_ms: i32,
    pub error_code: KafkaErrorCode,
    /// 投票人 ID
    pub voter_id: i32,
    /// 投票人当前纪元
    pub vote_epoch: i32,
}

impl KafkaResponseEncoder for VoteResponse {
    fn encode(&self, writer: &mut KafkaWriter<'_>, _version: i16) -> Result<()> {
        writer.write_i32(self.throttle_time_ms);
        writer.write_i16(self.error_code.as_i16());
        writer.write_i32(self.voter_id);
        writer.write_i32(self.vote_epoch);
        writer.write_tagged_fields(&[]);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_vote_request_decode_v0() {
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        w.write_compact_string("cluster-1");
        w.write_i32(1);       // candidate_id
        w.write_i32(5);       // candidate_epoch
        w.write_i64(100);     // last_offset
        w.write_tagged_fields(&[]);

        let mut reader = KafkaReader::new(&buf);
        let req = VoteRequest::decode(&mut reader, 0).unwrap();
        assert_eq!(req.cluster_id, "cluster-1");
        assert_eq!(req.candidate_id, 1);
        assert_eq!(req.candidate_epoch, 5);
        assert_eq!(req.last_offset, 100);
    }

    #[test]
    fn test_vote_response_encode_v0() {
        let resp = VoteResponse {
            throttle_time_ms: 0,
            error_code: KafkaErrorCode::None,
            voter_id: 2,
            vote_epoch: 5,
        };
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        resp.encode(&mut w, 0).unwrap();
        // throttle(4) + error(2) + voter_id(4) + vote_epoch(4) + tagged(1) = 15
        assert_eq!(buf.len(), 15);
    }

    #[test]
    fn test_vote_response_encode_error() {
        let resp = VoteResponse {
            throttle_time_ms: 100,
            error_code: KafkaErrorCode::InvalidRequest,
            voter_id: 0,
            vote_epoch: 0,
        };
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        resp.encode(&mut w, 0).unwrap();
        assert!(buf.len() > 0);
    }
}
