//! EndQuorumEpoch API (Key = 53)
//!
//! KRaft 纪元结束: Leader 通知 Follower 当前纪元结束。

use rk_core::error::Result;
use crate::codec::{KafkaRequestDecoder, KafkaResponseEncoder};
use crate::types::{KafkaReader, KafkaWriter};
use crate::error_codes::KafkaErrorCode;

// ─── Request ──────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct EndQuorumEpochRequest {
    /// 集群 ID
    pub cluster_id: String,
    /// Leader Broker ID
    pub leader_id: i32,
    /// Leader 纪元
    pub leader_epoch: i32,
}

impl KafkaRequestDecoder for EndQuorumEpochRequest {
    fn decode(reader: &mut KafkaReader<'_>, version: i16) -> Result<Self> {
        if version >= 0 {
            let cluster_id = reader.read_compact_string()?;
            let leader_id = reader.read_i32()?;
            let leader_epoch = reader.read_i32()?;
            let _tags = reader.read_tagged_fields()?;
            Ok(Self { cluster_id, leader_id, leader_epoch })
        } else {
            Err(rk_core::error::RkError::Protocol(
                format!("Unsupported EndQuorumEpoch API version: {}", version),
            ))
        }
    }
}

// ─── Response ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct EndQuorumEpochResponse {
    pub throttle_time_ms: i32,
    pub error_code: KafkaErrorCode,
    /// 响应者 ID
    pub voter_id: i32,
    /// 响应者当前纪元
    pub voter_epoch: i32,
}

impl KafkaResponseEncoder for EndQuorumEpochResponse {
    fn encode(&self, writer: &mut KafkaWriter<'_>, _version: i16) -> Result<()> {
        writer.write_i32(self.throttle_time_ms);
        writer.write_i16(self.error_code.as_i16());
        writer.write_i32(self.voter_id);
        writer.write_i32(self.voter_epoch);
        writer.write_tagged_fields(&[]);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_end_quorum_request_decode() {
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        w.write_compact_string("cluster-1");
        w.write_i32(1);       // leader_id
        w.write_i32(10);      // leader_epoch
        w.write_tagged_fields(&[]);

        let mut reader = KafkaReader::new(&buf);
        let req = EndQuorumEpochRequest::decode(&mut reader, 0).unwrap();
        assert_eq!(req.cluster_id, "cluster-1");
        assert_eq!(req.leader_id, 1);
        assert_eq!(req.leader_epoch, 10);
    }

    #[test]
    fn test_end_quorum_response_encode() {
        let resp = EndQuorumEpochResponse {
            throttle_time_ms: 0,
            error_code: KafkaErrorCode::None,
            voter_id: 3,
            voter_epoch: 10,
        };
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        resp.encode(&mut w, 0).unwrap();
        // throttle(4) + error(2) + voter_id(4) + voter_epoch(4) + tagged(1) = 15
        assert_eq!(buf.len(), 15);
    }
}
