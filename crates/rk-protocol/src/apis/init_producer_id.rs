//! InitProducerId API (Key = 22)
//!
//! 幂等生产者: 获取 Producer ID 和 Epoch。
//! Phase 1: 简化实现，直接分配递增 PID。

use crate::codec::{KafkaRequestDecoder, KafkaResponseEncoder};
use crate::error_codes::KafkaErrorCode;
use crate::types::{KafkaReader, KafkaWriter};
use rk_core::error::Result;

// ─── Request ──────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct InitProducerIdRequest {
    /// nullable_string: null = 非事务生产者
    pub transactional_id: Option<String>,
    /// v0+: transaction_timeout_ms (i32)
    pub transaction_timeout_ms: i32,
    /// v3+: producer_id (i64), 用于重试时保持 PID
    pub producer_id: i64,
    /// v3+: producer_epoch (i16)
    pub producer_epoch: i16,
}

impl KafkaRequestDecoder for InitProducerIdRequest {
    fn decode(reader: &mut KafkaReader<'_>, version: i16) -> Result<Self> {
        if version >= 3 {
            // Flexible
            let transactional_id = reader.read_compact_nullable_string()?;
            let transaction_timeout_ms = reader.read_i32()?;
            let producer_id = reader.read_i64()?;
            let producer_epoch = reader.read_i16()?;
            let _tags = reader.read_tagged_fields()?;
            Ok(Self {
                transactional_id,
                transaction_timeout_ms,
                producer_id,
                producer_epoch,
            })
        } else {
            // Legacy v0-v2
            let transactional_id = reader.read_nullable_string()?;
            let transaction_timeout_ms = reader.read_i32()?;
            Ok(Self {
                transactional_id,
                transaction_timeout_ms,
                producer_id: -1,
                producer_epoch: -1,
            })
        }
    }
}

// ─── Response ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct InitProducerIdResponse {
    pub throttle_time_ms: i32,
    pub error_code: KafkaErrorCode,
    pub producer_id: i64,
    pub producer_epoch: i16,
}

impl KafkaResponseEncoder for InitProducerIdResponse {
    fn encode(&self, writer: &mut KafkaWriter<'_>, version: i16) -> Result<()> {
        writer.write_i32(self.throttle_time_ms);
        writer.write_i16(self.error_code.as_i16());
        if version >= 3 {
            // Flexible
            writer.write_i64(self.producer_id);
            writer.write_i16(self.producer_epoch);
            writer.write_tagged_fields(&[]);
        } else {
            writer.write_i64(self.producer_id);
            writer.write_i16(self.producer_epoch);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_init_producer_id_response_encode_v0() {
        let resp = InitProducerIdResponse {
            throttle_time_ms: 0,
            error_code: KafkaErrorCode::None,
            producer_id: 1000,
            producer_epoch: 0,
        };
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        resp.encode(&mut w, 0).unwrap();
        // throttle(4) + error(2) + pid(8) + epoch(2) = 16
        assert_eq!(buf.len(), 16);
    }

    #[test]
    fn test_init_producer_id_request_decode_v0() {
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        // transactional_id: null → -1
        w.write_i16(-1);
        // transaction_timeout_ms
        w.write_i32(30000);
        let mut r = KafkaReader::new(&buf);
        let req = InitProducerIdRequest::decode(&mut r, 0).unwrap();
        assert_eq!(req.transactional_id, None);
        assert_eq!(req.transaction_timeout_ms, 30000);
    }
}
