//! EndTxn API (Key = 26)
//!
//! 结束事务: 提交或中止。

use crate::codec::{KafkaRequestDecoder, KafkaResponseEncoder};
use crate::error_codes::KafkaErrorCode;
use crate::types::{KafkaReader, KafkaWriter};
use rk_core::error::Result;

// ─── Request ──────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct EndTxnRequest {
    pub transactional_id: String,
    pub producer_id: i64,
    pub producer_epoch: i16,
    /// true = 提交, false = 中止
    pub committed: bool,
}

impl KafkaRequestDecoder for EndTxnRequest {
    fn decode(reader: &mut KafkaReader<'_>, version: i16) -> Result<Self> {
        if version >= 3 {
            // Flexible v3+
            let transactional_id = reader.read_compact_string()?;
            let producer_id = reader.read_i64()?;
            let producer_epoch = reader.read_i16()?;
            let committed = reader.read_bool()?;
            let _tags = reader.read_tagged_fields()?;
            Ok(Self {
                transactional_id,
                producer_id,
                producer_epoch,
                committed,
            })
        } else {
            // Legacy v0-v2
            let transactional_id = reader.read_string()?;
            let producer_id = reader.read_i64()?;
            let producer_epoch = reader.read_i16()?;
            let committed = reader.read_bool()?;
            Ok(Self {
                transactional_id,
                producer_id,
                producer_epoch,
                committed,
            })
        }
    }
}

// ─── Response ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct EndTxnResponse {
    pub throttle_time_ms: i32,
    pub error_code: KafkaErrorCode,
}

impl KafkaResponseEncoder for EndTxnResponse {
    fn encode(&self, writer: &mut KafkaWriter<'_>, version: i16) -> Result<()> {
        if version >= 3 {
            // Flexible v3+
            writer.write_i32(self.throttle_time_ms);
            writer.write_i16(self.error_code.as_i16());
            writer.write_tagged_fields(&[]);
        } else {
            // Legacy v0-v2
            writer.write_i32(self.throttle_time_ms);
            writer.write_i16(self.error_code.as_i16());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_end_txn_response_encode_v0() {
        let resp = EndTxnResponse {
            throttle_time_ms: 0,
            error_code: KafkaErrorCode::None,
        };
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        resp.encode(&mut w, 0).unwrap();
        // throttle_time_ms(4) + error_code(2) = 6
        assert_eq!(buf.len(), 6);
    }

    #[test]
    fn test_end_txn_response_encode_v3_flexible() {
        let resp = EndTxnResponse {
            throttle_time_ms: 0,
            error_code: KafkaErrorCode::None,
        };
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        resp.encode(&mut w, 3).unwrap();
        // throttle_time_ms(4) + error_code(2) + tagged_fields(1) = 7
        assert_eq!(buf.len(), 7);
    }

    #[test]
    fn test_end_txn_request_decode_v0() {
        // 手动构建 v0 请求字节
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        w.write_string("txn-1");
        w.write_i64(1000);
        w.write_i16(0);
        w.write_bool(true);

        let mut reader = KafkaReader::new(&buf);
        let req = EndTxnRequest::decode(&mut reader, 0).unwrap();
        assert_eq!(req.transactional_id, "txn-1");
        assert_eq!(req.producer_id, 1000);
        assert_eq!(req.producer_epoch, 0);
        assert!(req.committed);
    }
}
