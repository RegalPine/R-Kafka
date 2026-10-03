//! AddOffsetsToTxn API (Key = 25)
//!
//! 将消费者组协调器加入事务，允许事务性地提交消费者偏移量。
//! v0-v2: 传统格式
//! v3+: Flexible 格式

use crate::codec::{KafkaRequestDecoder, KafkaResponseEncoder};
use crate::error_codes::KafkaErrorCode;
use crate::types::{KafkaReader, KafkaWriter};
use rk_core::error::Result;

// ─── Request ──────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct AddOffsetsToTxnRequest {
    /// 事务 ID
    pub transactional_id: String,
    /// 生产者 ID
    pub producer_id: i64,
    /// 生产者 Epoch
    pub producer_epoch: i16,
    /// 消费者组 ID
    pub group_id: String,
}

impl KafkaRequestDecoder for AddOffsetsToTxnRequest {
    fn decode(reader: &mut KafkaReader<'_>, version: i16) -> Result<Self> {
        if version >= 3 {
            // Flexible v3+
            let transactional_id = reader.read_compact_string()?;
            let producer_id = reader.read_i64()?;
            let producer_epoch = reader.read_i16()?;
            let group_id = reader.read_compact_string()?;
            let _tags = reader.read_tagged_fields()?;
            Ok(Self {
                transactional_id,
                producer_id,
                producer_epoch,
                group_id,
            })
        } else {
            // Legacy v0-v2
            let transactional_id = reader.read_string()?;
            let producer_id = reader.read_i64()?;
            let producer_epoch = reader.read_i16()?;
            let group_id = reader.read_string()?;
            Ok(Self {
                transactional_id,
                producer_id,
                producer_epoch,
                group_id,
            })
        }
    }
}

// ─── Response ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct AddOffsetsToTxnResponse {
    pub throttle_time_ms: i32,
    pub error_code: KafkaErrorCode,
}

impl KafkaResponseEncoder for AddOffsetsToTxnResponse {
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
    fn test_add_offsets_to_txn_response_encode_v0() {
        let resp = AddOffsetsToTxnResponse {
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
    fn test_add_offsets_to_txn_response_encode_v3_flexible() {
        let resp = AddOffsetsToTxnResponse {
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
    fn test_add_offsets_to_txn_request_decode_v0() {
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        w.write_string("txn-1");
        w.write_i64(1000);
        w.write_i16(0);
        w.write_string("my-group");

        let mut reader = KafkaReader::new(&buf);
        let req = AddOffsetsToTxnRequest::decode(&mut reader, 0).unwrap();
        assert_eq!(req.transactional_id, "txn-1");
        assert_eq!(req.producer_id, 1000);
        assert_eq!(req.producer_epoch, 0);
        assert_eq!(req.group_id, "my-group");
    }

    #[test]
    fn test_add_offsets_to_txn_request_decode_v3() {
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        w.write_compact_string("txn-2");
        w.write_i64(2000);
        w.write_i16(1);
        w.write_compact_string("consumer-group");
        w.write_tagged_fields(&[]);

        let mut reader = KafkaReader::new(&buf);
        let req = AddOffsetsToTxnRequest::decode(&mut reader, 3).unwrap();
        assert_eq!(req.transactional_id, "txn-2");
        assert_eq!(req.producer_id, 2000);
        assert_eq!(req.producer_epoch, 1);
        assert_eq!(req.group_id, "consumer-group");
    }
}
