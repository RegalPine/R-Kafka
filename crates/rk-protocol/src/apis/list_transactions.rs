//! ListTransactions API (Key = 65)
//!
//! 列出当前活跃的事务。
//! KIP-664, v0+ 全部为 Flexible 格式。

use rk_core::error::Result;
use crate::codec::{KafkaRequestDecoder, KafkaResponseEncoder};
use crate::types::{KafkaReader, KafkaWriter};
use crate::error_codes::KafkaErrorCode;

// ─── Request ──────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ListTransactionsRequest {
    /// 按 transactional_id 前缀过滤 (空 = 不过滤)
    pub transactional_id_prefixes: Vec<String>,
    /// 按状态过滤 (空 = 不过滤): "Ongoing", "Dead", "PrepareCommit", "PrepareAbort"
    pub states: Vec<String>,
}

impl KafkaRequestDecoder for ListTransactionsRequest {
    fn decode(reader: &mut KafkaReader<'_>, _version: i16) -> Result<Self> {
        // v0+ is always flexible
        let transactional_id_prefixes = reader.read_compact_array(|r| r.read_compact_string())?;
        let states = reader.read_compact_array(|r| r.read_compact_string())?;
        let _tags = reader.read_tagged_fields();
        Ok(Self { transactional_id_prefixes, states })
    }
}

impl KafkaResponseEncoder for ListTransactionsRequest {
    fn encode(&self, _writer: &mut KafkaWriter<'_>, _version: i16) -> Result<()> {
        Ok(()) // Request only
    }
}

// ─── Response ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ListTransactionsResponse {
    pub throttle_time_ms: i32,
    pub error_code: KafkaErrorCode,
    pub error_message: Option<String>,
    pub transaction_states: Vec<ListTransactionsResponseState>,
}

#[derive(Debug, Clone)]
pub struct ListTransactionsResponseState {
    pub transactional_id: String,
    pub producer_id: i64,
    pub transaction_state: String,
    pub transaction_timeout_ms: i32,
    pub transaction_start_time_ms: i64,
}

impl KafkaResponseEncoder for ListTransactionsResponse {
    fn encode(&self, writer: &mut KafkaWriter<'_>, _version: i16) -> Result<()> {
        // Always flexible
        writer.write_i32(self.throttle_time_ms);
        writer.write_i16(self.error_code as i16);
        writer.write_compact_nullable_string(self.error_message.as_deref());
        writer.write_compact_array(&self.transaction_states, |w, state| {
            w.write_compact_string(&state.transactional_id);
            w.write_i64(state.producer_id);
            w.write_compact_string(&state.transaction_state);
            w.write_i32(state.transaction_timeout_ms);
            w.write_i64(state.transaction_start_time_ms);
            w.write_tagged_fields(&[]);
        });
        writer.write_tagged_fields(&[]);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_list_transactions_encode() {
        let resp = ListTransactionsResponse {
            throttle_time_ms: 0,
            error_code: KafkaErrorCode::None,
            error_message: None,
            transaction_states: vec![ListTransactionsResponseState {
                transactional_id: "txn-1".to_string(),
                producer_id: 1000,
                transaction_state: "Ongoing".to_string(),
                transaction_timeout_ms: 60000,
                transaction_start_time_ms: 1234567890,
            }],
        };
        let mut buf = BytesMut::new();
        let mut writer = KafkaWriter::new(&mut buf);
        resp.encode(&mut writer, 0).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_list_transactions_empty() {
        let resp = ListTransactionsResponse {
            throttle_time_ms: 0,
            error_code: KafkaErrorCode::None,
            error_message: None,
            transaction_states: vec![],
        };
        let mut buf = BytesMut::new();
        let mut writer = KafkaWriter::new(&mut buf);
        resp.encode(&mut writer, 0).unwrap();
        assert!(!buf.is_empty());
    }
}
