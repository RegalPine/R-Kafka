//! AddPartitionsToTxn API (Key = 24)
//!
//! 将分区添加到事务中。
//! v0-v2: 单事务，一次一个 transactional_id
//! v3+: 批量事务

use crate::codec::{KafkaRequestDecoder, KafkaResponseEncoder};
use crate::error_codes::KafkaErrorCode;
use crate::types::{KafkaReader, KafkaWriter};
use rk_core::error::Result;

// ─── Request ──────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct AddPartitionsToTxnRequest {
    /// v0-v2: 单个 transactional_id; v3+: nullable
    pub transactional_id: Option<String>,
    /// v0-v2: producer_id (i64)
    pub producer_id: i64,
    /// v0-v2: producer_epoch (i16)
    pub producer_epoch: i16,
    /// v0-v2: topics array
    pub topics: Vec<AddPartitionsToTxnRequestTopic>,
    /// v3+: 批量事务
    pub transactions: Option<Vec<AddPartitionsToTxnRequestTransaction>>,
}

#[derive(Debug, Clone)]
pub struct AddPartitionsToTxnRequestTopic {
    pub topic: String,
    pub partitions: Vec<i32>,
}

#[derive(Debug, Clone)]
pub struct AddPartitionsToTxnRequestTransaction {
    pub transactional_id: String,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub topics: Vec<AddPartitionsToTxnRequestTopic>,
}

impl KafkaRequestDecoder for AddPartitionsToTxnRequest {
    fn decode(reader: &mut KafkaReader<'_>, version: i16) -> Result<Self> {
        if version >= 3 {
            // Flexible v3+
            let _tags = reader.read_tagged_fields()?;
            // v3+: 批量模式
            let transactions = reader.read_compact_array(|r| {
                let transactional_id = r.read_compact_string()?;
                let producer_id = r.read_i64()?;
                let producer_epoch = r.read_i16()?;
                let _tags = r.read_tagged_fields()?;
                let topics = r.read_compact_array(|r| {
                    let topic = r.read_compact_string()?;
                    let partitions = r.read_compact_array(|r| r.read_i32())?;
                    let _tags = r.read_tagged_fields()?;
                    Ok(AddPartitionsToTxnRequestTopic { topic, partitions })
                })?;
                Ok(AddPartitionsToTxnRequestTransaction {
                    transactional_id,
                    producer_id,
                    producer_epoch,
                    topics,
                })
            })?;
            Ok(Self {
                transactional_id: None,
                producer_id: -1,
                producer_epoch: -1,
                topics: vec![],
                transactions: Some(transactions),
            })
        } else {
            // Legacy v0-v2
            let transactional_id = reader.read_string()?;
            let producer_id = reader.read_i64()?;
            let producer_epoch = reader.read_i16()?;
            let topics = reader.read_array(|r| {
                let topic = r.read_string()?;
                let partitions = r.read_array(|r| r.read_i32())?;
                Ok(AddPartitionsToTxnRequestTopic { topic, partitions })
            })?;
            Ok(Self {
                transactional_id: Some(transactional_id),
                producer_id,
                producer_epoch,
                topics,
                transactions: None,
            })
        }
    }
}

// ─── Response ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct AddPartitionsToTxnResponse {
    /// v0-v2: 单个结果
    pub results_by_topic: Vec<AddPartitionsToTxnResponseTopic>,
    /// v3+: 批量结果
    pub results_by_transaction: Option<Vec<AddPartitionsToTxnResponseTransaction>>,
}

#[derive(Debug, Clone)]
pub struct AddPartitionsToTxnResponseTopic {
    pub topic: String,
    pub results_by_partition: Vec<AddPartitionsToTxnResponsePartition>,
}

#[derive(Debug, Clone)]
pub struct AddPartitionsToTxnResponsePartition {
    pub partition_index: i32,
    pub error_code: KafkaErrorCode,
}

#[derive(Debug, Clone)]
pub struct AddPartitionsToTxnResponseTransaction {
    pub transactional_id: String,
    pub topic_results: Vec<AddPartitionsToTxnResponseTopic>,
}

impl KafkaResponseEncoder for AddPartitionsToTxnResponse {
    fn encode(&self, writer: &mut KafkaWriter<'_>, version: i16) -> Result<()> {
        if version >= 3 {
            // Flexible v3+
            if let Some(ref txns) = self.results_by_transaction {
                writer.write_compact_array(txns, |w, txn| {
                    w.write_compact_string(&txn.transactional_id);
                    w.write_compact_array(&txn.topic_results, |w, topic| {
                        w.write_compact_string(&topic.topic);
                        w.write_compact_array(&topic.results_by_partition, |w, part| {
                            w.write_i32(part.partition_index);
                            w.write_i16(part.error_code.as_i16());
                            w.write_tagged_fields(&[]);
                        });
                        w.write_tagged_fields(&[]);
                    });
                    w.write_tagged_fields(&[]);
                });
            } else {
                let empty: &[AddPartitionsToTxnResponseTransaction] = &[];
                writer.write_compact_array(empty, |_, _| {});
            }
            writer.write_tagged_fields(&[]);
        } else {
            // Legacy v0-v2
            writer.write_array(&self.results_by_topic, |w, topic| {
                w.write_string(&topic.topic);
                w.write_array(&topic.results_by_partition, |w, part| {
                    w.write_i32(part.partition_index);
                    w.write_i16(part.error_code.as_i16());
                });
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_add_partitions_to_txn_response_encode_v0() {
        let resp = AddPartitionsToTxnResponse {
            results_by_topic: vec![AddPartitionsToTxnResponseTopic {
                topic: "test".to_string(),
                results_by_partition: vec![AddPartitionsToTxnResponsePartition {
                    partition_index: 0,
                    error_code: KafkaErrorCode::None,
                }],
            }],
            results_by_transaction: None,
        };
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        resp.encode(&mut w, 0).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_add_partitions_to_txn_response_encode_v3() {
        let resp = AddPartitionsToTxnResponse {
            results_by_topic: vec![],
            results_by_transaction: Some(vec![AddPartitionsToTxnResponseTransaction {
                transactional_id: "txn-1".to_string(),
                topic_results: vec![AddPartitionsToTxnResponseTopic {
                    topic: "test".to_string(),
                    results_by_partition: vec![AddPartitionsToTxnResponsePartition {
                        partition_index: 0,
                        error_code: KafkaErrorCode::None,
                    }],
                }],
            }]),
        };
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        resp.encode(&mut w, 3).unwrap();
        assert!(!buf.is_empty());
    }
}
