//! ControlledShutdown API (Key = 7)
//!
//! Broker 优雅关闭前: 请求 Controller 迁移 Partition。

use rk_core::error::Result;
use crate::codec::{KafkaRequestDecoder, KafkaResponseEncoder};
use crate::types::{KafkaReader, KafkaWriter};
use crate::error_codes::KafkaErrorCode;

#[derive(Debug, Clone)]
pub struct ControlledShutdownRequest {
    pub broker_id: i32,
    pub broker_epoch: i64,
}

impl KafkaRequestDecoder for ControlledShutdownRequest {
    fn decode(reader: &mut KafkaReader<'_>, _version: i16) -> Result<Self> {
        let broker_id = reader.read_i32()?;
        let broker_epoch = reader.read_i64()?;
        let _tags = reader.read_tagged_fields()?;
        Ok(Self { broker_id, broker_epoch })
    }
}

#[derive(Debug, Clone)]
pub struct ControlledShutdownResponse {
    pub throttle_time_ms: i32,
    pub error_code: KafkaErrorCode,
    pub remaining_partitions: Vec<(String, i32)>,
}

impl KafkaResponseEncoder for ControlledShutdownResponse {
    fn encode(&self, writer: &mut KafkaWriter<'_>, _version: i16) -> Result<()> {
        writer.write_i32(self.throttle_time_ms);
        writer.write_i16(self.error_code.as_i16());
        writer.write_compact_array(&self.remaining_partitions, |w, (topic, partition)| {
            w.write_compact_string(topic);
            w.write_i32(*partition);
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
    fn test_controlled_shutdown_request_decode() {
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        w.write_i32(1);
        w.write_i64(42);
        w.write_tagged_fields(&[]);

        let mut reader = KafkaReader::new(&buf);
        let req = ControlledShutdownRequest::decode(&mut reader, 0).unwrap();
        assert_eq!(req.broker_id, 1);
        assert_eq!(req.broker_epoch, 42);
    }

    #[test]
    fn test_controlled_shutdown_response_encode() {
        let resp = ControlledShutdownResponse {
            throttle_time_ms: 0,
            error_code: KafkaErrorCode::None,
            remaining_partitions: vec![
                ("topic-a".to_string(), 0),
                ("topic-b".to_string(), 1),
            ],
        };
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        resp.encode(&mut w, 0).unwrap();
        assert!(buf.len() > 0);
    }
}
