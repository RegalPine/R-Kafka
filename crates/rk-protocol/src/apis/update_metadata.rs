//! UpdateMetadata API (Key = 6)
//!
//! Controller 发送: 广播 Metadata 变更给所有 Broker。

use crate::codec::{KafkaRequestDecoder, KafkaResponseEncoder};
use crate::error_codes::KafkaErrorCode;
use crate::types::{KafkaReader, KafkaWriter};
use rk_core::error::Result;

#[derive(Debug, Clone)]
pub struct UpdateMetadataBroker {
    pub broker_id: i32,
    pub host: String,
    pub port: i32,
}

#[derive(Debug, Clone)]
pub struct UpdateMetadataRequest {
    pub controller_id: i32,
    pub controller_epoch: i32,
    pub brokers: Vec<UpdateMetadataBroker>,
}

impl KafkaRequestDecoder for UpdateMetadataRequest {
    fn decode(reader: &mut KafkaReader<'_>, _version: i16) -> Result<Self> {
        let controller_id = reader.read_i32()?;
        let controller_epoch = reader.read_i32()?;
        let broker_count = reader.read_i32()? as usize;
        let mut brokers = Vec::with_capacity(broker_count);
        for _ in 0..broker_count {
            let broker_id = reader.read_i32()?;
            let host = reader.read_compact_string()?;
            let port = reader.read_i32()?;
            let _tags = reader.read_tagged_fields()?;
            brokers.push(UpdateMetadataBroker {
                broker_id,
                host,
                port,
            });
        }
        let _tags = reader.read_tagged_fields()?;
        Ok(Self {
            controller_id,
            controller_epoch,
            brokers,
        })
    }
}

#[derive(Debug, Clone)]
pub struct UpdateMetadataResponse {
    pub throttle_time_ms: i32,
    pub error_code: KafkaErrorCode,
}

impl KafkaResponseEncoder for UpdateMetadataResponse {
    fn encode(&self, writer: &mut KafkaWriter<'_>, _version: i16) -> Result<()> {
        writer.write_i32(self.throttle_time_ms);
        writer.write_i16(self.error_code.as_i16());
        writer.write_tagged_fields(&[]);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_update_metadata_request_decode() {
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        w.write_i32(1); // controller_id
        w.write_i32(3); // controller_epoch
        w.write_i32(2); // 2 brokers
        w.write_i32(1);
        w.write_compact_string("host1");
        w.write_i32(9092);
        w.write_tagged_fields(&[]);
        w.write_i32(2);
        w.write_compact_string("host2");
        w.write_i32(9093);
        w.write_tagged_fields(&[]);
        w.write_tagged_fields(&[]);

        let mut reader = KafkaReader::new(&buf);
        let req = UpdateMetadataRequest::decode(&mut reader, 0).unwrap();
        assert_eq!(req.controller_id, 1);
        assert_eq!(req.brokers.len(), 2);
        assert_eq!(req.brokers[0].host, "host1");
    }

    #[test]
    fn test_update_metadata_response_encode() {
        let resp = UpdateMetadataResponse {
            throttle_time_ms: 0,
            error_code: KafkaErrorCode::None,
        };
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        resp.encode(&mut w, 0).unwrap();
        // throttle(4) + error(2) + tagged(1) = 7
        assert_eq!(buf.len(), 7);
    }
}
