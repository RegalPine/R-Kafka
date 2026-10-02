//! StopReplica API (Key = 5)
//!
//! Controller 发送: 停止指定副本。

use crate::codec::{KafkaRequestDecoder, KafkaResponseEncoder};
use crate::error_codes::KafkaErrorCode;
use crate::types::{KafkaReader, KafkaWriter};
use rk_core::error::Result;

#[derive(Debug, Clone)]
pub struct StopReplicaPartitionInfo {
    pub topic_name: String,
    pub partition_index: i32,
}

#[derive(Debug, Clone)]
pub struct StopReplicaRequest {
    pub controller_id: i32,
    pub controller_epoch: i32,
    pub delete_partitions: bool,
    pub partitions: Vec<StopReplicaPartitionInfo>,
}

impl KafkaRequestDecoder for StopReplicaRequest {
    fn decode(reader: &mut KafkaReader<'_>, _version: i16) -> Result<Self> {
        let controller_id = reader.read_i32()?;
        let controller_epoch = reader.read_i32()?;
        let delete_partitions = reader.read_bool()?;
        let count = reader.read_i32()? as usize;
        let mut partitions = Vec::with_capacity(count);
        for _ in 0..count {
            let topic_name = reader.read_compact_string()?;
            let partition_index = reader.read_i32()?;
            let _tags = reader.read_tagged_fields()?;
            partitions.push(StopReplicaPartitionInfo {
                topic_name,
                partition_index,
            });
        }
        let _tags = reader.read_tagged_fields()?;
        Ok(Self {
            controller_id,
            controller_epoch,
            delete_partitions,
            partitions,
        })
    }
}

#[derive(Debug, Clone)]
pub struct StopReplicaPartitionError {
    pub topic_name: String,
    pub partition_index: i32,
    pub error_code: KafkaErrorCode,
}

#[derive(Debug, Clone)]
pub struct StopReplicaResponse {
    pub throttle_time_ms: i32,
    pub error_code: KafkaErrorCode,
    pub partition_errors: Vec<StopReplicaPartitionError>,
}

impl KafkaResponseEncoder for StopReplicaResponse {
    fn encode(&self, writer: &mut KafkaWriter<'_>, _version: i16) -> Result<()> {
        writer.write_i32(self.throttle_time_ms);
        writer.write_i16(self.error_code.as_i16());
        writer.write_compact_array(&self.partition_errors, |w, pe| {
            w.write_compact_string(&pe.topic_name);
            w.write_i32(pe.partition_index);
            w.write_i16(pe.error_code.as_i16());
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
    fn test_stop_replica_request_decode() {
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        w.write_i32(1); // controller_id
        w.write_i32(3); // controller_epoch
        w.write_bool(true); // delete_partitions
        w.write_i32(1); // 1 partition
        w.write_compact_string("topic-a");
        w.write_i32(0);
        w.write_tagged_fields(&[]);
        w.write_tagged_fields(&[]);

        let mut reader = KafkaReader::new(&buf);
        let req = StopReplicaRequest::decode(&mut reader, 0).unwrap();
        assert_eq!(req.controller_id, 1);
        assert!(req.delete_partitions);
        assert_eq!(req.partitions.len(), 1);
    }

    #[test]
    fn test_stop_replica_response_encode() {
        let resp = StopReplicaResponse {
            throttle_time_ms: 0,
            error_code: KafkaErrorCode::None,
            partition_errors: vec![],
        };
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        resp.encode(&mut w, 0).unwrap();
        assert!(buf.len() > 0);
    }
}
