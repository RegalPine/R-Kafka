//! LeaderAndIsr API (Key = 4)
//!
//! Controller 发送: 更新 Partition 的 Leader 和 ISR 列表。

use crate::codec::{KafkaRequestDecoder, KafkaResponseEncoder};
use crate::error_codes::KafkaErrorCode;
use crate::types::{KafkaReader, KafkaWriter};
use rk_core::error::Result;

#[derive(Debug, Clone)]
pub struct LeaderAndIsrPartitionState {
    pub topic_name: String,
    pub partition_index: i32,
    pub controller_epoch: i32,
    pub leader: i32,
    pub leader_epoch: i32,
    pub isr: Vec<i32>,
    pub partition_epoch: i32,
    pub replicas: Vec<i32>,
    pub adding_replicas: Vec<i32>,
    pub removing_replicas: Vec<i32>,
}

#[derive(Debug, Clone)]
pub struct LeaderAndIsrRequest {
    pub controller_id: i32,
    pub controller_epoch: i32,
    pub partition_states: Vec<LeaderAndIsrPartitionState>,
}

impl KafkaRequestDecoder for LeaderAndIsrRequest {
    fn decode(reader: &mut KafkaReader<'_>, _version: i16) -> Result<Self> {
        let controller_id = reader.read_i32()?;
        let controller_epoch = reader.read_i32()?;
        let count = reader.read_i32()? as usize;
        let mut partition_states = Vec::with_capacity(count);
        for _ in 0..count {
            let topic_name = reader.read_compact_string()?;
            let partition_index = reader.read_i32()?;
            let controller_epoch = reader.read_i32()?;
            let leader = reader.read_i32()?;
            let leader_epoch = reader.read_i32()?;
            let isr = reader.read_compact_array(|r| r.read_i32())?;
            let partition_epoch = reader.read_i32()?;
            let replicas = reader.read_compact_array(|r| r.read_i32())?;
            let adding_replicas = reader.read_compact_array(|r| r.read_i32())?;
            let removing_replicas = reader.read_compact_array(|r| r.read_i32())?;
            let _tags = reader.read_tagged_fields()?;
            partition_states.push(LeaderAndIsrPartitionState {
                topic_name,
                partition_index,
                controller_epoch,
                leader,
                leader_epoch,
                isr,
                partition_epoch,
                replicas,
                adding_replicas,
                removing_replicas,
            });
        }
        let _tags = reader.read_tagged_fields()?;
        Ok(Self {
            controller_id,
            controller_epoch,
            partition_states,
        })
    }
}

#[derive(Debug, Clone)]
pub struct LeaderAndIsrPartitionError {
    pub topic_name: String,
    pub partition_index: i32,
    pub error_code: KafkaErrorCode,
}

#[derive(Debug, Clone)]
pub struct LeaderAndIsrResponse {
    pub throttle_time_ms: i32,
    pub error_code: KafkaErrorCode,
    pub partition_errors: Vec<LeaderAndIsrPartitionError>,
}

impl KafkaResponseEncoder for LeaderAndIsrResponse {
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
    fn test_leader_and_isr_request_decode() {
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        w.write_i32(1); // controller_id
        w.write_i32(5); // controller_epoch
        w.write_i32(1); // 1 partition (non-compact count)
                        // partition state
        w.write_compact_string("test-topic");
        w.write_i32(0); // partition_index
        w.write_i32(5); // controller_epoch
        w.write_i32(1); // leader
        w.write_i32(3); // leader_epoch
        w.write_compact_array(&[1i32, 2, 3], |w2, &v| w2.write_i32(v)); // isr
        w.write_i32(1); // partition_epoch
        w.write_compact_array(&[1i32, 2, 3], |w2, &v| w2.write_i32(v)); // replicas
        w.write_compact_array(&[] as &[i32], |w2: &mut KafkaWriter<'_>, &v: &i32| {
            w2.write_i32(v)
        }); // adding
        w.write_compact_array(&[] as &[i32], |w2: &mut KafkaWriter<'_>, &v: &i32| {
            w2.write_i32(v)
        }); // removing
        w.write_tagged_fields(&[]);
        w.write_tagged_fields(&[]);

        let mut reader = KafkaReader::new(&buf);
        let req = LeaderAndIsrRequest::decode(&mut reader, 0).unwrap();
        assert_eq!(req.controller_id, 1);
        assert_eq!(req.controller_epoch, 5);
        assert_eq!(req.partition_states.len(), 1);
        assert_eq!(req.partition_states[0].topic_name, "test-topic");
        assert_eq!(req.partition_states[0].leader, 1);
        assert_eq!(req.partition_states[0].isr, vec![1, 2, 3]);
    }

    #[test]
    fn test_leader_and_isr_response_encode() {
        let resp = LeaderAndIsrResponse {
            throttle_time_ms: 0,
            error_code: KafkaErrorCode::None,
            partition_errors: vec![LeaderAndIsrPartitionError {
                topic_name: "test".to_string(),
                partition_index: 0,
                error_code: KafkaErrorCode::None,
            }],
        };
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        resp.encode(&mut w, 0).unwrap();
        assert!(buf.len() > 0);
    }
}
