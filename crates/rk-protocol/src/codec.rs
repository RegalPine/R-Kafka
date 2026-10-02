//! Kafka 协议编解码 Trait
//!
//! 所有 Kafka 请求/响应类型实现这些 trait。

use bytes::BytesMut;
use rk_core::error::Result;

use crate::types::{KafkaReader, KafkaWriter};

/// Kafka 请求体解码
pub trait KafkaRequestDecoder: Sized {
    fn decode(reader: &mut KafkaReader<'_>, version: i16) -> Result<Self>;
}

/// Kafka 响应体编码
pub trait KafkaResponseEncoder {
    fn encode(&self, writer: &mut KafkaWriter<'_>, version: i16) -> Result<()>;
}

/// 将响应编码为 Bytes
pub fn encode_response(resp: &dyn KafkaResponseEncoder, version: i16) -> Result<BytesMut> {
    let mut buf = BytesMut::with_capacity(256);
    let mut writer = KafkaWriter::new(&mut buf);
    resp.encode(&mut writer, version)?;
    Ok(buf)
}
