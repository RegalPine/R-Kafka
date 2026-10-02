//! Legacy MessageSet v0/v1 向后兼容
//!
//! 解析 Kafka 0.11 之前的旧消息格式 (MessageSet v0/v1)，
//! 自动转换为 v2 RecordBatch 格式落盘。
//!
//! ## MessageSet 格式
//!
//! ```text
//! MessageSet:
//!   offset        (i64)   8 bytes
//!   message_size  (i32)   4 bytes
//!   message       (...)   variable
//!
//! Message (v0, magic=0):
//!   crc           (i32)   4 bytes  (CRC32 of attributes..value)
//!   magic         (i8)    1 byte   (= 0)
//!   attributes    (i8)    1 byte
//!   key           (bytes) variable (i32 length prefix, -1 = null)
//!   value         (bytes) variable (i32 length prefix, -1 = null)
//!
//! Message (v1, magic=1):
//!   crc           (i32)   4 bytes
//!   magic         (i8)    1 byte   (= 1)
//!   attributes    (i8)    1 byte
//!   timestamp     (i64)   8 bytes
//!   key           (bytes) variable
//!   value         (bytes) variable
//! ```

use crate::record::{CompressionType, RECORDBATCH_MAGIC};
use crate::types::KafkaReader;
use rk_core::error::{Result, RkError};

/// 旧格式单条 Message
#[derive(Debug, Clone)]
pub struct LegacyMessage {
    pub offset: i64,
    pub magic: i8,
    pub attributes: i8,
    pub timestamp_ms: i64,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
}

/// 解析后的 MessageSet (可能包含多条 Message)
#[derive(Debug, Clone)]
pub struct LegacyMessageSet {
    pub messages: Vec<LegacyMessage>,
}

/// 解析 MessageSet (v0/v1)
///
/// 从 KafkaReader 中读取完整的 MessageSet，返回解析后的消息列表。
pub fn decode_message_set(reader: &mut KafkaReader) -> Result<LegacyMessageSet> {
    let mut messages = Vec::new();

    while reader.remaining() > 0 {
        // 至少需要 12 字节 (offset 8 + message_size 4)
        if reader.remaining() < 12 {
            break;
        }

        let offset = reader.read_i64()?;
        let message_size = reader.read_i32()? as usize;

        if reader.remaining() < message_size {
            break;
        }

        // 读取 message 内容
        let msg_start = reader.position();
        let _crc = reader.read_i32()?;
        let magic = reader.read_i8()?;

        match magic {
            0 => {
                let msg = decode_v0_message(reader, offset)?;
                messages.push(msg);
            }
            1 => {
                let msg = decode_v1_message(reader, offset)?;
                messages.push(msg);
            }
            2 => {
                return Err(RkError::Protocol(
                    "v2 RecordBatch should not be decoded as legacy MessageSet".to_string(),
                ));
            }
            _ => {
                return Err(RkError::Protocol(format!("Unknown message magic: {magic}")));
            }
        }

        // 确保跳到 message 末尾
        let consumed = reader.position() - msg_start;
        if consumed < message_size {
            reader.advance(message_size - consumed)?;
        }
    }

    Ok(LegacyMessageSet { messages })
}

/// 解码 v0 Message (magic=0, 无 timestamp)
fn decode_v0_message(reader: &mut KafkaReader, offset: i64) -> Result<LegacyMessage> {
    let attributes = reader.read_i8()?;
    let key = read_legacy_nullable_bytes(reader)?;
    let value = read_legacy_nullable_bytes(reader)?;

    Ok(LegacyMessage {
        offset,
        magic: 0,
        attributes,
        timestamp_ms: 0,
        key,
        value,
    })
}

/// 解码 v1 Message (magic=1, 有 timestamp)
fn decode_v1_message(reader: &mut KafkaReader, offset: i64) -> Result<LegacyMessage> {
    let attributes = reader.read_i8()?;
    let timestamp_ms = reader.read_i64()?;
    let key = read_legacy_nullable_bytes(reader)?;
    let value = read_legacy_nullable_bytes(reader)?;

    Ok(LegacyMessage {
        offset,
        magic: 1,
        attributes,
        timestamp_ms,
        key,
        value,
    })
}

/// 读取旧格式可空 bytes (i32 长度前缀, -1 = null)
///
/// 注意: KafkaReader::read_bytes() 已自带 i32 长度前缀读取，
/// 但返回空 Vec 而非 None。这里用 Option 区分 null。
fn read_legacy_nullable_bytes(reader: &mut KafkaReader) -> Result<Option<Vec<u8>>> {
    let len = reader.read_i32()?;
    if len < 0 {
        return Ok(None);
    }
    let data = reader.advance(len as usize)?;
    Ok(Some(data.to_vec()))
}

/// 将 Legacy MessageSet 转换为 v2 RecordBatch 字节
///
/// 将多条旧格式 Message 合并为一个 v2 RecordBatch。
pub fn convert_to_v2_batch(messages: &LegacyMessageSet) -> Result<Vec<u8>> {
    if messages.messages.is_empty() {
        return Ok(Vec::new());
    }

    let first = &messages.messages[0];
    let base_offset = first.offset;

    // 计算时间戳范围
    let mut base_timestamp = i64::MAX;
    let mut max_timestamp = i64::MIN;
    for msg in &messages.messages {
        let ts = if msg.timestamp_ms == 0 {
            0
        } else {
            msg.timestamp_ms
        };
        base_timestamp = base_timestamp.min(ts);
        max_timestamp = max_timestamp.max(ts);
    }
    if base_timestamp == i64::MAX {
        base_timestamp = 0;
    }
    if max_timestamp == i64::MIN {
        max_timestamp = 0;
    }

    let compression = CompressionType::from_attributes(first.attributes as i16);
    let attributes: i16 = compression as i16;
    let last_offset_delta = (messages.messages.len() as i32) - 1;
    let records_count = messages.messages.len() as i32;

    // 编码 records (v2 Record 格式)
    let mut records_data = Vec::new();
    for (i, msg) in messages.messages.iter().enumerate() {
        let record = encode_v2_record(msg, i as i32);
        records_data.extend_from_slice(&record);
    }

    // 构建 v2 RecordBatch 到 Vec<u8>
    let mut buf = Vec::with_capacity(256 + records_data.len());

    // base_offset (i64)
    buf.extend_from_slice(&base_offset.to_be_bytes());
    // batch_length (i32) — 占位
    let batch_length_pos = buf.len();
    buf.extend_from_slice(&0i32.to_be_bytes());
    // partition_leader_epoch (i32)
    buf.extend_from_slice(&0i32.to_be_bytes());
    // magic (i8)
    buf.push(RECORDBATCH_MAGIC as u8);
    // crc (i32) — 占位
    let crc_pos = buf.len();
    buf.extend_from_slice(&0i32.to_be_bytes());
    // attributes (i16)
    buf.extend_from_slice(&attributes.to_be_bytes());
    // last_offset_delta (i32)
    buf.extend_from_slice(&last_offset_delta.to_be_bytes());
    // base_timestamp (i64)
    buf.extend_from_slice(&base_timestamp.to_be_bytes());
    // max_timestamp (i64)
    buf.extend_from_slice(&max_timestamp.to_be_bytes());
    // producer_id (i64)
    buf.extend_from_slice(&(-1i64).to_be_bytes());
    // producer_epoch (i16)
    buf.extend_from_slice(&(-1i16).to_be_bytes());
    // base_sequence (i32)
    buf.extend_from_slice(&(-1i32).to_be_bytes());
    // records_count (i32)
    buf.extend_from_slice(&records_count.to_be_bytes());
    // records data (raw, no length prefix)
    buf.extend_from_slice(&records_data);

    // 回填 batch_length (从 partition_leader_epoch 到末尾)
    let batch_length = (buf.len() - 12) as i32;
    buf[batch_length_pos..batch_length_pos + 4].copy_from_slice(&batch_length.to_be_bytes());

    // 计算 CRC32C (从 magic 到末尾)
    let crc_data = &buf[16..];
    let crc = crc32c::crc32c(crc_data);
    buf[crc_pos..crc_pos + 4].copy_from_slice(&(crc as i32).to_be_bytes());

    Ok(buf)
}

/// 编码单条 v2 Record
fn encode_v2_record(msg: &LegacyMessage, offset_delta: i32) -> Vec<u8> {
    let mut record_buf = Vec::new();

    // attributes (i8)
    record_buf.push(0);
    // timestamp_delta (varint)
    let ts_delta = if msg.timestamp_ms > 0 {
        msg.timestamp_ms
    } else {
        0
    };
    write_unsigned_varint(&mut record_buf, zigzag64(ts_delta));
    // offset_delta (varint)
    write_unsigned_varint(&mut record_buf, zigzag32(offset_delta) as u64);

    // key
    match &msg.key {
        Some(k) => {
            write_unsigned_varint(&mut record_buf, k.len() as u64);
            record_buf.extend_from_slice(k);
        }
        None => {
            write_unsigned_varint(&mut record_buf, zigzag64(-1));
        }
    }

    // value
    match &msg.value {
        Some(v) => {
            write_unsigned_varint(&mut record_buf, v.len() as u64);
            record_buf.extend_from_slice(v);
        }
        None => {
            write_unsigned_varint(&mut record_buf, zigzag64(-1));
        }
    }

    // headers_count = 0
    write_unsigned_varint(&mut record_buf, 0);

    // 前置 record length
    let record_length = record_buf.len() as u64;
    let mut result = Vec::with_capacity(record_buf.len() + 10);
    write_unsigned_varint(&mut result, record_length);
    result.extend_from_slice(&record_buf);

    result
}

/// ZigZag 编码 i32 → u32
fn zigzag32(v: i32) -> u32 {
    ((v << 1) ^ (v >> 31)) as u32
}

/// ZigZag 编码 i64 → u64
fn zigzag64(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

/// 写入 unsigned varint
fn write_unsigned_varint(buf: &mut Vec<u8>, mut value: u64) {
    loop {
        if value & !0x7F == 0 {
            buf.push(value as u8);
            break;
        }
        buf.push((value as u8 & 0x7F) | 0x80);
        value >>= 7;
    }
}

/// 检测数据格式版本并解码
pub fn detect_and_decode(data: &[u8]) -> Result<DecodedMessages> {
    if data.len() < 17 {
        return Err(RkError::Protocol("Data too short".to_string()));
    }

    // magic byte 在 offset 16 (base_offset 8 + batch_length 4 + partition_leader_epoch 4)
    // 对于 MessageSet: offset(8) + message_size(4) + crc(4) → magic 在 offset 16
    let magic = data[16] as i8;

    match magic {
        0 | 1 => {
            let mut reader = KafkaReader::new(data);
            let msg_set = decode_message_set(&mut reader)?;
            Ok(DecodedMessages::Legacy(msg_set))
        }
        2 => Ok(DecodedMessages::V2),
        _ => Err(RkError::Protocol(format!("Unknown magic: {magic}"))),
    }
}

/// 解码结果
#[derive(Debug)]
pub enum DecodedMessages {
    /// 旧格式 MessageSet (v0/v1)
    Legacy(LegacyMessageSet),
    /// v2 RecordBatch (由 record 模块处理)
    V2,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_v0_message() {
        let mut data = Vec::new();
        // offset
        data.extend_from_slice(&0i64.to_be_bytes());
        // message_size: crc(4) + magic(1) + attrs(1) + key_len(4) + val_len(4) + val(5)
        let msg_size = 4 + 1 + 1 + 4 + 4 + 5;
        data.extend_from_slice(&(msg_size as i32).to_be_bytes());
        // crc
        data.extend_from_slice(&0i32.to_be_bytes());
        // magic = 0
        data.push(0);
        // attributes = 0
        data.push(0);
        // key = null
        data.extend_from_slice(&(-1i32).to_be_bytes());
        // value = "hello"
        data.extend_from_slice(&5i32.to_be_bytes());
        data.extend_from_slice(b"hello");

        let mut reader = KafkaReader::new(&data);
        let result = decode_message_set(&mut reader).unwrap();
        assert_eq!(result.messages.len(), 1);
        assert_eq!(result.messages[0].offset, 0);
        assert_eq!(result.messages[0].magic, 0);
        assert_eq!(result.messages[0].key, None);
        assert_eq!(result.messages[0].value, Some(b"hello".to_vec()));
    }

    #[test]
    fn test_decode_v1_message() {
        let mut data = Vec::new();
        data.extend_from_slice(&5i64.to_be_bytes());
        let msg_size = 4 + 1 + 1 + 8 + 4 + 3 + 4 + 5;
        data.extend_from_slice(&(msg_size as i32).to_be_bytes());
        data.extend_from_slice(&0i32.to_be_bytes());
        data.push(1);
        data.push(0);
        data.extend_from_slice(&1000i64.to_be_bytes());
        data.extend_from_slice(&3i32.to_be_bytes());
        data.extend_from_slice(b"abc");
        data.extend_from_slice(&5i32.to_be_bytes());
        data.extend_from_slice(b"world");

        let mut reader = KafkaReader::new(&data);
        let result = decode_message_set(&mut reader).unwrap();
        assert_eq!(result.messages.len(), 1);
        assert_eq!(result.messages[0].offset, 5);
        assert_eq!(result.messages[0].magic, 1);
        assert_eq!(result.messages[0].timestamp_ms, 1000);
        assert_eq!(result.messages[0].key, Some(b"abc".to_vec()));
        assert_eq!(result.messages[0].value, Some(b"world".to_vec()));
    }

    #[test]
    fn test_convert_to_v2_batch() {
        let messages = LegacyMessageSet {
            messages: vec![
                LegacyMessage {
                    offset: 0,
                    magic: 1,
                    attributes: 0,
                    timestamp_ms: 1000,
                    key: Some(b"key1".to_vec()),
                    value: Some(b"value1".to_vec()),
                },
                LegacyMessage {
                    offset: 1,
                    magic: 1,
                    attributes: 0,
                    timestamp_ms: 2000,
                    key: None,
                    value: Some(b"value2".to_vec()),
                },
            ],
        };

        let batch_bytes = convert_to_v2_batch(&messages).unwrap();
        assert!(!batch_bytes.is_empty());

        let mut reader = KafkaReader::new(&batch_bytes);
        let base_offset = reader.read_i64().unwrap();
        assert_eq!(base_offset, 0);

        let batch_length = reader.read_i32().unwrap();
        assert!(batch_length > 0);

        let _epoch = reader.read_i32().unwrap();
        let magic = reader.read_i8().unwrap();
        assert_eq!(magic, 2);
    }

    #[test]
    fn test_empty_message_set() {
        let messages = LegacyMessageSet { messages: vec![] };
        let result = convert_to_v2_batch(&messages).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_write_unsigned_varint() {
        let mut buf = Vec::new();
        write_unsigned_varint(&mut buf, 0);
        assert_eq!(buf, vec![0]);

        let mut buf = Vec::new();
        write_unsigned_varint(&mut buf, 1);
        assert_eq!(buf, vec![1]);

        let mut buf = Vec::new();
        write_unsigned_varint(&mut buf, 127);
        assert_eq!(buf, vec![127]);

        let mut buf = Vec::new();
        write_unsigned_varint(&mut buf, 128);
        assert_eq!(buf, vec![0x80, 0x01]);
    }

    #[test]
    fn test_detect_v2() {
        // 构造一个最小 v2 batch (magic=2 at offset 16)
        let mut data = vec![0u8; 17];
        data[16] = 2; // magic
        let result = detect_and_decode(&data).unwrap();
        assert!(matches!(result, DecodedMessages::V2));
    }
}
