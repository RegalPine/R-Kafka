//! Kafka 协议数据类型
//!
//! 精确实现 Kafka Wire Protocol 的全部基础类型，
//! 参考 <https://kafka.apache.org/protocol.html#protocol_types>
//!
//! 每种类型提供 `read` (从 KafkaReader) 和 `write` (到 KafkaWriter) 方法。

use bytes::BufMut;
use rk_core::error::{RkError, Result};
use std::string::FromUtf8Error;

// ─── KafkaReader: 零拷贝读取器 ──────────────────────────────────────

/// 从 `&[u8]` 切片读取 Kafka 协议类型
pub struct KafkaReader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> KafkaReader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    pub fn position(&self) -> usize {
        self.pos
    }

    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    pub fn slice(&self) -> &'a [u8] {
        &self.buf[self.pos..]
    }

    /// 前进 n 字节，返回跳过的切片
    pub fn advance(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.remaining() < n {
            return Err(RkError::BufferUnderflow { need: n, have: self.remaining() });
        }
        let slice = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice)
    }

    // ─── 基础类型 ──────────────────────────────────────────────────

    pub fn read_i8(&mut self) -> Result<i8> {
        let s = self.advance(1)?;
        Ok(s[0] as i8)
    }

    pub fn read_i16(&mut self) -> Result<i16> {
        let s = self.advance(2)?;
        Ok(i16::from_be_bytes([s[0], s[1]]))
    }

    pub fn read_i32(&mut self) -> Result<i32> {
        let s = self.advance(4)?;
        Ok(i32::from_be_bytes([s[0], s[1], s[2], s[3]]))
    }

    pub fn read_i64(&mut self) -> Result<i64> {
        let s = self.advance(8)?;
        Ok(i64::from_be_bytes([s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7]]))
    }

    pub fn read_u32(&mut self) -> Result<u32> {
        let s = self.advance(4)?;
        Ok(u32::from_be_bytes([s[0], s[1], s[2], s[3]]))
    }

    pub fn read_u64(&mut self) -> Result<u64> {
        let s = self.advance(8)?;
        Ok(u64::from_be_bytes([s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7]]))
    }

    pub fn read_f64(&mut self) -> Result<f64> {
        let s = self.advance(8)?;
        Ok(f64::from_be_bytes([s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7]]))
    }

    pub fn read_bool(&mut self) -> Result<bool> {
        Ok(self.read_i8()? != 0)
    }

    // ─── STRING / NULLABLE_STRING ────────────────────────────────────

    /// INT16 长度前缀 + UTF-8 字节
    pub fn read_string(&mut self) -> Result<String> {
        let len = self.read_i16()? as usize;
        if len == 0xFFFF_u16 as usize {
            // -1 表示 null，但 STRING 不应为 null
            return Err(RkError::InvalidDataType("unexpected null STRING".into()));
        }
        let bytes = self.advance(len)?;
        String::from_utf8(bytes.to_vec())
            .map_err(|e: FromUtf8Error| RkError::InvalidDataType(e.to_string()))
    }

    /// INT16 长度前缀 + UTF-8 字节，-1 表示 null
    pub fn read_nullable_string(&mut self) -> Result<Option<String>> {
        let len = self.read_i16()?;
        if len < 0 {
            return Ok(None);
        }
        let bytes = self.advance(len as usize)?;
        let s = String::from_utf8(bytes.to_vec())
            .map_err(|e: FromUtf8Error| RkError::InvalidDataType(e.to_string()))?;
        Ok(Some(s))
    }

    // ─── COMPACT_STRING / COMPACT_NULLABLE_STRING ────────────────────

    /// UNSIGNED_VARINT 长度前缀 (实际长度 + 1) + UTF-8 字节
    pub fn read_compact_string(&mut self) -> Result<String> {
        let len = self.read_unsigned_varint()? as usize;
        if len == 0 {
            return Err(RkError::InvalidDataType("unexpected null COMPACT_STRING".into()));
        }
        let actual_len = len - 1;
        let bytes = self.advance(actual_len)?;
        String::from_utf8(bytes.to_vec())
            .map_err(|e: FromUtf8Error| RkError::InvalidDataType(e.to_string()))
    }

    pub fn read_compact_nullable_string(&mut self) -> Result<Option<String>> {
        let len = self.read_unsigned_varint()? as usize;
        if len == 0 {
            return Ok(None);
        }
        let actual_len = len - 1;
        let bytes = self.advance(actual_len)?;
        let s = String::from_utf8(bytes.to_vec())
            .map_err(|e: FromUtf8Error| RkError::InvalidDataType(e.to_string()))?;
        Ok(Some(s))
    }

    // ─── BYTES / COMPACT_BYTES ───────────────────────────────────────

    /// INT32 长度前缀 + 字节
    pub fn read_bytes(&mut self) -> Result<Vec<u8>> {
        let len = self.read_i32()?;
        if len < 0 {
            return Ok(Vec::new());
        }
        let bytes = self.advance(len as usize)?;
        Ok(bytes.to_vec())
    }

    pub fn read_nullable_bytes(&mut self) -> Result<Option<Vec<u8>>> {
        let len = self.read_i32()?;
        if len < 0 {
            return Ok(None);
        }
        let bytes = self.advance(len as usize)?;
        Ok(Some(bytes.to_vec()))
    }

    /// UNSIGNED_VARINT 长度前缀 (实际长度 + 1) + 字节
    pub fn read_compact_bytes(&mut self) -> Result<Vec<u8>> {
        let len = self.read_unsigned_varint()? as usize;
        if len == 0 {
            return Ok(Vec::new());
        }
        let actual_len = len - 1;
        let bytes = self.advance(actual_len)?;
        Ok(bytes.to_vec())
    }

    pub fn read_compact_nullable_bytes(&mut self) -> Result<Option<Vec<u8>>> {
        let len = self.read_unsigned_varint()? as usize;
        if len == 0 {
            return Ok(None);
        }
        let actual_len = len - 1;
        let bytes = self.advance(actual_len)?;
        Ok(Some(bytes.to_vec()))
    }

    // ─── VARINT / VARLONG ────────────────────────────────────────────

    /// ZigZag 编码的 VARINT (i32)
    pub fn read_varint(&mut self) -> Result<i32> {
        let raw = self.read_unsigned_varint()?;
        Ok(decode_zigzag32(raw as u32))
    }

    /// ZigZag 编码的 VARLONG (i64)
    pub fn read_varlong(&mut self) -> Result<i64> {
        let raw = self.read_unsigned_varlong()?;
        Ok(decode_zigzag64(raw))
    }

    /// 无符号 VARINT (最多 5 字节)
    pub fn read_unsigned_varint(&mut self) -> Result<u64> {
        let mut result: u64 = 0;
        let mut shift: u32 = 0;
        loop {
            if shift >= 35 {
                return Err(RkError::InvalidDataType("VARINT too long".into()));
            }
            let b = self.advance(1)?[0];
            result |= ((b & 0x7F) as u64) << shift;
            if b & 0x80 == 0 {
                return Ok(result);
            }
            shift += 7;
        }
    }

    /// 无符号 VARLONG (最多 10 字节)
    pub fn read_unsigned_varlong(&mut self) -> Result<u64> {
        let mut result: u64 = 0;
        let mut shift: u32 = 0;
        loop {
            if shift >= 70 {
                return Err(RkError::InvalidDataType("VARLONG too long".into()));
            }
            let b = self.advance(1)?[0];
            result |= ((b & 0x7F) as u64) << shift;
            if b & 0x80 == 0 {
                return Ok(result);
            }
            shift += 7;
        }
    }

    // ─── ARRAY / COMPACT_ARRAY ───────────────────────────────────────

    /// 读取数组: 先读 INT32 长度，再逐个读元素
    pub fn read_array<T, F>(&mut self, read_elem: F) -> Result<Vec<T>>
    where
        F: Fn(&mut Self) -> Result<T>,
    {
        let len = self.read_i32()?;
        if len < 0 {
            return Ok(Vec::new()); // null array
        }
        // 安全限制: 数组元素数不超过剩余字节数 (每个元素至少 1 字节)
        let len_usize = len as usize;
        if len_usize > self.remaining() {
            return Err(rk_core::error::RkError::Protocol(
                format!("Array length {} exceeds remaining bytes {}", len, self.remaining())
            ));
        }
        let mut items = Vec::with_capacity(len_usize);
        for _ in 0..len {
            items.push(read_elem(self)?);
        }
        Ok(items)
    }

    /// 读取可空数组: i32 长度 (-1 = null → None)
    pub fn read_nullable_array<T, F>(&mut self, read_elem: F) -> Result<Option<Vec<T>>>
    where
        F: Fn(&mut Self) -> Result<T>,
    {
        let len = self.read_i32()?;
        if len < 0 {
            return Ok(None); // null
        }
        // 安全限制
        let len_usize = len as usize;
        if len_usize > self.remaining() {
            return Err(rk_core::error::RkError::Protocol(
                format!("Array length {} exceeds remaining bytes {}", len, self.remaining())
            ));
        }
        let mut items = Vec::with_capacity(len_usize);
        for _ in 0..len {
            items.push(read_elem(self)?);
        }
        Ok(Some(items))
    }

    /// 读取紧凑数组: UNSIGNED_VARINT 长度 (实际长度 + 1)
    pub fn read_compact_array<T, F>(&mut self, read_elem: F) -> Result<Vec<T>>
    where
        F: Fn(&mut Self) -> Result<T>,
    {
        let len = self.read_unsigned_varint()? as usize;
        if len == 0 {
            return Ok(Vec::new()); // null compact array
        }
        let actual_len = len - 1;
        // 安全限制
        if actual_len > self.remaining() {
            return Err(rk_core::error::RkError::Protocol(
                format!("Compact array length {} exceeds remaining bytes {}", actual_len, self.remaining())
            ));
        }
        let mut items = Vec::with_capacity(actual_len);
        for _ in 0..actual_len {
            items.push(read_elem(self)?);
        }
        Ok(items)
    }

    /// 读取可空紧凑数组: UNSIGNED_VARINT 长度, 0 = null
    pub fn read_compact_nullable_array<T, F>(&mut self, read_elem: F) -> Result<Option<Vec<T>>>
    where
        F: Fn(&mut Self) -> Result<T>,
    {
        let len = self.read_unsigned_varint()? as usize;
        if len == 0 {
            return Ok(None);
        }
        let actual_len = len - 1;
        if actual_len > self.remaining() {
            return Err(rk_core::error::RkError::Protocol(
                format!("Compact nullable array length {} exceeds remaining bytes {}", actual_len, self.remaining())
            ));
        }
        let mut items = Vec::with_capacity(actual_len);
        for _ in 0..actual_len {
            items.push(read_elem(self)?);
        }
        Ok(Some(items))
    }

    // ─── TAGGED_FIELDS (Flexible 版本) ───────────────────────────────

    /// 读取 tagged fields (KIP-482)
    /// 格式: num_fields (unsigned_varint), 然后每个 tag:
    ///   tag (unsigned_varint), size (unsigned_varint), data (bytes)
    pub fn read_tagged_fields(&mut self) -> Result<Vec<TaggedField>> {
        let num_fields = self.read_unsigned_varint()? as usize;
        let mut fields = Vec::with_capacity(num_fields);
        for _ in 0..num_fields {
            let tag = self.read_unsigned_varint()? as u32;
            let size = self.read_unsigned_varint()? as usize;
            let data = self.advance(size)?.to_vec();
            fields.push(TaggedField { tag, data });
        }
        Ok(fields)
    }
}

// ─── KafkaWriter: 写入器 ────────────────────────────────────────────

/// 向 `BytesMut` 写入 Kafka 协议类型
pub struct KafkaWriter<'a> {
    buf: &'a mut bytes::BytesMut,
}

impl<'a> KafkaWriter<'a> {
    pub fn new(buf: &'a mut bytes::BytesMut) -> Self {
        Self { buf }
    }

    pub fn written(&self) -> usize {
        self.buf.len()
    }

    // ─── 基础类型 ──────────────────────────────────────────────────

    pub fn write_i8(&mut self, v: i8) {
        self.buf.put_i8(v);
    }

    pub fn write_i16(&mut self, v: i16) {
        self.buf.put_i16(v);
    }

    pub fn write_i32(&mut self, v: i32) {
        self.buf.put_i32(v);
    }

    pub fn write_i64(&mut self, v: i64) {
        self.buf.put_i64(v);
    }

    pub fn write_u32(&mut self, v: u32) {
        self.buf.put_u32(v);
    }

    pub fn write_u64(&mut self, v: u64) {
        self.buf.put_u64(v);
    }

    pub fn write_bool(&mut self, v: bool) {
        self.buf.put_i8(if v { 1 } else { 0 });
    }

    // ─── STRING / NULLABLE_STRING ────────────────────────────────────

    pub fn write_string(&mut self, s: &str) {
        self.buf.put_i16(s.len() as i16);
        self.buf.put_slice(s.as_bytes());
    }

    pub fn write_nullable_string(&mut self, s: Option<&str>) {
        match s {
            Some(s) => self.write_string(s),
            None => self.buf.put_i16(-1),
        }
    }

    // ─── COMPACT_STRING / COMPACT_NULLABLE_STRING ────────────────────

    pub fn write_compact_string(&mut self, s: &str) {
        self.write_unsigned_varint((s.len() + 1) as u64);
        self.buf.put_slice(s.as_bytes());
    }

    pub fn write_compact_nullable_string(&mut self, s: Option<&str>) {
        match s {
            Some(s) => {
                self.write_unsigned_varint((s.len() + 1) as u64);
                self.buf.put_slice(s.as_bytes());
            }
            None => self.write_unsigned_varint(0),
        }
    }

    // ─── BYTES / COMPACT_BYTES ───────────────────────────────────────

    pub fn write_bytes(&mut self, data: &[u8]) {
        self.buf.put_i32(data.len() as i32);
        self.buf.put_slice(data);
    }

    pub fn write_nullable_bytes(&mut self, data: Option<&[u8]>) {
        match data {
            Some(data) => self.write_bytes(data),
            None => self.buf.put_i32(-1),
        }
    }

    pub fn write_compact_bytes(&mut self, data: &[u8]) {
        self.write_unsigned_varint((data.len() + 1) as u64);
        self.buf.put_slice(data);
    }

    pub fn write_compact_nullable_bytes(&mut self, data: Option<&[u8]>) {
        match data {
            Some(data) => {
                self.write_unsigned_varint((data.len() + 1) as u64);
                self.buf.put_slice(data);
            }
            None => self.write_unsigned_varint(0),
        }
    }

    // ─── VARINT / VARLONG ────────────────────────────────────────────

    pub fn write_varint(&mut self, v: i32) {
        self.write_unsigned_varint(encode_zigzag32(v) as u64);
    }

    pub fn write_varlong(&mut self, v: i64) {
        self.write_unsigned_varlong(encode_zigzag64(v));
    }

    pub fn write_unsigned_varint(&mut self, mut value: u64) {
        loop {
            let mut b = (value & 0x7F) as u8;
            value >>= 7;
            if value != 0 {
                b |= 0x80;
            }
            self.buf.put_u8(b);
            if value == 0 {
                break;
            }
        }
    }

    pub fn write_unsigned_varlong(&mut self, mut value: u64) {
        loop {
            let mut b = (value & 0x7F) as u8;
            value >>= 7;
            if value != 0 {
                b |= 0x80;
            }
            self.buf.put_u8(b);
            if value == 0 {
                break;
            }
        }
    }

    // ─── ARRAY / COMPACT_ARRAY ───────────────────────────────────────

    pub fn write_array<T, F>(&mut self, items: &[T], write_elem: F)
    where
        F: Fn(&mut Self, &T),
    {
        self.buf.put_i32(items.len() as i32);
        for item in items {
            write_elem(self, item);
        }
    }

    pub fn write_compact_array<T, F>(&mut self, items: &[T], write_elem: F)
    where
        F: Fn(&mut Self, &T),
    {
        self.write_unsigned_varint((items.len() + 1) as u64);
        for item in items {
            write_elem(self, item);
        }
    }

    // ─── TAGGED_FIELDS ───────────────────────────────────────────────

    pub fn write_tagged_fields(&mut self, fields: &[TaggedField]) {
        self.write_unsigned_varint(fields.len() as u64);
        for field in fields {
            self.write_unsigned_varint(field.tag as u64);
            self.write_unsigned_varint(field.data.len() as u64);
            self.buf.put_slice(&field.data);
        }
    }
}

// ─── Tagged Field ────────────────────────────────────────────────────

/// KIP-482 Tagged Field (Flexible 版本)
#[derive(Debug, Clone)]
pub struct TaggedField {
    pub tag: u32,
    pub data: Vec<u8>,
}

// ─── ZigZag 编解码 ──────────────────────────────────────────────────

fn encode_zigzag32(v: i32) -> u32 {
    ((v << 1) ^ (v >> 31)) as u32
}

fn decode_zigzag32(v: u32) -> i32 {
    ((v >> 1) as i32) ^ (-((v & 1) as i32))
}

fn encode_zigzag64(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

fn decode_zigzag64(v: u64) -> i64 {
    ((v >> 1) as i64) ^ (-((v & 1) as i64))
}

// ─── 测试 ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_i8_roundtrip() {
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        w.write_i8(42);
        w.write_i8(-1);
        let mut r = KafkaReader::new(&buf);
        assert_eq!(r.read_i8().unwrap(), 42);
        assert_eq!(r.read_i8().unwrap(), -1);
    }

    #[test]
    fn test_i32_roundtrip() {
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        w.write_i32(0);
        w.write_i32(i32::MAX);
        w.write_i32(i32::MIN);
        let mut r = KafkaReader::new(&buf);
        assert_eq!(r.read_i32().unwrap(), 0);
        assert_eq!(r.read_i32().unwrap(), i32::MAX);
        assert_eq!(r.read_i32().unwrap(), i32::MIN);
    }

    #[test]
    fn test_string_roundtrip() {
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        w.write_string("hello");
        w.write_string("");
        let mut r = KafkaReader::new(&buf);
        assert_eq!(r.read_string().unwrap(), "hello");
        assert_eq!(r.read_string().unwrap(), "");
    }

    #[test]
    fn test_nullable_string_roundtrip() {
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        w.write_nullable_string(Some("hello"));
        w.write_nullable_string(None);
        let mut r = KafkaReader::new(&buf);
        assert_eq!(r.read_nullable_string().unwrap(), Some("hello".to_string()));
        assert_eq!(r.read_nullable_string().unwrap(), None);
    }

    #[test]
    fn test_compact_string_roundtrip() {
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        w.write_compact_string("world");
        let mut r = KafkaReader::new(&buf);
        assert_eq!(r.read_compact_string().unwrap(), "world");
    }

    #[test]
    fn test_varint_roundtrip() {
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        for v in [0, 1, -1, 127, -128, i32::MAX, i32::MIN] {
            w.write_varint(v);
        }
        let mut r = KafkaReader::new(&buf);
        assert_eq!(r.read_varint().unwrap(), 0);
        assert_eq!(r.read_varint().unwrap(), 1);
        assert_eq!(r.read_varint().unwrap(), -1);
        assert_eq!(r.read_varint().unwrap(), 127);
        assert_eq!(r.read_varint().unwrap(), -128);
        assert_eq!(r.read_varint().unwrap(), i32::MAX);
        assert_eq!(r.read_varint().unwrap(), i32::MIN);
    }

    #[test]
    fn test_varlong_roundtrip() {
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        for v in [0i64, 1, -1, i64::MAX, i64::MIN] {
            w.write_varlong(v);
        }
        let mut r = KafkaReader::new(&buf);
        assert_eq!(r.read_varlong().unwrap(), 0);
        assert_eq!(r.read_varlong().unwrap(), 1);
        assert_eq!(r.read_varlong().unwrap(), -1);
        assert_eq!(r.read_varlong().unwrap(), i64::MAX);
        assert_eq!(r.read_varlong().unwrap(), i64::MIN);
    }

    #[test]
    fn test_bytes_roundtrip() {
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        w.write_bytes(b"hello world");
        w.write_nullable_bytes(None);
        let mut r = KafkaReader::new(&buf);
        assert_eq!(r.read_bytes().unwrap(), b"hello world");
        assert_eq!(r.read_nullable_bytes().unwrap(), None);
    }

    #[test]
    fn test_zigzag() {
        // 标准 ZigZag 映射
        assert_eq!(encode_zigzag32(0), 0);
        assert_eq!(encode_zigzag32(-1), 1);
        assert_eq!(encode_zigzag32(1), 2);
        assert_eq!(encode_zigzag32(-2), 3);
        assert_eq!(decode_zigzag32(0), 0);
        assert_eq!(decode_zigzag32(1), -1);
        assert_eq!(decode_zigzag32(2), 1);
        assert_eq!(decode_zigzag32(3), -2);
    }

    #[test]
    fn test_buffer_underflow() {
        let mut r = KafkaReader::new(&[0x00]);
        assert!(r.read_i32().is_err());
    }
}
