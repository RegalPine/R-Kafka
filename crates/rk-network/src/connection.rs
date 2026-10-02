//! 连接管理
//!
//! 每个 TCP 连接的生命周期管理：帧解码、SASL 状态机、会话跟踪、读写缓冲。
//! 集成 BrokerRouter 完成请求处理。
//! 支持纯 TCP 和 TLS 两种传输方式。

use bytes::{Buf, BytesMut};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::server::TlsStream;
use tracing::{debug, error, warn};

use rk_broker::connection_session::ConnectionSession;
use rk_broker::BrokerRouter;
use rk_core::error::Result;

/// 连接状态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// 刚建立连接
    Connected,
    /// SASL 握手完成，等待认证
    SaslHandshaked,
    /// 已认证 (或无需认证)
    Authenticated,
    /// 正在读取请求
    Reading,
    /// 正在写入响应
    Writing,
    /// 连接关闭
    Closed,
}

/// 传输层流: 支持纯 TCP 和 TLS
enum TransportStream {
    Tcp(TcpStream),
    Tls(TlsStream<TcpStream>),
}

impl AsyncRead for TransportStream {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            TransportStream::Tcp(s) => std::pin::Pin::new(s).poll_read(cx, buf),
            TransportStream::Tls(s) => std::pin::Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for TransportStream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match self.get_mut() {
            TransportStream::Tcp(s) => std::pin::Pin::new(s).poll_write(cx, buf),
            TransportStream::Tls(s) => std::pin::Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            TransportStream::Tcp(s) => std::pin::Pin::new(s).poll_flush(cx),
            TransportStream::Tls(s) => std::pin::Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            TransportStream::Tcp(s) => std::pin::Pin::new(s).poll_shutdown(cx),
            TransportStream::Tls(s) => std::pin::Pin::new(s).poll_shutdown(cx),
        }
    }
}

/// 单个客户端连接
pub struct Connection {
    stream: TransportStream,
    state: ConnectionState,
    read_buf: BytesMut,
    /// 已读到的完整帧 (4字节长度前缀后的数据)
    frame_buf: BytesMut,
    /// 连接会话
    session: ConnectionSession,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        let peer = stream.peer_addr().ok();
        let addr = peer.unwrap_or_else(|| {
            std::net::SocketAddr::new(
                std::net::IpAddr::V4(std::net::Ipv4Addr::new(0, 0, 0, 0)),
                0,
            )
        });
        Self {
            stream: TransportStream::Tcp(stream),
            state: ConnectionState::Connected,
            read_buf: BytesMut::with_capacity(8192),
            frame_buf: BytesMut::with_capacity(8192),
            session: ConnectionSession::new(addr),
        }
    }

    /// 创建 TLS 连接
    pub fn new_tls(stream: TlsStream<TcpStream>, peer_addr: std::net::SocketAddr) -> Self {
        Self {
            stream: TransportStream::Tls(stream),
            state: ConnectionState::Connected,
            read_buf: BytesMut::with_capacity(8192),
            frame_buf: BytesMut::with_capacity(8192),
            session: ConnectionSession::new(peer_addr),
        }
    }

    pub fn state(&self) -> ConnectionState {
        self.state
    }

    pub fn session(&self) -> &ConnectionSession {
        &self.session
    }

    pub fn session_mut(&mut self) -> &mut ConnectionSession {
        &mut self.session
    }

    /// 读取下一个完整的 Kafka 请求帧
    /// 返回帧数据 (不含 4 字节长度前缀)
    pub async fn read_frame(&mut self) -> Result<Option<&[u8]>> {
        loop {
            // 尝试从已有 buffer 中解析帧
            if self.read_buf.len() >= 4 {
                let frame_len = (&self.read_buf[..4]).get_u32() as usize;
                if frame_len > rk_core::MAX_MESSAGE_SIZE {
                    return Err(rk_core::RkError::RequestTooLarge {
                        size: frame_len,
                        max: rk_core::MAX_MESSAGE_SIZE,
                    });
                }
                if self.read_buf.len() >= 4 + frame_len {
                    // 跳过 4 字节长度前缀
                    self.read_buf.advance(4);
                    self.frame_buf = self.read_buf.split_to(frame_len);
                    self.state = ConnectionState::Reading;
                    return Ok(Some(&self.frame_buf));
                }
            }

            // 从 socket 读取更多数据
            let n = self.stream.read_buf(&mut self.read_buf).await?;
            if n == 0 {
                self.state = ConnectionState::Closed;
                return Ok(None);
            }
            tracing::trace!("Read {} bytes from connection", n);
        }
    }

    /// 写入响应帧 (自动添加 4 字节长度前缀)
    pub async fn write_frame(&mut self, data: &[u8]) -> Result<()> {
        let len = data.len() as u32;
        self.stream.write_all(&len.to_be_bytes()).await?;
        self.stream.write_all(data).await?;
        self.stream.flush().await?;
        self.state = ConnectionState::Connected;
        Ok(())
    }
}

/// 处理单个 TCP 连接的完整生命周期
///
/// 循环读取请求帧 → SASL 状态机检查 → BrokerRouter 处理 → 写入响应帧，
/// 直到连接关闭或出错。
pub async fn handle_connection(
    stream: TcpStream,
    router: Arc<BrokerRouter>,
) {
    let peer = stream.peer_addr().ok();
    let peer_str = peer.map(|a| a.to_string()).unwrap_or_else(|| "unknown".to_string());
    debug!(peer = %peer_str, "New client connection");

    let conn = Connection::new(stream);
    handle_connection_inner(conn, peer_str, router).await;
}

/// 处理单个 TLS 连接的完整生命周期
pub async fn handle_tls_connection(
    stream: TlsStream<TcpStream>,
    peer_addr: std::net::SocketAddr,
    router: Arc<BrokerRouter>,
) {
    let peer_str = peer_addr.to_string();
    debug!(peer = %peer_str, "New TLS client connection");

    let conn = Connection::new_tls(stream, peer_addr);
    handle_connection_inner(conn, peer_str, router).await;
}

/// 连接处理核心逻辑 (TCP/TLS 共用)
async fn handle_connection_inner(
    mut conn: Connection,
    peer_str: String,
    router: Arc<BrokerRouter>,
) {
    let sasl_enabled = router.is_sasl_enabled();

    loop {
        // 读取下一帧
        let frame = match conn.read_frame().await {
            Ok(Some(frame)) => frame.to_vec(),
            Ok(None) => {
                debug!(peer = %peer_str, "Client disconnected");
                break;
            }
            Err(e) => {
                warn!(peer = %peer_str, error = %e, "Connection read error");
                break;
            }
        };

        // 递增请求计数
        conn.session.increment_request_count();

        // 提取 api_key
        let api_key = match BrokerRouter::extract_api_key(&frame) {
            Some(key) => key,
            None => {
                warn!(peer = %peer_str, "Frame too short to extract api_key");
                break;
            }
        };

        // SASL 状态机: 认证前检查
        if sasl_enabled && !conn.session.is_authenticated() {
            if !BrokerRouter::is_pre_auth_api(api_key) {
                warn!(
                    peer = %peer_str,
                    api_key = api_key,
                    "Rejected non-auth API before SASL authentication"
                );
                break;
            }
        }

        // 通过 BrokerRouter 处理请求
        match router.handle_frame(&frame) {
            Ok(response) => {
                // SASL 状态转换: 在成功处理后更新 session 状态
                if sasl_enabled && !conn.session.is_authenticated() {
                    match api_key {
                        17 => {
                            // SaslHandshake 成功: 提取 mechanism
                            if let Some(mechanism) = extract_sasl_mechanism(&frame) {
                                conn.session.set_sasl_handshaked(mechanism);
                                conn.state = ConnectionState::SaslHandshaked;
                                debug!(peer = %peer_str, "SASL handshake completed");
                            }
                        }
                        36 => {
                            // SaslAuthenticate 成功: 提取 username
                            if let Some(username) = extract_sasl_username(&frame) {
                                conn.session.set_authenticated(
                                    username,
                                    conn.session.auth_state_mechanism().unwrap_or("PLAIN").to_string(),
                                );
                                conn.state = ConnectionState::Authenticated;
                                debug!(peer = %peer_str, "Client authenticated");
                            }
                        }
                        _ => {}
                    }
                }

                if let Err(e) = conn.write_frame(&response).await {
                    warn!(peer = %peer_str, error = %e, "Connection write error");
                    break;
                }
            }
            Err(e) => {
                error!(peer = %peer_str, error = %e, "Request handling error");
                break;
            }
        }
    }

    debug!(
        peer = %peer_str,
        requests = conn.session.request_count(),
        uptime_ms = conn.session.uptime_ms(),
        "Connection closed"
    );
}

/// 从 SaslHandshake 请求帧中提取 mechanism
fn extract_sasl_mechanism(frame: &[u8]) -> Option<String> {
    use rk_protocol::apis::sasl_handshake::SaslHandshakeRequest;
    use rk_protocol::codec::KafkaRequestDecoder;
    use rk_protocol::types::KafkaReader;
    use rk_protocol::RequestHeader;

    let mut reader = KafkaReader::new(frame);
    let header = RequestHeader::decode(&mut reader).ok()?;
    let body_bytes = &frame[reader.position()..];
    let mut body_reader = KafkaReader::new(body_bytes);
    let request = SaslHandshakeRequest::decode(&mut body_reader, header.api_version).ok()?;
    Some(request.mechanism)
}

/// 从 SaslAuthenticate 请求帧中提取 username (SASL/PLAIN 格式)
fn extract_sasl_username(frame: &[u8]) -> Option<String> {
    use rk_protocol::apis::sasl_authenticate::SaslAuthenticateRequest;
    use rk_protocol::codec::KafkaRequestDecoder;
    use rk_protocol::types::KafkaReader;
    use rk_protocol::RequestHeader;

    let mut reader = KafkaReader::new(frame);
    let header = RequestHeader::decode(&mut reader).ok()?;
    let body_bytes = &frame[reader.position()..];
    let mut body_reader = KafkaReader::new(body_bytes);
    let request = SaslAuthenticateRequest::decode(&mut body_reader, header.api_version).ok()?;
    // 解析 SASL/PLAIN 格式: \0username\0password
    let (username, _password) = request.parse_plain_credentials()?;
    Some(username)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_state_initial() {
        assert_eq!(ConnectionState::Connected, ConnectionState::Connected);
        assert_ne!(ConnectionState::Connected, ConnectionState::Closed);
        assert_ne!(ConnectionState::SaslHandshaked, ConnectionState::Authenticated);
    }

    #[test]
    fn test_connection_state_transitions() {
        let mut state = ConnectionState::Connected;
        assert_eq!(state, ConnectionState::Connected);
        state = ConnectionState::SaslHandshaked;
        assert_eq!(state, ConnectionState::SaslHandshaked);
        state = ConnectionState::Authenticated;
        assert_eq!(state, ConnectionState::Authenticated);
    }
}
