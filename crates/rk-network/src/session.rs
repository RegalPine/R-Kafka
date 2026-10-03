//! Connection Session
//!
//! 管理 TCP 连接的认证状态和会话信息。
//! 每个客户端连接对应一个 ConnectionSession。
//! 此模块属于网络层，无 broker 业务依赖。

use std::net::SocketAddr;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Instant;

use tracing::debug;

/// 连接认证状态
#[derive(Debug, Clone, PartialEq)]
pub enum AuthState {
    /// 未认证 (初始状态 / 无需认证)
    NotAuthenticated,
    /// SASL 握手完成，等待认证数据
    SaslHandshaked { mechanism: String },
    /// 已认证
    Authenticated { username: String, mechanism: String },
}

/// 连接会话: 跟踪每个客户端连接的状态
pub struct ConnectionSession {
    /// 远程地址
    remote_addr: SocketAddr,
    /// 认证状态
    auth_state: AuthState,
    /// 连接创建时间
    created_at: Instant,
    /// 请求计数
    request_count: AtomicI64,
    /// 客户端 ID (从 RequestHeader 获取)
    client_id: Option<String>,
    /// 客户端软件名称 (从 ApiVersions v3+ 获取)
    client_software_name: Option<String>,
    /// 客户端软件版本
    client_software_version: Option<String>,
}

impl ConnectionSession {
    /// 创建新会话
    pub fn new(remote_addr: SocketAddr) -> Self {
        Self {
            remote_addr,
            auth_state: AuthState::NotAuthenticated,
            created_at: Instant::now(),
            request_count: AtomicI64::new(0),
            client_id: None,
            client_software_name: None,
            client_software_version: None,
        }
    }

    /// 获取远程地址
    pub fn remote_addr(&self) -> SocketAddr {
        self.remote_addr
    }

    /// 获取认证状态
    pub fn auth_state(&self) -> &AuthState {
        &self.auth_state
    }

    /// 是否已认证
    pub fn is_authenticated(&self) -> bool {
        matches!(self.auth_state, AuthState::Authenticated { .. })
    }

    /// 获取认证用户名
    pub fn username(&self) -> Option<&str> {
        match &self.auth_state {
            AuthState::Authenticated { username, .. } => Some(username),
            _ => None,
        }
    }

    /// 获取当前认证机制名称 (如果处于 SASL 状态)
    pub fn auth_state_mechanism(&self) -> Option<&str> {
        match &self.auth_state {
            AuthState::SaslHandshaked { mechanism } => Some(mechanism),
            AuthState::Authenticated { mechanism, .. } => Some(mechanism),
            _ => None,
        }
    }

    /// 设置 SASL 握手完成
    pub fn set_sasl_handshaked(&mut self, mechanism: String) {
        debug!(
            remote = %self.remote_addr,
            mechanism = %mechanism,
            "SASL handshake completed"
        );
        self.auth_state = AuthState::SaslHandshaked { mechanism };
    }

    /// 设置认证成功
    pub fn set_authenticated(&mut self, username: String, mechanism: String) {
        debug!(
            remote = %self.remote_addr,
            username = %username,
            mechanism = %mechanism,
            "Client authenticated"
        );
        self.auth_state = AuthState::Authenticated {
            username,
            mechanism,
        };
    }

    /// 递增请求计数
    pub fn increment_request_count(&self) -> i64 {
        self.request_count.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// 获取请求计数
    pub fn request_count(&self) -> i64 {
        self.request_count.load(Ordering::Relaxed)
    }

    /// 设置客户端 ID
    pub fn set_client_id(&mut self, client_id: String) {
        self.client_id = Some(client_id);
    }

    /// 获取客户端 ID
    pub fn client_id(&self) -> Option<&str> {
        self.client_id.as_deref()
    }

    /// 设置客户端软件信息
    pub fn set_client_software(&mut self, name: String, version: String) {
        self.client_software_name = Some(name);
        self.client_software_version = Some(version);
    }

    /// 获取连接存活时间 (毫秒)
    pub fn uptime_ms(&self) -> u64 {
        self.created_at.elapsed().as_millis() as u64
    }

    /// 重置认证状态 (断开重连时)
    pub fn reset_auth(&mut self) {
        self.auth_state = AuthState::NotAuthenticated;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    fn test_addr() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 12345)
    }

    #[test]
    fn test_new_session() {
        let session = ConnectionSession::new(test_addr());
        assert_eq!(session.remote_addr(), test_addr());
        assert!(!session.is_authenticated());
        assert_eq!(session.auth_state(), &AuthState::NotAuthenticated);
        assert_eq!(session.request_count(), 0);
        assert!(session.client_id().is_none());
        assert!(session.username().is_none());
    }

    #[test]
    fn test_sasl_handshake_flow() {
        let mut session = ConnectionSession::new(test_addr());

        // 1. SASL Handshake
        session.set_sasl_handshaked("PLAIN".to_string());
        assert!(!session.is_authenticated());
        assert_eq!(
            session.auth_state(),
            &AuthState::SaslHandshaked {
                mechanism: "PLAIN".to_string()
            }
        );

        // 2. SASL Authenticate
        session.set_authenticated("admin".to_string(), "PLAIN".to_string());
        assert!(session.is_authenticated());
        assert_eq!(session.username(), Some("admin"));
    }

    #[test]
    fn test_request_count() {
        let session = ConnectionSession::new(test_addr());
        assert_eq!(session.increment_request_count(), 1);
        assert_eq!(session.increment_request_count(), 2);
        assert_eq!(session.increment_request_count(), 3);
        assert_eq!(session.request_count(), 3);
    }

    #[test]
    fn test_uptime() {
        let session = ConnectionSession::new(test_addr());
        assert!(session.uptime_ms() < 100);
    }

    #[test]
    fn test_reset_auth() {
        let mut session = ConnectionSession::new(test_addr());
        session.set_authenticated("admin".to_string(), "PLAIN".to_string());
        assert!(session.is_authenticated());

        session.reset_auth();
        assert!(!session.is_authenticated());
        assert_eq!(session.auth_state(), &AuthState::NotAuthenticated);
    }
}
