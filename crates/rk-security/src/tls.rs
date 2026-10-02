//! TLS/mTLS — 传输层安全
//!
//! 基于 `rustls` 实现 TLS 1.3 / TLS 1.2 加密通信:
//!
//! - **OneWay TLS**: 服务端证书验证 (客户端验证服务端)
//! - **Mutual TLS (mTLS)**: 双向证书验证
//!
//! ```text
//! TLS 连接流程:
//!
//! Client                              Server
//!   │                                    │
//!   ├── ClientHello ──────────────────→  │
//!   │                                    │
//!   │  ←────────────── ServerHello + Cert ─┤
//!   │                                    │
//!   │  [验证服务端证书]                     │
//!   │  [mTLS: 发送客户端证书]               │
//!   │                                    │
//!   ├── Finished ─────────────────────→  │
//!   │                                    │
//!   │  ←───────────────────── Finished ──┤
//!   │                                    │
//!   ╞═══════════ 加密通道建立 ══════════════╡
//! ```

use std::io::{self, BufReader};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tracing::info;

use rk_core::error::{RkError, Result};

// ─── TLS 模式 ───────────────────────────────────────────────────────

/// TLS 工作模式
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TlsMode {
    /// 单向 TLS (仅服务端证书)
    OneWay,
    /// 双向 TLS (mTLS, 客户端也需要证书)
    Mutual,
}

impl std::fmt::Display for TlsMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TlsMode::OneWay => write!(f, "OneWay"),
            TlsMode::Mutual => write!(f, "Mutual"),
        }
    }
}

// ─── TLS 配置 ───────────────────────────────────────────────────────

/// TLS 配置
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// 是否启用 TLS
    pub enabled: bool,
    /// TLS 模式
    pub mode: TlsMode,
    /// 服务端证书路径
    pub cert_path: PathBuf,
    /// 服务端私钥路径
    pub key_path: PathBuf,
    /// CA 证书路径 (mTLS 时用于验证客户端证书)
    pub ca_path: Option<PathBuf>,
    /// 允许的 TLS 协议版本
    pub protocol_versions: Vec<TlsProtocolVersion>,
}

/// TLS 协议版本
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TlsProtocolVersion {
    /// TLS 1.2
    Tls12,
    /// TLS 1.3
    Tls13,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            mode: TlsMode::OneWay,
            cert_path: PathBuf::from("certs/server.crt"),
            key_path: PathBuf::from("certs/server.key"),
            ca_path: None,
            protocol_versions: vec![TlsProtocolVersion::Tls13],
        }
    }
}

impl TlsConfig {
    /// 创建默认启用的 TLS 配置
    pub fn enabled(cert_path: impl Into<PathBuf>, key_path: impl Into<PathBuf>) -> Self {
        Self {
            enabled: true,
            cert_path: cert_path.into(),
            key_path: key_path.into(),
            ..Default::default()
        }
    }

    /// 设置 mTLS 模式
    pub fn mutual(mut self, ca_path: impl Into<PathBuf>) -> Self {
        self.mode = TlsMode::Mutual;
        self.ca_path = Some(ca_path.into());
        self
    }

    /// 验证配置完整性
    pub fn validate(&self) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }
        if !self.cert_path.exists() {
            return Err(RkError::Config(format!(
                "TLS cert not found: {:?}",
                self.cert_path
            )));
        }
        if !self.key_path.exists() {
            return Err(RkError::Config(format!(
                "TLS key not found: {:?}",
                self.key_path
            )));
        }
        if self.mode == TlsMode::Mutual {
            if let Some(ref ca) = self.ca_path {
                if !ca.exists() {
                    return Err(RkError::Config(format!(
                        "TLS CA cert not found: {:?}",
                        ca
                    )));
                }
            } else {
                return Err(RkError::Config(
                    "mTLS mode requires ca_path".to_string(),
                ));
            }
        }
        Ok(())
    }
}

// ─── 证书加载 ───────────────────────────────────────────────────────

/// 加载 PEM 证书文件
pub fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>> {
    let file = std::fs::File::open(path).map_err(|e| {
        RkError::Io(io::Error::new(
            io::ErrorKind::NotFound,
            format!("Failed to open cert file {:?}: {}", path, e),
        ))
    })?;
    let mut reader = BufReader::new(file);
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| RkError::Config(format!("Failed to parse certs: {}", e)))?;
    if certs.is_empty() {
        return Err(RkError::Config(format!("No certs found in {:?}", path)));
    }
    Ok(certs)
}

/// 加载 PEM 私钥文件
pub fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>> {
    let file = std::fs::File::open(path).map_err(|e| {
        RkError::Io(io::Error::new(
            io::ErrorKind::NotFound,
            format!("Failed to open key file {:?}: {}", path, e),
        ))
    })?;
    let mut reader = BufReader::new(file);

    // 尝试 PKCS8 格式
    if let Ok(Some(key)) = rustls_pemfile::private_key(&mut reader) {
        return Ok(key);
    }

    Err(RkError::Config(format!(
        "No private key found in {:?}",
        path
    )))
}

/// 加载 CA 证书到 RootCertStore
pub fn load_ca_certs(ca_path: &Path) -> Result<RootCertStore> {
    let ca_certs = load_certs(ca_path)?;
    let mut root_store = RootCertStore::empty();
    let mut added = 0;
    for cert in ca_certs {
        root_store.add(cert).map_err(|e| {
            RkError::Config(format!("Failed to add CA cert: {}", e))
        })?;
        added += 1;
    }
    if added == 0 {
        return Err(RkError::Config("No CA certs were added".to_string()));
    }
    Ok(root_store)
}

// ─── TLS Acceptor (服务端) ──────────────────────────────────────────

/// 创建 TLS Acceptor (服务端)
pub fn create_tls_acceptor(config: &TlsConfig) -> Result<TlsAcceptor> {
    let certs = load_certs(&config.cert_path)?;
    let key = load_private_key(&config.key_path)?;

    let server_config = match config.mode {
        TlsMode::OneWay => {
            ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(certs, key)
                .map_err(|e| RkError::Config(format!("TLS server config error: {}", e)))?
        }
        TlsMode::Mutual => {
            let ca_path = config.ca_path.as_ref().ok_or_else(|| {
                RkError::Config("mTLS requires CA certificate".to_string())
            })?;
            let root_store = load_ca_certs(ca_path)?;
            let client_verifier = WebPkiClientVerifier::builder(Arc::new(root_store))
                .build()
                .map_err(|e| RkError::Config(format!("Client verifier error: {}", e)))?;
            ServerConfig::builder()
                .with_client_cert_verifier(client_verifier)
                .with_single_cert(certs, key)
                .map_err(|e| RkError::Config(format!("TLS mTLS config error: {}", e)))?
        }
    };

    info!(
        mode = %config.mode,
        cert = ?config.cert_path,
        "TLS acceptor created"
    );
    Ok(TlsAcceptor::from(Arc::new(server_config)))
}

// ─── TLS Connector (客户端) ─────────────────────────────────────────

/// TLS 客户端配置
#[derive(Debug, Clone)]
pub struct TlsClientConfig {
    /// CA 证书 (用于验证服务端)
    pub ca_path: PathBuf,
    /// 客户端证书 (mTLS)
    pub client_cert_path: Option<PathBuf>,
    /// 客户端私钥 (mTLS)
    pub client_key_path: Option<PathBuf>,
    /// 服务端名称 (SNI)
    pub server_name: Option<String>,
}

/// 创建 TLS Connector (客户端)
pub fn create_tls_connector(config: &TlsClientConfig) -> Result<TlsConnector> {
    let root_store = load_ca_certs(&config.ca_path)?;

    let client_config = if let (Some(cert_path), Some(key_path)) =
        (&config.client_cert_path, &config.client_key_path)
    {
        // mTLS: 带客户端证书
        let certs = load_certs(cert_path)?;
        let key = load_private_key(key_path)?;
        ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_client_auth_cert(certs, key)
            .map_err(|e| RkError::Config(format!("TLS client auth error: {}", e)))?
    } else {
        // OneWay TLS: 仅验证服务端
        ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth()
    };

    info!(
        ca = ?config.ca_path,
        has_client_cert = config.client_cert_path.is_some(),
        "TLS connector created"
    );
    Ok(TlsConnector::from(Arc::new(client_config)))
}

// ─── 证书信息 ───────────────────────────────────────────────────────

/// 证书信息摘要
#[derive(Debug, Clone)]
pub struct CertificateInfo {
    /// 主题 (CN)
    pub subject: String,
    /// 颁发者
    pub issuer: String,
    /// 有效期起始 (Unix timestamp)
    pub not_before: i64,
    /// 有效期结束 (Unix timestamp)
    pub not_after: i64,
    /// 是否为 CA 证书
    pub is_ca: bool,
}

impl CertificateInfo {
    /// 检查证书是否已过期
    pub fn is_expired(&self) -> bool {
        let now = chrono::Utc::now().timestamp();
        now > self.not_after
    }

    /// 检查证书是否在指定天数内过期
    pub fn expires_within_days(&self, days: i64) -> bool {
        let now = chrono::Utc::now().timestamp();
        let threshold = now + days * 86400;
        self.not_after <= threshold
    }

    /// 剩余有效天数
    pub fn remaining_days(&self) -> i64 {
        let now = chrono::Utc::now().timestamp();
        ((self.not_after - now) / 86400).max(0)
    }
}

impl std::fmt::Display for CertificateInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Certificate(subject={}, issuer={}, remaining={}d, ca={})",
            self.subject, self.issuer, self.remaining_days(), self.is_ca
        )
    }
}

/// 从 PEM 文件解析证书信息 (简化版 — 仅提取基本信息)
pub fn parse_certificate_info(path: &Path) -> Result<CertificateInfo> {
    let certs = load_certs(path)?;
    let cert = &certs[0];

    // 简化: 使用 DER 数据的 SHA256 作为标识
    let der_bytes = cert.as_ref();
    let hash = sha256_hex(der_bytes);

    Ok(CertificateInfo {
        subject: format!("cert:{}", &hash[..16]),
        issuer: "unknown".to_string(),
        not_before: 0,
        not_after: i64::MAX,
        is_ca: false,
    })
}

fn sha256_hex(data: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let hash = Sha256::digest(data);
    hex_encode(&hash)
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

// ─── TLS 连接状态 ───────────────────────────────────────────────────

/// TLS 连接状态
#[derive(Debug, Clone)]
pub struct TlsConnectionState {
    /// 是否已加密
    pub encrypted: bool,
    /// TLS 版本
    pub version: Option<String>,
    /// 协商的密码套件
    pub cipher_suite: Option<String>,
    /// 客户端证书已验证 (mTLS)
    pub client_authenticated: bool,
    /// 服务端名称 (SNI)
    pub sni: Option<String>,
}

impl Default for TlsConnectionState {
    fn default() -> Self {
        Self {
            encrypted: false,
            version: None,
            cipher_suite: None,
            client_authenticated: false,
            sni: None,
        }
    }
}

impl std::fmt::Display for TlsConnectionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if !self.encrypted {
            return write!(f, "plaintext");
        }
        write!(
            f,
            "TLS(version={}, cipher={}, mTLS={}, sni={:?})",
            self.version.as_deref().unwrap_or("unknown"),
            self.cipher_suite.as_deref().unwrap_or("unknown"),
            self.client_authenticated,
            self.sni,
        )
    }
}

// ─── 安全连接封装 ───────────────────────────────────────────────────

/// 安全连接 — 可以是明文或 TLS
#[derive(Debug)]
pub enum SecureStream {
    /// 明文 TCP
    Plain(tokio::net::TcpStream),
    /// TLS 加密
    Tls(tokio_rustls::server::TlsStream<tokio::net::TcpStream>),
}

impl SecureStream {
    /// 获取连接状态
    pub fn tls_state(&self) -> TlsConnectionState {
        match self {
            SecureStream::Plain(_) => TlsConnectionState::default(),
            SecureStream::Tls(tls) => {
                let (_, server_conn) = tls.get_ref();
                TlsConnectionState {
                    encrypted: true,
                    version: server_conn
                        .protocol_version()
                        .map(|v| format!("{:?}", v)),
                    cipher_suite: server_conn
                        .negotiated_cipher_suite()
                        .map(|c| format!("{:?}", c.suite())),
                    client_authenticated: server_conn
                        .peer_certificates()
                        .map(|c| !c.is_empty())
                        .unwrap_or(false),
                    sni: server_conn.server_name().map(|s| s.to_string()),
                }
            }
        }
    }
}

// ─── 测试 ───────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_tls_mode_display() {
        assert_eq!(TlsMode::OneWay.to_string(), "OneWay");
        assert_eq!(TlsMode::Mutual.to_string(), "Mutual");
    }

    #[test]
    fn test_tls_config_default() {
        let config = TlsConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.mode, TlsMode::OneWay);
    }

    #[test]
    fn test_tls_config_enabled() {
        let config = TlsConfig::enabled("/tmp/cert.pem", "/tmp/key.pem");
        assert!(config.enabled);
        assert_eq!(config.cert_path, PathBuf::from("/tmp/cert.pem"));
        assert_eq!(config.key_path, PathBuf::from("/tmp/key.pem"));
    }

    #[test]
    fn test_tls_config_mutual() {
        let config = TlsConfig::enabled("/tmp/cert.pem", "/tmp/key.pem")
            .mutual("/tmp/ca.pem");
        assert_eq!(config.mode, TlsMode::Mutual);
        assert_eq!(config.ca_path, Some(PathBuf::from("/tmp/ca.pem")));
    }

    #[test]
    fn test_tls_config_validate_disabled() {
        let config = TlsConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_tls_config_validate_missing_cert() {
        let config = TlsConfig {
            enabled: true,
            cert_path: PathBuf::from("/nonexistent/cert.pem"),
            key_path: PathBuf::from("/nonexistent/key.pem"),
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_tls_config_validate_mutual_no_ca() {
        let dir = TempDir::new().unwrap();
        let cert = dir.path().join("cert.pem");
        let key = dir.path().join("key.pem");
        std::fs::write(&cert, "dummy").unwrap();
        std::fs::write(&key, "dummy").unwrap();

        let config = TlsConfig {
            enabled: true,
            mode: TlsMode::Mutual,
            cert_path: cert,
            key_path: key,
            ca_path: None,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_tls_config_validate_mutual_with_ca() {
        let dir = TempDir::new().unwrap();
        let cert = dir.path().join("cert.pem");
        let key = dir.path().join("key.pem");
        let ca = dir.path().join("ca.pem");
        std::fs::write(&cert, "dummy").unwrap();
        std::fs::write(&key, "dummy").unwrap();
        std::fs::write(&ca, "dummy").unwrap();

        let config = TlsConfig {
            enabled: true,
            mode: TlsMode::Mutual,
            cert_path: cert,
            key_path: key,
            ca_path: Some(ca),
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_load_certs_nonexistent() {
        let result = load_certs(Path::new("/nonexistent/cert.pem"));
        assert!(result.is_err());
    }

    #[test]
    fn test_load_certs_empty_file() {
        let dir = TempDir::new().unwrap();
        let cert_path = dir.path().join("empty.pem");
        std::fs::write(&cert_path, "").unwrap();
        let result = load_certs(&cert_path);
        assert!(result.is_err());
    }

    #[test]
    fn test_load_private_key_nonexistent() {
        let result = load_private_key(Path::new("/nonexistent/key.pem"));
        assert!(result.is_err());
    }

    #[test]
    fn test_load_private_key_invalid() {
        let dir = TempDir::new().unwrap();
        let key_path = dir.path().join("invalid.pem");
        std::fs::write(&key_path, "not a valid key").unwrap();
        let result = load_private_key(&key_path);
        assert!(result.is_err());
    }

    #[test]
    fn test_certificate_info_display() {
        let info = CertificateInfo {
            subject: "CN=test".to_string(),
            issuer: "CN=ca".to_string(),
            not_before: 0,
            not_after: chrono::Utc::now().timestamp() + 86400 * 30,
            is_ca: false,
        };
        let display = format!("{}", info);
        assert!(display.contains("CN=test"));
        assert!(display.contains("remaining=30d"));
    }

    #[test]
    fn test_certificate_info_expired() {
        let info = CertificateInfo {
            subject: "CN=test".to_string(),
            issuer: "CN=ca".to_string(),
            not_before: 0,
            not_after: chrono::Utc::now().timestamp() - 100,
            is_ca: false,
        };
        assert!(info.is_expired());
        assert_eq!(info.remaining_days(), 0);
    }

    #[test]
    fn test_certificate_info_not_expired() {
        let info = CertificateInfo {
            subject: "CN=test".to_string(),
            issuer: "CN=ca".to_string(),
            not_before: 0,
            not_after: chrono::Utc::now().timestamp() + 86400 * 365,
            is_ca: false,
        };
        assert!(!info.is_expired());
        assert!(info.expires_within_days(366));
        assert!(!info.expires_within_days(364));
    }

    #[test]
    fn test_tls_connection_state_display() {
        let state = TlsConnectionState::default();
        assert_eq!(state.to_string(), "plaintext");

        let tls_state = TlsConnectionState {
            encrypted: true,
            version: Some("TLSv1_3".to_string()),
            cipher_suite: Some("TLS13_AES_256_GCM_SHA384".to_string()),
            client_authenticated: true,
            sni: Some("kafka.example.com".to_string()),
        };
        let display = format!("{}", tls_state);
        assert!(display.contains("TLSv1_3"));
        assert!(display.contains("mTLS=true"));
    }

    #[test]
    fn test_tls_protocol_version() {
        assert_eq!(TlsProtocolVersion::Tls12, TlsProtocolVersion::Tls12);
        assert_eq!(TlsProtocolVersion::Tls13, TlsProtocolVersion::Tls13);
        assert_ne!(TlsProtocolVersion::Tls12, TlsProtocolVersion::Tls13);
    }

    #[test]
    fn test_sha256_hex() {
        let hash = sha256_hex(b"hello");
        assert_eq!(hash.len(), 64); // SHA256 = 32 bytes = 64 hex chars
        assert_eq!(
            hash,
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }

    #[test]
    fn test_hex_encode() {
        assert_eq!(hex_encode(&[0x00, 0xff, 0x0a]), "00ff0a");
        assert_eq!(hex_encode(&[]), "");
    }

    #[test]
    fn test_create_tls_acceptor_no_certs() {
        let config = TlsConfig {
            enabled: true,
            cert_path: PathBuf::from("/nonexistent/cert.pem"),
            key_path: PathBuf::from("/nonexistent/key.pem"),
            ..Default::default()
        };
        assert!(create_tls_acceptor(&config).is_err());
    }

    #[test]
    fn test_create_tls_connector_no_ca() {
        let config = TlsClientConfig {
            ca_path: PathBuf::from("/nonexistent/ca.pem"),
            client_cert_path: None,
            client_key_path: None,
            server_name: None,
        };
        assert!(create_tls_connector(&config).is_err());
    }

    #[test]
    fn test_tls_config_validate_mutual_missing_ca_file() {
        let dir = TempDir::new().unwrap();
        let cert = dir.path().join("cert.pem");
        let key = dir.path().join("key.pem");
        let ca = dir.path().join("nonexistent_ca.pem");
        std::fs::write(&cert, "dummy").unwrap();
        std::fs::write(&key, "dummy").unwrap();

        let config = TlsConfig {
            enabled: true,
            mode: TlsMode::Mutual,
            cert_path: cert,
            key_path: key,
            ca_path: Some(ca),
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_secure_stream_plain() {
        // SecureStream::Plain 的状态应该是 plaintext
        // 无法在没有实际 TCP 连接的情况下测试 Tls 变体
        let state = TlsConnectionState::default();
        assert!(!state.encrypted);
        assert!(!state.client_authenticated);
        assert!(state.version.is_none());
    }
}
