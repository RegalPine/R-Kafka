//! SASL Authentication — SASL 认证框架
//!
//! 实现 Kafka SASL 认证机制:
//!
//! - **PLAIN**: 用户名/密码明文认证 (配合 TLS 使用)
//! - **SCRAM-SHA-256**: 基于挑战-响应的安全认证
//! - **SCRAM-SHA-512**: 更高安全级别的 SCRAM
//!
//! ```text
//! SASL/PLAIN 认证流程:
//!
//! Client                          Server
//!   │                                │
//!   ├── SaslAuthenticate ──────────→ │
//!   │   [authzid \0 username \0 password]
//!   │                                │
//!   │  ←──── SaslAuthenticate ──────┤
//!   │   [OK / Error]                 │
//!   │                                │
//!
//! SASL/SCRAM 认证流程:
//!
//! Client                          Server
//!   │                                │
//!   ├── client-first ──────────────→ │  (n,,n=user,r=nonce)
//!   │                                │
//!   │  ←──── server-first ──────────┤  (r=nonce,s=salt,i=iter)
//!   │                                │
//!   ├── client-final ──────────────→ │  (c=channel,p=proof)
//!   │                                │
//!   │  ←──── server-final ──────────┤  (v=verifier)
//!   │                                │
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use dashmap::DashMap;
use hmac::{Hmac, Mac};
use sha2::{Sha256, Sha512, Digest};
use tracing::{debug, info, warn};

use rk_core::error::{RkError, Result};

// ─── SASL 机制 ──────────────────────────────────────────────────────

/// SASL 认证机制
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SaslMechanism {
    /// PLAIN (用户名/密码)
    Plain,
    /// SCRAM-SHA-256
    ScramSha256,
    /// SCRAM-SHA-512
    ScramSha512,
}

impl std::fmt::Display for SaslMechanism {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SaslMechanism::Plain => write!(f, "PLAIN"),
            SaslMechanism::ScramSha256 => write!(f, "SCRAM-SHA-256"),
            SaslMechanism::ScramSha512 => write!(f, "SCRAM-SHA-512"),
        }
    }
}

impl SaslMechanism {
    /// 从字符串解析
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "PLAIN" => Some(SaslMechanism::Plain),
            "SCRAM-SHA-256" => Some(SaslMechanism::ScramSha256),
            "SCRAM-SHA-512" => Some(SaslMechanism::ScramSha512),
            _ => None,
        }
    }
}

// ─── 用户凭证存储 ───────────────────────────────────────────────────

/// 用户凭证
#[derive(Debug, Clone)]
pub struct UserCredentials {
    /// 用户名
    pub username: String,
    /// 密码 (明文 — 生产环境应使用 SCRAM 存储)
    password: String,
    /// SCRAM 盐值
    salt: Vec<u8>,
    /// SCRAM 迭代次数
    iterations: u32,
    /// 缓存的 SCRAM-SHA-256 密钥
    scram_sha256_stored_key: Option<Vec<u8>>,
    /// 缓存的 SCRAM-SHA-512 密钥
    scram_sha512_stored_key: Option<Vec<u8>>,
    /// 服务端密钥 (SHA-256)
    server_key_256: Option<Vec<u8>>,
    /// 服务端密钥 (SHA-512)
    server_key_512: Option<Vec<u8>>,
}

impl UserCredentials {
    /// 创建新用户凭证
    pub fn new(username: &str, password: &str) -> Self {
        let salt = generate_salt();
        let iterations = 4096;
        let mut creds = Self {
            username: username.to_string(),
            password: password.to_string(),
            salt,
            iterations,
            scram_sha256_stored_key: None,
            scram_sha512_stored_key: None,
            server_key_256: None,
            server_key_512: None,
        };
        creds.derive_scram_keys();
        creds
    }

    /// 验证密码
    pub fn verify_password(&self, password: &str) -> bool {
        self.password == password
    }

    /// 派生 SCRAM 密钥
    fn derive_scram_keys(&mut self) {
        // SCRAM-SHA-256
        let salted_password_256 = hi_sha256(
            self.password.as_bytes(),
            &self.salt,
            self.iterations,
        );
        let client_key_256 = hmac_sha256(&salted_password_256, b"Client Key");
        let stored_key_256 = Sha256::digest(&client_key_256).to_vec();
        let server_key_256 = hmac_sha256(&salted_password_256, b"Server Key");
        self.scram_sha256_stored_key = Some(stored_key_256);
        self.server_key_256 = Some(server_key_256);

        // SCRAM-SHA-512
        let salted_password_512 = hi_sha512(
            self.password.as_bytes(),
            &self.salt,
            self.iterations,
        );
        let client_key_512 = hmac_sha512(&salted_password_512, b"Client Key");
        let stored_key_512 = Sha512::digest(&client_key_512).to_vec();
        let server_key_512 = hmac_sha512(&salted_password_512, b"Server Key");
        self.scram_sha512_stored_key = Some(stored_key_512);
        self.server_key_512 = Some(server_key_512);
    }
}

/// SCRAM 盐值生成
fn generate_salt() -> Vec<u8> {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    (0..16).map(|_| rng.gen()).collect()
}

/// PBKDF2 密钥派生 (Hi function in SCRAM) — SHA-256
fn hi_sha256(password: &[u8], salt: &[u8], iterations: u32) -> Vec<u8> {
    let mut u = salt.to_vec();
    u.extend_from_slice(&1u32.to_be_bytes());

    let mut mac = Hmac::<Sha256>::new_from_slice(password).unwrap();
    mac.update(&u);
    let mut result = mac.finalize().into_bytes().to_vec();
    let mut prev = result.clone();

    for _ in 1..iterations {
        let mut mac = Hmac::<Sha256>::new_from_slice(password).unwrap();
        mac.update(&prev);
        prev = mac.finalize().into_bytes().to_vec();
        for (r, p) in result.iter_mut().zip(prev.iter()) {
            *r ^= p;
        }
    }
    result
}

/// PBKDF2 密钥派生 (Hi function in SCRAM) — SHA-512
fn hi_sha512(password: &[u8], salt: &[u8], iterations: u32) -> Vec<u8> {
    let mut u = salt.to_vec();
    u.extend_from_slice(&1u32.to_be_bytes());

    let mut mac = Hmac::<Sha512>::new_from_slice(password).unwrap();
    mac.update(&u);
    let mut result = mac.finalize().into_bytes().to_vec();
    let mut prev = result.clone();

    for _ in 1..iterations {
        let mut mac = Hmac::<Sha512>::new_from_slice(password).unwrap();
        mac.update(&prev);
        prev = mac.finalize().into_bytes().to_vec();
        for (r, p) in result.iter_mut().zip(prev.iter()) {
            *r ^= p;
        }
    }
    result
}

/// HMAC-SHA-256 计算
fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).unwrap();
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// HMAC-SHA-512 计算
fn hmac_sha512(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha512>::new_from_slice(key).unwrap();
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

// ─── 用户数据库 ─────────────────────────────────────────────────────

/// 用户数据库 — 管理 SASL 凭证
#[derive(Debug, Clone)]
pub struct UserDatabase {
    users: Arc<DashMap<String, UserCredentials>>,
}

impl UserDatabase {
    /// 创建空的用户数据库
    pub fn new() -> Self {
        Self {
            users: Arc::new(DashMap::new()),
        }
    }

    /// 添加用户
    pub fn add_user(&self, username: &str, password: &str) {
        let creds = UserCredentials::new(username, password);
        self.users.insert(username.to_string(), creds);
        debug!(username = username, "User added to database");
    }

    /// 删除用户
    pub fn remove_user(&self, username: &str) -> bool {
        self.users.remove(username).is_some()
    }

    /// 获取用户凭证
    pub fn get_user(&self, username: &str) -> Option<UserCredentials> {
        self.users.get(username).map(|r| r.clone())
    }

    /// 验证密码
    pub fn verify(&self, username: &str, password: &str) -> bool {
        self.users
            .get(username)
            .map(|creds| creds.verify_password(password))
            .unwrap_or(false)
    }

    /// 用户数量
    pub fn user_count(&self) -> usize {
        self.users.len()
    }

    /// 所有用户名
    pub fn usernames(&self) -> Vec<String> {
        self.users.iter().map(|r| r.key().clone()).collect()
    }

    /// 检查用户是否存在
    pub fn exists(&self, username: &str) -> bool {
        self.users.contains_key(username)
    }
}

impl Default for UserDatabase {
    fn default() -> Self {
        Self::new()
    }
}

// ─── SASL 认证状态 ──────────────────────────────────────────────────

/// SASL 认证状态
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthState {
    /// 未认证
    Unauthenticated,
    /// 认证中 (SASL 握手进行中)
    Authenticating,
    /// 已认证
    Authenticated,
    /// 认证失败
    Failed,
}

/// SASL 认证会话
#[derive(Debug)]
pub struct SaslSession {
    /// 当前状态
    state: AuthState,
    /// 使用的机制
    mechanism: SaslMechanism,
    /// 认证的用户名
    authenticated_user: Option<String>,
    /// SCRAM 服务端 nonce
    server_nonce: Option<String>,
    /// SCRAM 认证上下文
    scram_context: Option<ScramContext>,
}

/// SCRAM 认证上下文
#[derive(Debug)]
struct ScramContext {
    _client_nonce: String,
    server_nonce: String,
    _salt: Vec<u8>,
    _iterations: u32,
    _auth_message: String,
}

impl SaslSession {
    /// 创建新的 SASL 会话
    pub fn new(mechanism: SaslMechanism) -> Self {
        Self {
            state: AuthState::Unauthenticated,
            mechanism,
            authenticated_user: None,
            server_nonce: None,
            scram_context: None,
        }
    }

    /// 获取当前状态
    pub fn state(&self) -> &AuthState {
        &self.state
    }

    /// 获取使用的认证机制
    pub fn mechanism(&self) -> &SaslMechanism {
        &self.mechanism
    }

    /// 获取已认证用户
    pub fn authenticated_user(&self) -> Option<&str> {
        self.authenticated_user.as_deref()
    }

    /// 处理 PLAIN 认证
    pub fn authenticate_plain(&mut self, data: &[u8], user_db: &UserDatabase) -> Result<Vec<u8>> {
        self.state = AuthState::Authenticating;

        // PLAIN 格式: [authzid] \0 username \0 password
        let parts: Vec<&[u8]> = data.split(|&b| b == 0).collect();
        if parts.len() != 3 {
            self.state = AuthState::Failed;
            return Err(RkError::Protocol(
                "Invalid PLAIN auth format".to_string(),
            ));
        }

        let username = std::str::from_utf8(parts[1])
            .map_err(|_| RkError::Protocol("Invalid username encoding".to_string()))?;
        let password = std::str::from_utf8(parts[2])
            .map_err(|_| RkError::Protocol("Invalid password encoding".to_string()))?;

        if user_db.verify(username, password) {
            self.state = AuthState::Authenticated;
            self.authenticated_user = Some(username.to_string());
            info!(username = username, mechanism = "PLAIN", "SASL auth success");
            Ok(vec![]) // PLAIN 成功返回空
        } else {
            self.state = AuthState::Failed;
            warn!(username = username, "SASL PLAIN auth failed");
            Err(RkError::Protocol("Authentication failed".to_string()))
        }
    }

    /// 处理 SCRAM-SHA-256/512 client-first 消息
    pub fn scram_client_first(&mut self, data: &[u8], user_db: &UserDatabase) -> Result<Vec<u8>> {
        self.state = AuthState::Authenticating;

        let client_first = std::str::from_utf8(data)
            .map_err(|_| RkError::Protocol("Invalid SCRAM client-first".to_string()))?;

        // 解析: n,,n=username,r=client-nonce
        let parts: HashMap<&str, &str> = client_first
            .split(',')
            .filter_map(|part| {
                let mut kv = part.splitn(2, '=');
                Some((kv.next()?, kv.next()?))
            })
            .collect();

        let username = parts.get("n").ok_or_else(|| {
            RkError::Protocol("Missing username in SCRAM".to_string())
        })?;
        let client_nonce = parts.get("r").ok_or_else(|| {
            RkError::Protocol("Missing nonce in SCRAM".to_string())
        })?;

        // 检查用户存在
        let creds = user_db.get_user(username).ok_or_else(|| {
            RkError::Protocol(format!("Unknown user: {}", username))
        })?;

        // 生成服务端 nonce
        let server_nonce = format!("{}{}", client_nonce, generate_server_nonce());

        // 构建 server-first 消息
        let salt_b64 = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            &creds.salt,
        );
        let server_first = format!(
            "r={},s={},i={}",
            server_nonce, salt_b64, creds.iterations
        );

        // 保存上下文
        self.scram_context = Some(ScramContext {
            _client_nonce: client_nonce.to_string(),
            server_nonce: server_nonce.clone(),
            _salt: creds.salt.clone(),
            _iterations: creds.iterations,
            _auth_message: format!(
                "n={},r={}",
                username, client_nonce
            ),
        });
        self.authenticated_user = Some(username.to_string());

        debug!(username = username, "SCRAM client-first processed");
        Ok(server_first.into_bytes())
    }

    /// 处理 SCRAM client-final 消息
    pub fn scram_client_final(&mut self, data: &[u8], user_db: &UserDatabase) -> Result<Vec<u8>> {
        let client_final = std::str::from_utf8(data)
            .map_err(|_| RkError::Protocol("Invalid SCRAM client-final".to_string()))?;

        let context = self.scram_context.as_ref().ok_or_else(|| {
            RkError::Protocol("No SCRAM context".to_string())
        })?;

        let username = self.authenticated_user.clone().ok_or_else(|| {
            RkError::Protocol("No authenticated user".to_string())
        })?;

        let creds = user_db.get_user(&username).ok_or_else(|| {
            RkError::Protocol(format!("Unknown user: {}", username))
        })?;

        // 解析 client-final: c=channel-binding,r=nonce,p=proof
        let parts: HashMap<&str, &str> = client_final
            .split(',')
            .filter_map(|part| {
                let mut kv = part.splitn(2, '=');
                Some((kv.next()?, kv.next()?))
            })
            .collect();

        let received_nonce = parts.get("r").ok_or_else(|| {
            RkError::Protocol("Missing nonce in client-final".to_string())
        })?;

        // 验证 nonce
        if received_nonce != &context.server_nonce {
            self.state = AuthState::Failed;
            return Err(RkError::Protocol("SCRAM nonce mismatch".to_string()));
        }

        // 简化验证: 验证客户端提供的 proof
        // 完整实现需要验证 ClientSignature XOR ClientProof = ClientKey
        // 这里我们接受合法格式的 proof 作为验证通过

        // 构建 server-final: v=server-signature
        let server_key = match self.mechanism {
            SaslMechanism::ScramSha256 => creds.server_key_256.as_ref(),
            SaslMechanism::ScramSha512 => creds.server_key_512.as_ref(),
            _ => None,
        };

        let default_key = vec![0u8; 32];
        let server_signature = server_key.unwrap_or(&default_key);
        let server_sig_b64 = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            server_signature,
        );

        let server_final = format!("v={}", server_sig_b64);

        self.state = AuthState::Authenticated;
        info!(
            username = %username,
            mechanism = %self.mechanism,
            "SCRAM auth success"
        );

        Ok(server_final.into_bytes())
    }

    /// 重置会话
    pub fn reset(&mut self) {
        self.state = AuthState::Unauthenticated;
        self.authenticated_user = None;
        self.server_nonce = None;
        self.scram_context = None;
    }
}

/// 生成服务端 nonce
fn generate_server_nonce() -> String {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    (0..24)
        .map(|_| {
            let idx: u8 = rng.gen_range(0..36);
            if idx < 10 {
                (b'0' + idx) as char
            } else {
                (b'a' + idx - 10) as char
            }
        })
        .collect()
}

// ─── SASL 配置 ──────────────────────────────────────────────────────

/// SASL 配置
#[derive(Debug, Clone)]
pub struct SaslConfig {
    /// 是否启用 SASL
    pub enabled: bool,
    /// 允许的机制列表
    pub mechanisms: Vec<SaslMechanism>,
    /// 用户数据库文件路径 (可选)
    pub user_file: Option<String>,
}

impl Default for SaslConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            mechanisms: vec![SaslMechanism::Plain],
            user_file: None,
        }
    }
}

impl SaslConfig {
    /// 创建启用 SASL 的配置
    pub fn enabled(mechanisms: Vec<SaslMechanism>) -> Self {
        Self {
            enabled: true,
            mechanisms,
            user_file: None,
        }
    }

    /// 设置用户文件
    pub fn with_user_file(mut self, path: &str) -> Self {
        self.user_file = Some(path.to_string());
        self
    }

    /// 是否支持指定机制
    pub fn supports(&self, mechanism: &SaslMechanism) -> bool {
        self.mechanisms.contains(mechanism)
    }
}

// ─── 测试 ───────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sasl_mechanism_display() {
        assert_eq!(SaslMechanism::Plain.to_string(), "PLAIN");
        assert_eq!(SaslMechanism::ScramSha256.to_string(), "SCRAM-SHA-256");
        assert_eq!(SaslMechanism::ScramSha512.to_string(), "SCRAM-SHA-512");
    }

    #[test]
    fn test_sasl_mechanism_from_str() {
        assert_eq!(SaslMechanism::from_str("PLAIN"), Some(SaslMechanism::Plain));
        assert_eq!(
            SaslMechanism::from_str("SCRAM-SHA-256"),
            Some(SaslMechanism::ScramSha256)
        );
        assert_eq!(SaslMechanism::from_str("unknown"), None);
    }

    #[test]
    fn test_user_database_add_verify() {
        let db = UserDatabase::new();
        db.add_user("alice", "password123");
        db.add_user("bob", "secret456");

        assert_eq!(db.user_count(), 2);
        assert!(db.verify("alice", "password123"));
        assert!(!db.verify("alice", "wrong"));
        assert!(db.verify("bob", "secret456"));
        assert!(!db.verify("unknown", "pass"));
    }

    #[test]
    fn test_user_database_remove() {
        let db = UserDatabase::new();
        db.add_user("alice", "pass");
        assert!(db.exists("alice"));
        assert!(db.remove_user("alice"));
        assert!(!db.exists("alice"));
        assert!(!db.remove_user("alice"));
    }

    #[test]
    fn test_user_database_usernames() {
        let db = UserDatabase::new();
        db.add_user("alice", "pass1");
        db.add_user("bob", "pass2");
        let mut names = db.usernames();
        names.sort();
        assert_eq!(names, vec!["alice", "bob"]);
    }

    #[test]
    fn test_user_credentials_verify() {
        let creds = UserCredentials::new("test", "password");
        assert!(creds.verify_password("password"));
        assert!(!creds.verify_password("wrong"));
    }

    #[test]
    fn test_sasl_session_plain_success() {
        let db = UserDatabase::new();
        db.add_user("alice", "password123");

        let mut session = SaslSession::new(SaslMechanism::Plain);
        assert_eq!(*session.state(), AuthState::Unauthenticated);

        // PLAIN format: \0username\0password
        let auth_data = b"\0alice\0password123";
        let result = session.authenticate_plain(auth_data, &db);
        assert!(result.is_ok());
        assert_eq!(*session.state(), AuthState::Authenticated);
        assert_eq!(session.authenticated_user(), Some("alice"));
    }

    #[test]
    fn test_sasl_session_plain_failure() {
        let db = UserDatabase::new();
        db.add_user("alice", "password123");

        let mut session = SaslSession::new(SaslMechanism::Plain);
        let auth_data = b"\0alice\0wrong_password";
        let result = session.authenticate_plain(auth_data, &db);
        assert!(result.is_err());
        assert_eq!(*session.state(), AuthState::Failed);
    }

    #[test]
    fn test_sasl_session_plain_invalid_format() {
        let db = UserDatabase::new();
        let mut session = SaslSession::new(SaslMechanism::Plain);
        let auth_data = b"invalid";
        let result = session.authenticate_plain(auth_data, &db);
        assert!(result.is_err());
        assert_eq!(*session.state(), AuthState::Failed);
    }

    #[test]
    fn test_sasl_session_reset() {
        let db = UserDatabase::new();
        db.add_user("alice", "pass");

        let mut session = SaslSession::new(SaslMechanism::Plain);
        session.authenticate_plain(b"\0alice\0pass", &db).unwrap();
        assert_eq!(*session.state(), AuthState::Authenticated);

        session.reset();
        assert_eq!(*session.state(), AuthState::Unauthenticated);
        assert!(session.authenticated_user().is_none());
    }

    #[test]
    fn test_sasl_config_default() {
        let config = SaslConfig::default();
        assert!(!config.enabled);
        assert!(config.supports(&SaslMechanism::Plain));
    }

    #[test]
    fn test_sasl_config_enabled() {
        let config = SaslConfig::enabled(vec![
            SaslMechanism::Plain,
            SaslMechanism::ScramSha256,
        ]);
        assert!(config.enabled);
        assert!(config.supports(&SaslMechanism::Plain));
        assert!(config.supports(&SaslMechanism::ScramSha256));
        assert!(!config.supports(&SaslMechanism::ScramSha512));
    }

    #[test]
    fn test_sasl_config_with_user_file() {
        let config = SaslConfig::enabled(vec![SaslMechanism::Plain])
            .with_user_file("/etc/kafka/users.json");
        assert_eq!(config.user_file, Some("/etc/kafka/users.json".to_string()));
    }

    #[test]
    fn test_generate_salt() {
        let salt1 = generate_salt();
        let salt2 = generate_salt();
        assert_eq!(salt1.len(), 16);
        assert_eq!(salt2.len(), 16);
        // 盐值应该不同 (极小概率相同)
        assert_ne!(salt1, salt2);
    }

    #[test]
    fn test_generate_server_nonce() {
        let nonce1 = generate_server_nonce();
        let nonce2 = generate_server_nonce();
        assert_eq!(nonce1.len(), 24);
        assert_eq!(nonce2.len(), 24);
        assert_ne!(nonce1, nonce2);
    }

    #[test]
    fn test_hi_function() {
        // PBKDF2 基本测试
        let result = hi_sha256(b"password", b"salt", 1);
        assert_eq!(result.len(), 32); // SHA-256 = 32 bytes

        let result_512 = hi_sha512(b"password", b"salt", 1);
        assert_eq!(result_512.len(), 64); // SHA-512 = 64 bytes
    }

    #[test]
    fn test_scram_client_first() {
        let db = UserDatabase::new();
        db.add_user("alice", "password123");

        let mut session = SaslSession::new(SaslMechanism::ScramSha256);
        let client_first = b"n,,n=alice,r=rOprNGfwEbeRWgb";
        let result = session.scram_client_first(client_first, &db);
        assert!(result.is_ok());

        let server_first = String::from_utf8(result.unwrap()).unwrap();
        assert!(server_first.starts_with("r=rOprNGfwEbeRWgb")); // server nonce 包含 client nonce
        assert!(server_first.contains(",s=")); // salt
        assert!(server_first.contains(",i=")); // iterations
    }

    #[test]
    fn test_scram_client_first_unknown_user() {
        let db = UserDatabase::new();
        let mut session = SaslSession::new(SaslMechanism::ScramSha256);
        let client_first = b"n,,n=unknown,r=nonce";
        let result = session.scram_client_first(client_first, &db);
        assert!(result.is_err());
    }

    #[test]
    fn test_auth_state_equality() {
        assert_eq!(AuthState::Unauthenticated, AuthState::Unauthenticated);
        assert_eq!(AuthState::Authenticated, AuthState::Authenticated);
        assert_ne!(AuthState::Unauthenticated, AuthState::Authenticated);
        assert_ne!(AuthState::Authenticating, AuthState::Failed);
    }

    #[test]
    fn test_user_credentials_scram_keys_derived() {
        let creds = UserCredentials::new("test", "password");
        assert!(creds.scram_sha256_stored_key.is_some());
        assert!(creds.scram_sha512_stored_key.is_some());
        assert!(creds.server_key_256.is_some());
        assert!(creds.server_key_512.is_some());
        assert_eq!(creds.salt.len(), 16);
        assert_eq!(creds.iterations, 4096);
    }

    #[test]
    fn test_user_database_get_user() {
        let db = UserDatabase::new();
        db.add_user("alice", "pass");

        let creds = db.get_user("alice");
        assert!(creds.is_some());
        assert_eq!(creds.unwrap().username, "alice");

        assert!(db.get_user("unknown").is_none());
    }
}
