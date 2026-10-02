//! SASL 认证器
//!
//! 支持 SASL/PLAIN 机制 (RFC 4616)。
//! Phase 1: 静态用户凭证配置。
//! Phase 2: 集成外部认证提供者 (LDAP, OAuth, etc.)

use std::collections::HashMap;

use dashmap::DashMap;
use tracing::{debug, info, warn};

/// SASL 认证机制
#[derive(Debug, Clone, PartialEq)]
pub enum SaslMechanism {
    Plain,
    ScramSha256,
    ScramSha512,
}

impl SaslMechanism {
    pub fn as_str(&self) -> &str {
        match self {
            SaslMechanism::Plain => "PLAIN",
            SaslMechanism::ScramSha256 => "SCRAM-SHA-256",
            SaslMechanism::ScramSha512 => "SCRAM-SHA-512",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "PLAIN" => Some(SaslMechanism::Plain),
            "SCRAM-SHA-256" => Some(SaslMechanism::ScramSha256),
            "SCRAM-SHA-512" => Some(SaslMechanism::ScramSha512),
            _ => None,
        }
    }
}

/// 用户凭证
#[derive(Debug, Clone)]
pub struct UserCredential {
    pub username: String,
    pub password: String,
}

/// SASL 认证器配置
pub struct SaslAuthenticator {
    /// 支持的认证机制列表
    supported_mechanisms: Vec<SaslMechanism>,
    /// 用户凭证存储 (username → password)
    credentials: DashMap<String, String>,
    /// 是否启用认证 (false = 允许匿名)
    auth_enabled: bool,
}

impl Default for SaslAuthenticator {
    fn default() -> Self {
        Self::new()
    }
}

impl SaslAuthenticator {
    /// 创建认证器 (默认支持 PLAIN)
    pub fn new() -> Self {
        Self {
            supported_mechanisms: vec![SaslMechanism::Plain],
            credentials: DashMap::new(),
            auth_enabled: false,
        }
    }

    /// 创建启用认证的认证器
    pub fn with_auth(credentials: HashMap<String, String>) -> Self {
        let map = DashMap::new();
        for (username, password) in &credentials {
            map.insert(username.clone(), password.clone());
        }
        info!(
            users = credentials.len(),
            "SASL authenticator initialized with credentials"
        );
        Self {
            supported_mechanisms: vec![SaslMechanism::Plain],
            credentials: map,
            auth_enabled: true,
        }
    }

    /// 是否启用认证
    pub fn is_auth_enabled(&self) -> bool {
        self.auth_enabled
    }

    /// 获取支持的机制列表
    pub fn supported_mechanism_names(&self) -> Vec<String> {
        self.supported_mechanisms
            .iter()
            .map(|m| m.as_str().to_string())
            .collect()
    }

    /// 检查机制是否支持
    pub fn is_mechanism_supported(&self, mechanism: &str) -> bool {
        self.supported_mechanisms
            .iter()
            .any(|m| m.as_str().eq_ignore_ascii_case(mechanism))
    }

    /// 验证 SASL/PLAIN 凭证
    ///
    /// auth_bytes 格式: \0username\0password (RFC 4616)
    pub fn authenticate_plain(&self, auth_bytes: &[u8]) -> AuthResult {
        // 解析 SASL/PLAIN 格式
        let parts: Vec<&[u8]> = auth_bytes.splitn(3, |&b| b == 0).collect();
        if parts.len() != 3 {
            debug!("SASL/PLAIN: invalid format (expected 3 null-separated parts)");
            return AuthResult::Failure("Invalid SASL/PLAIN format".to_string());
        }

        let _authzid = String::from_utf8_lossy(parts[0]);
        let username = String::from_utf8_lossy(parts[1]).to_string();
        let password = String::from_utf8_lossy(parts[2]).to_string();

        if username.is_empty() {
            debug!("SASL/PLAIN: empty username");
            return AuthResult::Failure("Empty username".to_string());
        }

        // 查找凭证
        match self.credentials.get(&username) {
            Some(stored_password) => {
                if stored_password.value() == &password {
                    info!(username = %username, "SASL/PLAIN authentication successful");
                    AuthResult::Success(username)
                } else {
                    warn!(username = %username, "SASL/PLAIN authentication failed: wrong password");
                    AuthResult::Failure("Invalid password".to_string())
                }
            }
            None => {
                // 如果未配置任何用户，允许任意凭证 (开发模式)
                if self.credentials.is_empty() {
                    debug!(username = %username, "SASL/PLAIN: no credentials configured, allowing (dev mode)");
                    AuthResult::Success(username)
                } else {
                    warn!(username = %username, "SASL/PLAIN authentication failed: user not found");
                    AuthResult::Failure("User not found".to_string())
                }
            }
        }
    }

    /// 添加用户凭证
    pub fn add_credential(&self, username: String, password: String) {
        self.credentials.insert(username, password);
    }

    /// 移除用户凭证
    pub fn remove_credential(&self, username: &str) {
        self.credentials.remove(username);
    }
}

/// 认证结果
#[derive(Debug, Clone)]
pub enum AuthResult {
    /// 认证成功: 用户名
    Success(String),
    /// 认证失败: 原因
    Failure(String),
}

impl AuthResult {
    pub fn is_success(&self) -> bool {
        matches!(self, AuthResult::Success(_))
    }

    pub fn username(&self) -> Option<&str> {
        match self {
            AuthResult::Success(u) => Some(u),
            AuthResult::Failure(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sasl_mechanism_from_str() {
        assert_eq!(SaslMechanism::parse("PLAIN"), Some(SaslMechanism::Plain));
        assert_eq!(
            SaslMechanism::parse("SCRAM-SHA-256"),
            Some(SaslMechanism::ScramSha256)
        );
        assert_eq!(
            SaslMechanism::parse("SCRAM-SHA-512"),
            Some(SaslMechanism::ScramSha512)
        );
        assert_eq!(SaslMechanism::parse("UNKNOWN"), None);
    }

    #[test]
    fn test_sasl_mechanism_as_str() {
        assert_eq!(SaslMechanism::Plain.as_str(), "PLAIN");
        assert_eq!(SaslMechanism::ScramSha256.as_str(), "SCRAM-SHA-256");
        assert_eq!(SaslMechanism::ScramSha512.as_str(), "SCRAM-SHA-512");
    }

    #[test]
    fn test_authenticator_default_no_auth() {
        let auth = SaslAuthenticator::new();
        assert!(!auth.is_auth_enabled());
        assert!(auth.is_mechanism_supported("PLAIN"));
        assert!(!auth.is_mechanism_supported("SCRAM-SHA-256"));
    }

    #[test]
    fn test_authenticator_with_credentials() {
        let mut creds = HashMap::new();
        creds.insert("admin".to_string(), "secret".to_string());
        let auth = SaslAuthenticator::with_auth(creds);
        assert!(auth.is_auth_enabled());
    }

    #[test]
    fn test_plain_auth_success() {
        let mut creds = HashMap::new();
        creds.insert("admin".to_string(), "secret123".to_string());
        let auth = SaslAuthenticator::with_auth(creds);

        let auth_bytes = b"\0admin\0secret123";
        let result = auth.authenticate_plain(auth_bytes);
        assert!(result.is_success());
        assert_eq!(result.username(), Some("admin"));
    }

    #[test]
    fn test_plain_auth_wrong_password() {
        let mut creds = HashMap::new();
        creds.insert("admin".to_string(), "secret123".to_string());
        let auth = SaslAuthenticator::with_auth(creds);

        let auth_bytes = b"\0admin\0wrong";
        let result = auth.authenticate_plain(auth_bytes);
        assert!(!result.is_success());
    }

    #[test]
    fn test_plain_auth_user_not_found() {
        let mut creds = HashMap::new();
        creds.insert("admin".to_string(), "secret".to_string());
        let auth = SaslAuthenticator::with_auth(creds);

        let auth_bytes = b"\0unknown\0pass";
        let result = auth.authenticate_plain(auth_bytes);
        assert!(!result.is_success());
    }

    #[test]
    fn test_plain_auth_dev_mode_no_credentials() {
        // 无凭证配置时允许任意登录 (开发模式)
        let auth = SaslAuthenticator::new();
        let auth_bytes = b"\0anyuser\0anypass";
        let result = auth.authenticate_plain(auth_bytes);
        assert!(result.is_success());
        assert_eq!(result.username(), Some("anyuser"));
    }

    #[test]
    fn test_plain_auth_invalid_format() {
        let auth = SaslAuthenticator::new();
        let auth_bytes = b"no_null_separators";
        let result = auth.authenticate_plain(auth_bytes);
        assert!(!result.is_success());
    }

    #[test]
    fn test_plain_auth_empty_username() {
        let auth = SaslAuthenticator::new();
        let auth_bytes = b"\0\0password";
        let result = auth.authenticate_plain(auth_bytes);
        assert!(!result.is_success());
    }

    #[test]
    fn test_plain_auth_with_authzid() {
        let mut creds = HashMap::new();
        creds.insert("admin".to_string(), "pass".to_string());
        let auth = SaslAuthenticator::with_auth(creds);

        let auth_bytes = b"authz_user\0admin\0pass";
        let result = auth.authenticate_plain(auth_bytes);
        assert!(result.is_success());
        assert_eq!(result.username(), Some("admin"));
    }

    #[test]
    fn test_add_remove_credential() {
        let auth = SaslAuthenticator::new();
        auth.add_credential("user1".to_string(), "pass1".to_string());
        assert_eq!(auth.credentials.len(), 1);

        auth.remove_credential("user1");
        assert_eq!(auth.credentials.len(), 0);
    }

    #[test]
    fn test_supported_mechanism_names() {
        let auth = SaslAuthenticator::new();
        let names = auth.supported_mechanism_names();
        assert_eq!(names, vec!["PLAIN".to_string()]);
    }
}
