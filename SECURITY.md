# Security Policy

## Supported Versions

| Version | Supported |
|:---|:---|
| 0.1.x | Initial development release |

## Reporting a Vulnerability

We take security seriously. If you discover a security vulnerability in R-Kafka, please report it responsibly.

### How to Report

**Do NOT open a public GitHub issue for security vulnerabilities.**

Please report vulnerabilities via email to the project maintainers. Include:

- Description of the vulnerability
- Steps to reproduce or proof of concept
- Potential impact assessment
- Suggested fix (if any)

### What to Expect

- **Acknowledgment**: We will acknowledge receipt within 48 hours
- **Assessment**: We will evaluate the severity and impact within 7 days
- **Fix Timeline**: Critical vulnerabilities will be addressed as soon as possible
- **Disclosure**: We will coordinate with you on a public disclosure timeline

### Security Considerations

R-Kafka implements network-facing protocol handling. Areas of particular sensitivity:

- **Protocol parsing** — malformed input handling in `rk-protocol`
- **Authentication** — SASL/TLS implementation in `rk-security`
- **Network I/O** — connection handling in `rk-network`
- **Storage I/O** — file system operations in `rk-storage`

### Security Best Practices for Deployment

- Enable TLS/mTLS for all inter-broker and client-broker communication
- Use SASL SCRAM-SHA-256 or SCRAM-SHA-512 for authentication (prefer over PLAIN)
- Configure ACL authorization for production deployments
- Keep R-Kafka updated to the latest release
- Monitor audit logs for suspicious activity
