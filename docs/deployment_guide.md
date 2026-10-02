# R-Kafka 部署指南

R-Kafka 是纯 Rust 实现的 Kafka 兼容分布式消息流引擎。本文档介绍各种部署场景。

---

## 目录

1. [快速开始 (单机部署)](#1-快速开始-单机部署)
2. [3 节点集群部署](#2-3-节点集群部署)
3. [TLS/mTLS 加密通信](#3-tlsmutex-tls-加密通信)
4. [SASL 认证 + ACL 授权](#4-sasl-认证--acl-授权)
5. [生产环境推荐配置](#5-生产环境推荐配置)
6. [系统调优](#6-系统调优)

---

## 1. 快速开始 (单机部署)

### 1.1 前置条件

- Rust 1.75+ (见 `rust-toolchain.toml`，workspace `rust-version = "1.75"`)
- Linux / macOS / WSL2
- 磁盘空间: 根据数据量 (默认数据目录 `/data/r-kafka`)

### 1.2 编译

```bash
cd R-Kafka
cargo build --release
```

### 1.3 配置

复制单机配置:

```bash
cp config/examples/single-node.toml config/r-kafka.toml
```

关键配置项:

```toml
[broker]
id = 1
host = "0.0.0.0"
port = 9092

[storage]
data_dir = "/var/lib/r-kafka/data"
```

### 1.4 启动

```bash
# 创建数据目录
mkdir -p /var/lib/r-kafka/data

# 启动 Broker
./target/release/r-kafka --config config/r-kafka.toml
```

### 1.5 验证

```bash
# 使用 Kafka 命令行工具验证
kafka-topics.sh --bootstrap-server localhost:9092 --list

# 创建 Topic
kafka-topics.sh --bootstrap-server localhost:9092 --create \
    --topic test-topic --partitions 3 --replication-factor 1

# 生产消息
kafka-console-producer.sh --bootstrap-server localhost:9092 --topic test-topic

# 消费消息
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test-topic --from-beginning
```

---

## 2. 3 节点集群部署

### 2.1 架构

```
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│  Broker 1    │  │  Broker 2    │  │  Broker 3    │
│  rack-a      │  │  rack-b      │  │  rack-c      │
│  id=1        │  │  id=2        │  │  id=3        │
│  :9092       │  │  :9092       │  │  :9092       │
│  :9093 (ctrl)│  │  :9093 (ctrl)│  │  :9093 (ctrl)│
└──────────────┘  └──────────────┘  └──────────────┘
       ▲                ▲                ▲
       └────────────────┼────────────────┘
                        │
              KRaft Quorum (Raft 共识)
```

### 2.2 节点配置

每个节点使用独立配置文件，修改以下字段:

**节点 1** (`config/examples/cluster-node1.toml`):
```toml
[broker]
id = 1
rack = "rack-a"

[controller]
quorum_peers = [
    "1@broker1.internal:9093",
    "2@broker2.internal:9093",
    "3@broker3.internal:9093",
]
```

**节点 2**: 修改 `id = 2`, `rack = "rack-b"`
**节点 3**: 修改 `id = 3`, `rack = "rack-c"`

### 2.3 启动集群

按顺序启动 (非强制，但推荐):

```bash
# 节点 1
ssh broker1 "mkdir -p /var/lib/r-kafka/data && ./r-kafka --config cluster-node1.toml"

# 节点 2
ssh broker2 "mkdir -p /var/lib/r-kafka/data && ./r-kafka --config cluster-node2.toml"

# 节点 3
ssh broker3 "mkdir -p /var/lib/r-kafka/data && ./r-kafka --config cluster-node3.toml"
```

### 2.4 创建高可用 Topic

```bash
kafka-topics.sh --bootstrap-server broker1:9092 --create \
    --topic orders --partitions 6 --replication-factor 3
```

### 2.5 DNS / Hosts 配置

确保节点间可通过主机名通信:

```
# /etc/hosts
10.0.1.1  broker1.internal
10.0.1.2  broker2.internal
10.0.1.3  broker3.internal
```

---

## 3. TLS/mTLS 加密通信

### 3.1 生成证书

```bash
# 1. 生成 CA
openssl req -new -x509 -keyout ca.key -out ca.crt -days 365 \
    -subj "/CN=R-Kafka-CA" -nodes

# 2. 生成 Broker 证书
openssl req -new -keyout broker.key -out broker.csr -nodes \
    -subj "/CN=broker1.internal"
openssl x509 -req -in broker.csr -CA ca.crt -CAkey ca.key \
    -CAcreateserial -out broker.crt -days 365

# 3. 放置证书
mkdir -p /etc/r-kafka/tls
cp broker.crt broker.key ca.crt /etc/r-kafka/tls/
chmod 600 /etc/r-kafka/tls/broker.key
```

### 3.2 One-way TLS 配置

```toml
[security]
tls_enabled = true
tls_mode = "one-way"
cert_path = "/etc/r-kafka/tls/broker.crt"
key_path = "/etc/r-kafka/tls/broker.key"
# ca_path 可选 (one-way 不验证客户端)
```

客户端连接:
```bash
kafka-console-producer.sh --bootstrap-server broker1:9093 \
    --producer-property security.protocol=SSL \
    --producer-property ssl.truststore.location=/path/to/truststore.jks
```

### 3.3 Mutual TLS (mTLS) 配置

```toml
[security]
tls_enabled = true
tls_mode = "mutual"
cert_path = "/etc/r-kafka/tls/broker.crt"
key_path = "/etc/r-kafka/tls/broker.key"
ca_path = "/etc/r-kafka/tls/ca.crt"     # 验证客户端证书
```

客户端需要额外提供客户端证书:
```bash
kafka-console-producer.sh --bootstrap-server broker1:9093 \
    --producer-property security.protocol=SSL \
    --producer-property ssl.truststore.location=/path/to/truststore.jks \
    --producer-property ssl.keystore.location=/path/to/keystore.jks
```

---

## 4. SASL 认证 + ACL 授权

### 4.1 创建用户

通过 R-Kafka 管理 API 或启动时配置创建用户:

```bash
# 使用 SCRAM-SHA-256 创建用户
# (通过 Admin API 或配置文件)
```

### 4.2 SASL 配置

```toml
[security]
sasl_enabled = true
sasl_mechanisms = ["SCRAM-SHA-256", "SCRAM-SHA-512"]
super_users = ["User:admin"]
```

### 4.3 ACL 授权

```toml
[security]
acl_enabled = true
```

ACL 规则格式:

```
ALLOW|DENY principal=User:<name> [host=<ip>] operation=<op> resource=<type>:<name>
```

示例:

```
# 允许 alice 读写 orders topic
ALLOW principal=User:alice operation=READ resource=Topic:orders
ALLOW principal=User:alice operation=WRITE resource=Topic:orders

# 允许 bob 消费 orders-group
ALLOW principal=User:bob operation=READ resource=Group:orders-group

# 拒绝 192.168.1.100 的所有操作
DENY principal=User:* host=192.168.1.100 operation=ALL resource=Topic:*
```

### 4.4 TLS + SASL 组合

推荐生产环境同时启用 TLS 和 SASL:

```toml
[security]
tls_enabled = true
tls_mode = "one-way"
cert_path = "/etc/r-kafka/tls/broker.crt"
key_path = "/etc/r-kafka/tls/broker.key"

sasl_enabled = true
sasl_mechanisms = ["SCRAM-SHA-512"]
acl_enabled = true
```

客户端连接:
```bash
kafka-console-producer.sh --bootstrap-server broker1:9093 \
    --producer-property security.protocol=SASL_SSL \
    --producer-property sasl.mechanism=SCRAM-SHA-512 \
    --producer-property sasl.jaas.config='org.apache.kafka.common.security.scram.ScramLoginModule required username="alice" password="secret";' \
    --producer-property ssl.truststore.location=/path/to/truststore.jks
```

---

## 5. 生产环境推荐配置

参见 `config/examples/production.toml`。

关键推荐值:

| 配置项 | 推荐值 | 说明 |
|--------|--------|------|
| `[replication] default_replication_factor` | 3 | 3 副本保证高可用 |
| `[replication] min_isr_size` | 2 | 至少 2 个 ISR 确认 |
| `[replication] unclean_leader_election` | false | 禁止非 ISR 选举 |
| `[storage] flush_mode` | hybrid | 混合刷盘 (性能 + 持久) |
| `[storage] segment_max_size` | 1GB | 标准 segment 大小 |
| `[retention] max_ms` | 604800000 | 默认保留 7 天 |
| `[retention] compaction_enabled` | true | 启用 Log Compaction |
| `[security] tls_enabled` | true | 生产必须加密 |
| `[security] sasl_enabled` | true | 生产必须认证 |
| `[security] acl_enabled` | true | 生产必须授权 |

---

## 6. 系统调优

### 6.1 Linux 内核参数

```bash
# 网络
sysctl -w net.core.somaxconn=65535
sysctl -w net.core.netdev_max_backlog=32768
sysctl -w net.ipv4.tcp_max_syn_backlog=65535
sysctl -w net.ipv4.tcp_fin_timeout=30

# 内存
sysctl -w vm.swappiness=1

# 文件描述符
ulimit -n 100000
```

持久化到 `/etc/sysctl.conf`:

```
net.core.somaxconn=65535
net.core.netdev_max_backlog=32768
net.ipv4.tcp_max_syn_backlog=65535
vm.swappiness=1
```

### 6.2 磁盘建议

- 使用 SSD (NVMe 推荐)
- XFS 或 ext4 文件系统
- 禁用 atime: `mount -o noatime /data`
- 数据目录和 WAL 分不同磁盘 (可选)

### 6.3 JVM 参数等效项

R-Kafka 是纯 Rust 实现，无需 JVM。以下 Kafka JVM 参数在 R-Kafka 中的对应:

| Kafka JVM 参数 | R-Kafka 对应 |
|----------------|-------------|
| `-Xmx6g -Xms6g` | 无需 (Rust 无 GC, 内存由 OS 管理) |
| `-XX:+UseG1GC` | 无需 (Rust 无 GC) |
| `-XX:MaxGCPauseMillis=20` | 无需 (无 GC pause) |
| `-Djava.net.preferIPv4Stack=true` | 无需 (Rust 默认 IPv4) |
| `-Dcom.sun.management.jmxremote` | 使用 Prometheus metrics (port 9090) |

### 6.4 Systemd 服务

```ini
[Unit]
Description=R-Kafka Broker
After=network.target

[Service]
Type=simple
User=rkafka
Group=rkafka
ExecStart=/usr/local/bin/r-kafka --config /etc/r-kafka/r-kafka.toml
Restart=on-failure
RestartSec=10
LimitNOFILE=100000

[Install]
WantedBy=multi-user.target
```

```bash
# 安装服务
sudo cp r-kafka.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable r-kafka
sudo systemctl start r-kafka
sudo systemctl status r-kafka
```

---

## 配置文件参考

| 文件 | 说明 |
|------|------|
| `config/r-kafka.toml` | 默认配置 |
| `config/examples/single-node.toml` | 单机部署 |
| `config/examples/cluster-node1.toml` | 3 节点集群 (节点 1) |
| `config/examples/tls-config.toml` | TLS/mTLS 加密 |
| `config/examples/sasl-acl-config.toml` | SASL + ACL |
| `config/examples/production.toml` | 生产环境推荐 |
