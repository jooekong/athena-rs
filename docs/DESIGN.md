# Athena-RS: MySQL 代理中间件设计文档

## 项目概述

使用 Rust 开发的 MySQL 代理中间件，用于内部业务系统，支持：
- MySQL 协议
- 连接复用（双池：事务池 + 无状态池）
- 读写分离（轮询从库）
- 透明分库分表（SQL 解析 + 智能路由）
- 熔断限流（按 user+shard 并发控制）

**技术栈：** Rust + Tokio + sqlparser-rs + mysql_common

---

## 架构设计

### 整体架构

```
Client TCP
    ↓
[Session Task] ← Connection-per-Task 模型
    ↓
[Protocol Layer] ← MySQL 协议编解码
    ↓
[SQL Parser] ← sqlparser-rs 解析
    ↓
[Router] ← 分片键提取 + 路由计算
    ↓
[Concurrency Controller] ← 按 (user, shard) 限流
    ↓
[Connection Pool] ← 双池（事务/无状态）
    ↓
Backend MySQL
```

### 并发模型

- 每个 Client 连接一个 Tokio Task
- 请求-响应同步模型（串行处理）
- 使用 `select!` 处理 Client/Backend 同时断开

### 连接池设计

**双池模型：**

1. **事务池 (TransactionPool)**
   - 处理 BEGIN 后的连接
   - 连接绑定到 Session，直到 COMMIT/ROLLBACK
   - 保证事务内所有操作使用同一连接

**事务语义（延迟开启）：**
```
Client: BEGIN     → Proxy: 本地标记 in_transaction=true, 返回 OK（不发后端）
Client: SELECT... → Proxy: 绑定 shard, 创建连接, 发送 BEGIN, 吞掉 OK, 发送 SELECT
Client: COMMIT    → Proxy: 发送 COMMIT 到后端, 清理事务状态
```

- 若事务内无任何查询，COMMIT/ROLLBACK 只返回 OK，不发后端
- `transaction_started` 标志位追踪 BEGIN 是否已发送到后端

2. **无状态池 (StatelessPool)**
   - 普通读写查询
   - 用完即还，支持连接复用
   - 按 shard 组织，每个 shard 有独立的池

**连接生命周期：**
```
创建 → 空闲 → 获取 → 使用中 → 释放 → 空闲 → ... → 过期/关闭
```

**池配置：**
- `max_idle`: 最大空闲连接数
- `max_age`: 连接最大存活时间
- `max_idle_time`: 最大空闲时间

### 熔断限流设计

- 按 `(user, shard)` 粒度控制并发
- 超过限制进入等待队列
- 队列满或超时 → 拒绝请求（返回 MySQL 错误，不断开连接）

**获取流程（双路径）：**
1. **快速路径**：`try_acquire_owned` 尝试立即获取，成功则返回（不计入等待）
2. **慢速路径**：快速路径失败后，检查队列是否已满，未满则入队等待

**限流配置：**
```toml
[circuit]
enabled = true                      # 开关
max_concurrent_per_user_shard = 10  # 单键最大并发
queue_size = 100                    # 最大排队数
queue_timeout_ms = 5000             # 排队超时（毫秒）
```

**限流范围：**
- ✅ 普通查询（SELECT/INSERT/UPDATE/DELETE）
- ✅ Scatter 查询（每个分片单独限流）
- ❌ 事务内查询（连接已绑定，无需限流）
- ❌ 非查询命令（PING、InitDb 等）

**错误处理：**
- 队列满：返回 `ERROR 1040 (08004): Too many connections`
- 超时：返回 `ERROR 1205 (HY000): Rate limit timeout`
- 限流失败不断开连接，客户端可继续发送其他查询

---

## 模块设计

### 1. Protocol Layer (`src/protocol/`)

**职责：** MySQL 协议编解码

**组件：**
- `packet.rs` - MySQL Packet 结构（3字节长度 + 1字节序号 + payload）
- `codec.rs` - Tokio Codec 实现，用于 Framed 读写
- `handshake.rs` - 握手流程、认证、OK/ERR/EOF 包
- `command.rs` - 客户端命令解析（COM_QUERY, COM_QUIT 等）

**MySQL 包格式：**
```
+----------------+----------------+----------------+----------------+
| payload_length |  sequence_id   |     payload...                  |
|    (3 bytes)   |   (1 byte)     |                                 |
+----------------+----------------+----------------+----------------+
```

**握手流程：**
```
Client                          Proxy                           Backend
   |                              |                                |
   |<---- Initial Handshake ------|                                |
   |                              |                                |
   |---- Handshake Response ----->|                                |
   |                              |---- Connect + Auth ----------->|
   |                              |<-------- OK/ERR ---------------|
   |<-------- OK/ERR -------------|                                |
```

### 2. Parser Layer (`src/parser/`)

**职责：** SQL 解析与分析

**组件：**
- `analyzer.rs` - SQL 分析器
  - 表名提取
  - SQL 类型判断（SELECT/INSERT/UPDATE/DELETE）
  - 分片键值提取
- `rewriter.rs` - SQL 改写器
  - 逻辑表名 → 物理表名

**SQL 分析流程：**
```
SQL String
    ↓
sqlparser-rs 解析
    ↓
AST 遍历
    ↓
提取: 表名, SQL类型, WHERE条件
    ↓
分片键值提取
```

### 3. Router Layer (`src/router/`)

**职责：** 路由计算与分片

**组件：**
- `shard.rs` - 分片算法
  - 取模分片 (mod)
  - 范围分片 (range)
  - 哈希分片 (hash)
- `rw_split.rs` - 读写分离判断
- `rule.rs` - 路由规则配置

**分片路由流程：**
```
SQL + 分片键值
    ↓
查找分片规则
    ↓
计算目标 shard
    ↓
获取 shard 对应的后端配置
    ↓
SQL 改写（表名替换）
```

**分片配置示例：**
```toml
[[shards]]
name = "user"
table_pattern = "user"
shard_column = "user_id"
algorithm = "mod"
shard_count = 16

[[backends]]
shard_id = 0
master = "mysql://127.0.0.1:3306/db_0"
slaves = ["mysql://127.0.0.1:3307/db_0"]
```

### 4. Pool Layer (`src/pool/`)

**职责：** 连接池管理

**组件：**
- `connection.rs` - 连接封装
- `stateless.rs` - 无状态连接池
- `transaction.rs` - 事务连接池
- `manager.rs` - 池管理器

**连接获取流程（无状态）：**
```
请求连接
    ↓
检查空闲池
    ↓
有空闲？ → 检查健康 → 返回
    ↓ 无
创建新连接 → 返回
```

**连接获取流程（事务）：**
```
BEGIN
    ↓
检查 Session 是否已绑定
    ↓
已绑定？ → 返回绑定连接
    ↓ 否
创建新连接 → 绑定到 Session → 返回
```

### 5. Circuit Layer (`src/circuit/`)

**职责：** 熔断限流

**组件：**
- `limiter.rs` - 并发控制器（基于 Semaphore，内置等待队列）

**核心类型：**
- `LimitKey` - 限流键 (user, shard)
- `LimitConfig` - 限流配置
- `ConcurrencyController` - 并发控制器
- `LimitPermit` - RAII 许可证，drop 时自动释放

**限流流程：**
```
请求进入
    ↓
计算 key = (user, shard)
    ↓
检查队列是否已满
    ↓ 满
返回 QueueFull 错误
    ↓ 未满
入队等待 Semaphore
    ↓ 超时
返回 Timeout 错误
    ↓ 获取成功
返回 LimitPermit → 执行请求 → drop 自动释放
```

### 6. Session Layer (`src/session/`)

**职责：** 会话管理

**组件：**
- `state.rs` - Session 状态
- `mod.rs` - Session 处理逻辑

**Session 状态：**
```rust
struct SessionState {
    username: String,
    database: Option<String>,
    in_transaction: bool,
    capability_flags: u32,
    character_set: u8,
}
```

---

## 数据流

### 普通查询（无事务）

```
1. Client 发送 COM_QUERY
2. 解析 SQL，提取表名和分片键
3. 计算目标 shard
4. 判断读写类型
5. 从 StatelessPool 获取连接（主库/从库）
6. 改写 SQL（表名替换）
7. 转发请求到后端
8. 转发响应到 Client
9. 归还连接到池
```

### 事务查询

```
1. Client 发送 BEGIN
2. 从 TransactionPool 获取连接，绑定到 Session
3. Client 发送后续 SQL
4. 使用绑定的连接执行（强制主库）
5. Client 发送 COMMIT/ROLLBACK
6. 转发到后端
7. 解绑并释放连接
```

### 跨分片查询（全表扫描）

```
1. Client 发送无分片键的 SELECT
2. 解析 SQL，未找到分片键
3. 串行查询所有 shard
4. 合并结果返回 Client
```

---

## 配置设计

### 完整配置示例

```toml
# config/athena.toml

[server]
listen_addr = "0.0.0.0"
listen_port = 3307

# 默认后端（无分片时使用）
[backend]
host = "127.0.0.1"
port = 3306
user = "root"
password = "password"
database = "test"

# 连接池配置
[pool]
max_idle = 10
max_age_secs = 3600
max_idle_time_secs = 300

# 熔断限流配置
[circuit]
max_concurrent_per_user_shard = 10
queue_size = 100
queue_timeout_ms = 5000

# 分片规则
[[sharding.rules]]
name = "user_shard"
table_pattern = "user"
shard_column = "user_id"
algorithm = "mod"
shard_count = 16

[[sharding.rules]]
name = "order_shard"
table_pattern = "order"
shard_column = "order_id"
algorithm = "mod"
shard_count = 8

# 后端配置（分片）
[[sharding.backends]]
shard_id = 0
master = { host = "10.0.0.1", port = 3306, user = "root", password = "pwd" }
slaves = [
    { host = "10.0.0.2", port = 3306, user = "root", password = "pwd" },
    { host = "10.0.0.3", port = 3306, user = "root", password = "pwd" }
]

[[sharding.backends]]
shard_id = 1
master = { host = "10.0.1.1", port = 3306, user = "root", password = "pwd" }
slaves = [
    { host = "10.0.1.2", port = 3306, user = "root", password = "pwd" }
]

# ... 更多分片后端
```

---

## 实施阶段

### Phase 1: 项目骨架与基础协议 ✅

- Cargo 项目初始化
- MySQL 协议层实现
- 单后端连接转发
- 基础配置加载

### Phase 2: 连接池 ✅

- Connection 封装
- StatelessPool 实现
- TransactionPool 实现
- PoolManager 实现
- 集成到 Session

### Phase 3: SQL 解析与分片路由 ✅

- 集成 sqlparser-rs
- 表名提取
- 分片键值提取
- 分片算法实现
- SQL 改写
- 配置分片规则

### Phase 4: 读写分离 ✅

- SQL 类型判断
- 从库轮询选择
- 事务强制走主库
- ~~从库健康检查~~ (待实现)

### Phase 5: 熔断限流 ✅

- ConcurrencyController 实现
- 等待队列（内置于 Semaphore）
- 超时拒绝
- 配置并发限制

### Phase 6: 完善与优化

- 完整的错误处理和日志
- Metrics 指标暴露（Prometheus）
- 优雅关闭（drain connections）
- 配置热重载
- 性能测试与优化

---

## 目录结构

```
athena-rs/
├── Cargo.toml
├── src/
│   ├── main.rs
│   ├── config/
│   │   ├── mod.rs
│   │   └── schema.rs         # 配置结构定义
│   ├── protocol/
│   │   ├── mod.rs
│   │   ├── packet.rs         # Packet 结构
│   │   ├── codec.rs          # Tokio Codec
│   │   ├── handshake.rs      # 握手流程
│   │   └── command.rs        # 命令处理
│   ├── parser/
│   │   ├── mod.rs
│   │   ├── analyzer.rs       # SQL 分析
│   │   └── rewriter.rs       # SQL 改写
│   ├── router/
│   │   ├── mod.rs
│   │   ├── shard.rs          # 分片计算
│   │   ├── rw_split.rs       # 读写判断
│   │   └── rule.rs           # 路由规则
│   ├── pool/
│   │   ├── mod.rs
│   │   ├── connection.rs     # 连接封装
│   │   ├── stateless.rs      # 无状态池
│   │   ├── transaction.rs    # 事务池
│   │   └── manager.rs        # 池管理器
│   ├── circuit/
│   │   ├── mod.rs
│   │   └── limiter.rs        # 并发控制（内置等待队列）
│   └── session/
│       ├── mod.rs
│       └── state.rs          # Session 状态
├── config/
│   └── athena.toml           # 示例配置
├── docs/
│   ├── DESIGN.md             # 设计文档
│   └── PROGRESS.md           # 进度文档
└── tests/
    └── integration/
```

---

## 核心依赖

```toml
[dependencies]
tokio = { version = "1", features = ["full"] }
sqlparser = { version = "0.40", features = ["visitor"] }
mysql_common = "0.31"
dashmap = "5"
serde = { version = "1", features = ["derive"] }
toml = "0.8"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
thiserror = "1"
bytes = "1"
tokio-util = { version = "0.7", features = ["codec"] }
sha1 = "0.10"
rand = "0.8"
anyhow = "1"
futures = "0.3"
```

---

## 验证清单

### Phase 1 验证
```bash
mysql -h 127.0.0.1 -P 3307 -u root -p
> SELECT 1;
> SHOW DATABASES;
```

### Phase 2 验证
```bash
# 多连接复用
for i in {1..10}; do mysql -h 127.0.0.1 -P 3307 -e "SELECT $i" & done

# 事务绑定
mysql -h 127.0.0.1 -P 3307
> BEGIN;
> INSERT INTO test VALUES (1);
> SELECT * FROM test;  # 应能看到未提交数据
> ROLLBACK;
```

### Phase 3 验证
```sql
-- 应路由到 shard = hash(123) % 16
SELECT * FROM user WHERE user_id = 123;

-- 扫全片（串行）
SELECT * FROM user WHERE name = 'test';
```

### Phase 4 验证
```sql
-- 应走从库
SELECT * FROM user WHERE user_id = 1;

-- 应走主库
INSERT INTO user VALUES (...);

-- 事务内全走主库
BEGIN;
SELECT * FROM user WHERE user_id = 1;
COMMIT;
```

### Phase 5 验证
```bash
# 配置 max_concurrent = 5
# 启动 10 个并发查询
for i in {1..10}; do
  mysql -h 127.0.0.1 -P 3307 -e "SELECT SLEEP(2)" &
done
# 前 5 个应立即执行，后 5 个排队或拒绝
```
