# Athena-RS 实施进度

## 项目概述

使用 Rust 开发的 MySQL 代理中间件，支持：
- MySQL 协议
- 连接复用（双池：事务池 + 无状态池）
- 读写分离（轮询从库）
- 透明分库分表（SQL 解析 + 智能路由）
- 熔断限流（按 user+shard 并发控制）

## 实施进度

### Phase 1: 项目骨架与基础协议 ✅ 完成

**目标：** 能接收 Client 连接，完成握手，转发简单 SQL 到单个 MySQL

**完成内容：**
- [x] Cargo 项目初始化
- [x] 配置模块 (`src/config/`)
  - `schema.rs` - 配置结构定义
  - `mod.rs` - 配置加载
- [x] MySQL 协议层 (`src/protocol/`)
  - `packet.rs` - Packet 结构、命令类型、能力标志
  - `codec.rs` - Tokio Codec 编解码器
  - `handshake.rs` - 握手流程、认证、OK/ERR 包
  - `command.rs` - 客户端命令解析
- [x] 会话管理 (`src/session/`)
  - `state.rs` - Session 状态
  - `mod.rs` - Session 处理逻辑
- [x] 主程序 (`src/main.rs`)
- [x] 示例配置文件 (`config/athena.toml`)

**验证方法：**
```bash
# 启动代理
cargo run

# 测试连接
mysql -h 127.0.0.1 -P 3307 -u root -p
> SELECT 1;
> SHOW DATABASES;
```

---

### Phase 2: 连接池 ✅ 完成

**目标：** 实现双池连接管理，支持连接复用

**完成内容：**
- [x] `Connection` 封装 (`src/pool/connection.rs`)
  - 连接状态管理
  - 健康检查 (ping)
  - 连接重置
  - 数据库切换
- [x] `StatelessPool` (`src/pool/stateless.rs`)
  - 无状态连接池
  - 连接复用
  - 过期连接回收
- [x] `TransactionPool` (`src/pool/transaction.rs`)
  - 事务绑定连接
  - Session 到连接的映射
- [x] `PoolManager` (`src/pool/manager.rs`)
  - 多分片池管理
  - 主从池分离
  - 连接统计
- [x] 集成到 Session 中
  - 普通查询使用 StatelessPool
  - 事务使用 TransactionPool 绑定连接

**验证方法：**
```bash
# 开多个连接执行查询，观察后端连接数
mysql -e "SELECT 1" &
mysql -e "SELECT 2" &
# 后端连接数应小于客户端连接数

# 测试事务绑定
mysql
> BEGIN;
> INSERT INTO test VALUES (1);
> SELECT * FROM test;  # 应能看到未提交数据
> ROLLBACK;
```

---

### Phase 3: SQL 解析与分片路由 ✅ 完成

**目标：** 解析 SQL，提取分片键，路由到正确 shard

**完成内容：**
- [x] 集成 `sqlparser-rs` (`src/parser/analyzer.rs`)
  - SQL 解析与分析
  - 语句类型识别 (SELECT, INSERT, UPDATE, DELETE, BEGIN, COMMIT, ROLLBACK, etc.)
- [x] 表名提取
  - FROM 子句解析
  - JOIN 表提取
- [x] 分片键值提取
  - WHERE 条件解析
  - `column = value` 提取
  - `column IN (values)` 提取
  - `column BETWEEN start AND end` 提取
- [x] 分片算法实现 (`src/router/shard.rs`)
  - 取模算法 (Mod)
  - 哈希算法 (Hash)
  - 范围算法 (Range)
- [x] SQL 改写 (`src/parser/rewriter.rs`)
  - 逻辑表名 → 物理表名替换
  - 支持 backtick 表名
  - 支持 JOIN 表名改写
- [x] 路由规则配置 (`src/router/rule.rs`)
  - `ShardingRule` 定义
  - `RouterConfig` 管理
- [x] 路由器实现 (`src/router/mod.rs`)
  - 单分片路由
  - 散布查询（全分片）路由
  - 路由结果生成
- [x] 集成到 Session
  - SQL 分析
  - 路由决策
  - SQL 改写与执行
  - Scatter 查询支持

**测试覆盖：** 24 个单元测试全部通过

**验证方法：**
```sql
-- 应路由到 shard = 5 % 4 = 1
SELECT * FROM users WHERE user_id = 5;

-- 扫全片（串行）
SELECT * FROM users WHERE name = 'test';
```

---

### Phase 4: 读写分离 ✅ 完成

**目标：** SELECT 走从库，写操作走主库

**完成内容：**
- [x] SQL 类型判断 (`src/router/rw_split.rs`)
  - `RouteTarget::Master` / `RouteTarget::Slave`
  - `RwSplitter::route()` 决策
- [x] 从库轮询选择 (`src/pool/manager.rs`)
  - 原子计数器轮询
  - `get_slave()` 方法
- [x] 事务强制走主库
  - `in_transaction` 检查
  - 事务内所有查询走主库
- [x] 路由目标传递
  - `get_for_target()` / `put_for_target()` 统一接口

**验证方法：**
```sql
-- 应走从库
SELECT * FROM users WHERE user_id = 1;

-- 应走主库
INSERT INTO users VALUES (...);

-- 事务内全走主库
BEGIN;
SELECT * FROM users WHERE user_id = 1;  # 走主库
COMMIT;
```

---

### Phase 5: 熔断限流 ✅ 完成

**目标：** 按 (user, shard) 控制并发，防止过载

**完成内容：**
- [x] `ConcurrencyController` 实现 (`src/circuit/limiter.rs`)
  - 基于 Semaphore 的并发控制
  - 按 `(user, shard)` 粒度隔离
  - `acquire()` 方法支持超时等待
  - `try_acquire()` 非阻塞获取
- [x] 等待队列
  - 内置于 Semaphore 机制
  - 可配置最大队列大小
  - 队列满时立即拒绝
- [x] 超时拒绝
  - 可配置等待超时时间
  - 超时返回明确错误
- [x] 配置并发限制 (`src/config/schema.rs`)
  - `enabled` - 开关
  - `max_concurrent_per_user_shard` - 单键最大并发
  - `queue_size` - 最大排队数
  - `queue_timeout_ms` - 排队超时
- [x] 集成到 Session
  - 查询执行前获取 permit
  - 执行完成后自动释放（RAII）
  - 超限时返回错误给客户端

**配置示例：**
```toml
[circuit]
enabled = true
max_concurrent_per_user_shard = 10
queue_size = 100
queue_timeout_ms = 5000
```

**设计决策：**
- 事务内查询不受限流影响（事务连接已绑定，无需额外限流）
- 限流失败只拒绝当前查询，不断开连接
- 非查询命令（PING、InitDb等）不受限流影响

**已知限制：**
- Scatter 查询（跨分片）中途限流失败可能导致客户端收到不完整结果
  - 建议：对于关键业务，考虑禁用 scatter 查询或确保足够的并发配额

**验证方法：**
```bash
# 配置 max_concurrent = 2
# 启动 5 个并发查询
for i in {1..5}; do
  mysql -h 127.0.0.1 -P 3307 -e "SELECT SLEEP(2)" &
done
# 前 2 个立即执行，后 3 个排队或被拒绝
```

---

### Bug Fixes ✅

**修复的问题：**

1. **[Critical] 连接池状态泄漏**
   - 问题：`StatelessPool::put` 归还连接时未 reset，导致会话状态泄漏
   - 修复：归还前调用 `COM_RESET_CONNECTION`

2. **[Critical] 事务路由忽略分片**
   - 问题：`begin_transaction` 总是使用默认后端，忽略 `shard_id` 参数
   - 修复：`TransactionPool` 支持动态 backend 配置，从 backends 获取正确分片

3. **[High] 分片配置未接入**
   - 问题：Session 使用 `Router::default()`，配置中的分片规则未加载
   - 修复：在 `Config` 中添加 `sharding` 字段，main.rs 构建 `RouterConfig` 并传入 Session

4. **[High] 空分片列表 panic**
   - 问题：Router 交集逻辑可能返回空 shards，导致 `shards[0]` panic
   - 修复：Router 在空交集时回退到默认分片

5. **[High] Scatter 查询协议问题**
   - 问题：多分片查询每个分片都发送 EOF，违反 MySQL 协议
   - 修复：只有最后一个分片发送 EOF

---

### Phase 6: 完善与优化 ⏳ 待开始

**待完成：**
- [ ] 完整的错误处理和日志
- [ ] Metrics 指标暴露
- [ ] 优雅关闭
- [ ] 配置热重载
- [ ] 性能测试与优化

---

## 目录结构

```
athena-rs/
├── Cargo.toml
├── src/
│   ├── main.rs
│   ├── config/
│   │   ├── mod.rs
│   │   └── schema.rs           # 包含 CircuitConfig
│   ├── protocol/
│   │   ├── mod.rs
│   │   ├── packet.rs
│   │   ├── codec.rs
│   │   ├── handshake.rs
│   │   └── command.rs
│   ├── parser/
│   │   ├── mod.rs
│   │   ├── analyzer.rs
│   │   └── rewriter.rs
│   ├── router/
│   │   ├── mod.rs
│   │   ├── shard.rs
│   │   ├── rule.rs
│   │   └── rw_split.rs
│   ├── pool/
│   │   ├── mod.rs
│   │   ├── connection.rs
│   │   ├── stateless.rs
│   │   ├── transaction.rs
│   │   └── manager.rs
│   ├── circuit/                 # Phase 5 新增
│   │   ├── mod.rs
│   │   └── limiter.rs          # ConcurrencyController
│   └── session/
│       ├── mod.rs
│       └── state.rs
├── config/
│   └── athena.toml
└── docs/
    ├── DESIGN.md
    └── PROGRESS.md
```

## 依赖

```toml
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
