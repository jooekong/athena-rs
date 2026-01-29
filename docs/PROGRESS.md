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

### Phase 6: 完善与优化 ✅ 完成

**完成内容：**

#### Phase 6.1: Tracing Spans 和 SQL 执行时间记录
- [x] 添加 `#[instrument]` 宏到 `handle_query` 函数
- [x] 记录解析时间 (`parse_time_us`)
- [x] 记录路由时间 (`route_time_us`)
- [x] 记录总查询时间 (`query_time_ms`)
- [x] SQL 预览截断到 100 字符

#### Phase 6.2: Prometheus Metrics 暴露
- [x] 创建 `src/metrics/mod.rs` 模块
- [x] 连接指标：`athena_connections_total/active/closed`
- [x] 查询指标：`athena_queries_total`、`athena_query_duration_seconds`、`athena_query_errors_total`
- [x] 限流指标：`athena_rate_limit_acquired/rejected_*`
- [x] 路由指标：`athena_queries_routed_total`、`athena_scatter_queries_total`
- [x] HTTP 端点：`:PORT+1000` (`/metrics`, `/health`)

#### Phase 6.3: 优雅关闭
- [x] 监听 SIGTERM 和 SIGINT 信号
- [x] 停止接受新连接
- [x] 使用 `JoinSet` 追踪活跃会话
- [x] 等待最多 30 秒让会话完成
- [x] 超时后中止剩余会话

**待完成（可选优化）：**
- [ ] 配置热重载

---

### Phase 7: Bug Fixes & Testing ✅ 完成

**修复的问题：**

6. **[Critical] 事务语义修正**
   - 问题：客户端发送 BEGIN 后，代理只在本地标记事务状态
   - 修复：事务连接创建时设置 `autocommit=0`，无需显式 BEGIN，只需 COMMIT/ROLLBACK
   - 文件：`src/pool/transaction.rs`（添加 `set_autocommit_off`）

7. **[High] 限流队列边界错误**
   - 问题：`queue_size=0` 时，首个请求也会因 `0 >= 0` 判断被拒绝
   - 修复：添加快速路径，先尝试 `try_acquire_owned`，成功则直接返回；失败才检查队列是否已满
   - 文件：`src/circuit/limiter.rs`

**新增测试：**
- `test_zero_queue_size_first_acquire_succeeds` - 验证 queue_size=0 时首次获取成功

**集成测试框架：**
- [x] 创建 `tests/integration/` 目录结构
- [x] 事务测试用例（placeholder）
- [x] 限流测试用例（placeholder）
- [x] 性能测试文档 `docs/PERF.md`

---

### Phase 8: 多租户架构重构 ✅ 完成

**目标：** 引入 Group/DBGroup/DBInstance 概念，简化限流实现

**完成内容：**

#### 8.1 配置结构重构
- [x] `GroupConfig` - 客户端视角的逻辑 DB（1:1 对应租户）
- [x] `DBGroupConfig` - 后端集群（对应一个 shard）
- [x] `DBInstanceConfig` - 单个 MySQL 实例
- [x] `LimiterConfig` - 每个 DBInstance 的限流配置

#### 8.2 限流简化
- [x] 新 `Limiter` 结构：可内嵌到 DBInstance，无需 DashMap
- [x] 移除 `LimitKey = (user, shard)` 复杂 key 管理
- [x] 保留旧 API（`ConcurrencyController`）用于向后兼容

**配置示例：**
```toml
[[groups]]
name = "tenant_a"

[[groups.db_groups]]
shard_id = "shard_0"

[[groups.db_groups.instances]]
host = "mysql-1"
port = 3306
role = "master"

[groups.db_groups.instances.limiter]
max_concurrent = 100
max_queue_size = 50
```

**文件变更：**
- `src/config/schema.rs` - 新增 Group/DBGroup/DBInstance/LimiterConfig
- `src/circuit/limiter.rs` - 简化 Limiter 实现

---

### Phase 9: 读写分离增强 ✅ 完成

**目标：** 完善读写分离机制，支持多主多从，防止连接污染

**完成内容：**

#### 9.1 配置结构增强
- [x] `DBGroupConfig` 添加 `masters()`、`slaves()`、`primary_master()` 方法
- [x] `has_slaves()` 方法用于检查是否有从库

#### 9.2 连接污染防护
- [x] `SessionState.session_vars`：存储拦截的 SET 变量
- [x] SET 命令解析：支持多种格式（`SET var=val`、`SET NAMES`、`SET @@session.var`）
- [x] 事务关键命令例外：`SET autocommit`、`SET TRANSACTION` 在事务中转发

**文件变更：**
- `src/config/schema.rs` - DBGroupConfig helper 方法
- `src/session/state.rs` - session_vars 字段
- `src/session/mod.rs` - SET 命令拦截逻辑

---

### Phase 10: 健康检查模块 ✅ 完成

**目标：** 实现后端实例健康检查，支持自动摘除故障实例和 Master 选主

**完成内容：**

#### 10.1 健康状态管理（滑动窗口）
- [x] `InstanceHealth` - 实例健康状态（Healthy/Unhealthy/Unknown）
- [x] `HealthStatus` - 状态枚举
- [x] `WindowConfig` - 滑动窗口配置
- [x] 滑动窗口防抖，避免单次成功/失败导致状态抖动
- [x] 状态转换需要持续的成功/失败信号

#### 10.2 实例注册表（多租户去重）
- [x] `InstanceRegistry` - 按 host:port 去重
- [x] 引用计数，支持多 Group 共享同一实例

#### 10.3 Master 检测
- [x] `MasterDetector` - 检测实例角色
- [x] 查询 `@@read_only` 变量
- [x] 查询 `SHOW SLAVE STATUS`
- [x] 规则：`read_only=0 AND 无 slave status` = Master

#### 10.4 健康检查后台任务
- [x] 每实例一个长驻检查任务（无 spawn 开销）
- [x] 注册时自动启动任务，注销时自动取消
- [x] 随机初始延迟实现错峰检查
- [x] 持久连接（任务独占，无全局锁）
- [x] 合并查询 `SELECT 1, @@read_only`（减少 RTT）
- [x] 超时控制

#### 10.5 Selector 集成
- [x] `select_healthy()` - 选择健康实例
- [x] `select_healthy_with_target()` - 返回实际使用的 target
- [x] 自动过滤不健康实例，支持 fallback

#### 10.6 配置
- [x] `HealthCheckConfig` - 健康检查配置
- [x] 默认值：间隔 5s，阈值 5 次，超时 3s

**文件变更：**
- `src/health/mod.rs` - 模块入口
- `src/health/state.rs` - InstanceHealth, HealthStatus
- `src/health/registry.rs` - InstanceRegistry（去重）
- `src/health/master.rs` - MasterDetector（角色检测）
- `src/config/schema.rs` - HealthCheckConfig

---

### Phase 11: Groups-only 运行时 wiring ✅ 完成

**目标：** 使用 groups 配置完全替代遗留 backend 配置，实现多租户运行时

**完成内容：**

#### 11.1 GroupManager 模块
- [x] `GroupManager` - 管理所有 Group 及其运行时资源
- [x] `GroupContext` - 单个 Group 的运行时上下文
- [x] 按 database 名称查找 Group（1:1 映射）
- [x] 共享 HealthRegistry 跨所有 Group

#### 11.2 Session 集成
- [x] Session 支持两种模式：Legacy（直接 PoolManager）和 Groups（GroupManager）
- [x] Handshake 完成后根据 database 选择 GroupContext
- [x] 动态设置 pool_manager 和 router

#### 11.3 健康检查集成
- [x] GroupManager 初始化时自动注册所有 DBInstance 到 HealthRegistry
- [x] WindowConfig 从 failure_threshold 自动生成
- [x] 原子性的实例注册（避免并发 spawn 竞态）

#### 11.4 Metrics 扩展
- [x] `athena_health_check_total` - 健康检查结果计数（按 success/failure/timeout）
- [x] `athena_health_instances` - 当前实例健康状态分布

**文件变更：**
- `src/group/mod.rs` - 新增：模块入口
- `src/group/manager.rs` - 新增：GroupManager, GroupContext
- `src/main.rs` - 使用 GroupManager 替代直接 PoolManager
- `src/session/mod.rs` - 支持 with_group_manager，select_group
- `src/health/registry.rs` - 原子性注册，避免竞态
- `src/health/state.rs` - WindowConfig::from_failure_threshold
- `src/metrics/mod.rs` - 健康检查指标
- `src/config/schema.rs` - BackendConfig::default()

---

### Phase 12: 集成测试完善 ✅ 完成

**目标：** 添加可运行的集成测试覆盖分片、读写分离、跨片事务等场景

**完成内容：**

#### 12.1 测试基础设施
- [x] 添加 `mysql` crate 作为 dev-dependency
- [x] 通用测试工具（`tests/integration/main.rs`）
  - `get_proxy_config()` - 代理连接配置
  - `calculate_shard()` - 分片哈希计算（与生产代码一致）
  - `find_different_shard_user_ids()` - 查找不同分片的测试 user_id
  - `assert_mysql_error()` / `assert_query_error()` - MySQL 错误断言
  - `skip_if_not_enabled!` 宏 - 跳过未启用的测试

#### 12.2 分片测试（`tests/integration/sharding.rs`）
- [x] 正常场景
  - 带分片键的单分片查询
  - INSERT/UPDATE/DELETE 路由到正确分片
- [x] JOIN 场景
  - 同分片键 JOIN 成功
  - 不同分片键 JOIN 返回 "Empty shard intersection" 错误
- [x] 异常场景
  - 无分片键的 UPDATE/DELETE 返回 "Scatter writes not allowed" 错误

#### 12.3 聚合测试（跨分片合并）
- [x] `COUNT(*)` 跨分片求和
- [x] `SUM(column)` 跨分片求和
- [x] `MAX(column)` 跨分片取最大
- [x] `MIN(column)` 跨分片取最小
- [x] 多聚合函数单查询

#### 12.4 事务测试（`tests/integration/transaction.rs`）
- [x] 基础事务：COMMIT/ROLLBACK 语义正确
- [x] 事务隔离：未提交数据在事务内可见
- [x] 跨分片事务拒绝："Cross-shard query in transaction not allowed"
- [x] 事务内 scatter 查询拒绝："Scatter queries not allowed in transaction"

#### 12.5 读写分离测试（`tests/integration/rw_split.rs`）
- [x] 事务内读走 master（read-your-writes）
- [x] 事务控制语句走 master
- [x] 从库路由检测（若配置了 slave）

**运行方式：**
```bash
ATHENA_RUN_INTEGRATION_TESTS=1 cargo test --test integration
```

**环境变量：**
- `ATHENA_TEST_PROXY_HOST` - 代理地址（默认 127.0.0.1）
- `ATHENA_TEST_PROXY_PORT` - 代理端口（默认 3307）
- `ATHENA_TEST_PROXY_USER` - 代理用户（默认 app_user）
- `ATHENA_TEST_PROXY_PASS` - 代理密码（默认 test123）
- `ATHENA_TEST_SHARD_COUNT` - 分片数量（默认 4）

---

## 目录结构

```
athena-rs/
├── Cargo.toml
├── src/
│   ├── main.rs                  # 入口，优雅关闭支持
│   ├── config/
│   │   ├── mod.rs
│   │   └── schema.rs           # Group/DBGroup/DBInstance/LimiterConfig
│   ├── group/
│   │   ├── mod.rs
│   │   └── manager.rs          # GroupManager, GroupContext
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
│   │   ├── connection.rs       # 包含 backend_addr 追踪
│   │   ├── stateless.rs
│   │   ├── transaction.rs
│   │   └── manager.rs
│   ├── circuit/
│   │   ├── mod.rs
│   │   └── limiter.rs          # 简化的 Limiter（可内嵌）
│   ├── health/
│   │   ├── mod.rs              # 健康检查模块入口
│   │   ├── state.rs            # InstanceHealth, HealthStatus
│   │   ├── registry.rs         # InstanceRegistry（多租户去重）
│   │   └── master.rs           # MasterDetector（角色检测）
│   ├── metrics/
│   │   └── mod.rs              # Prometheus 指标 + HTTP 端点
│   └── session/
│       ├── mod.rs              # 带 tracing spans + SET 拦截
│       └── state.rs            # 含 session_vars 存储
├── tests/
│   └── integration/             # 集成测试
│       ├── main.rs              # 测试入口和共享工具
│       ├── sharding.rs          # 分片/JOIN/聚合测试
│       ├── transaction.rs       # 事务和跨分片事务测试
│       ├── rw_split.rs          # 读写分离测试
│       └── limiter.rs           # 限流测试（placeholder）
├── config/
│   └── athena.toml
└── docs/
    ├── DESIGN.md
    ├── PROGRESS.md
    └── PERF.md                  # 性能测试指南
```

## 依赖

```toml
tokio = { version = "1", features = ["full", "signal"] }
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
prometheus = "0.13"           # Phase 6 新增
hyper = { version = "1.1", features = ["server", "http1"] }  # Phase 6 新增
hyper-util = { version = "0.1", features = ["tokio"] }       # Phase 6 新增
http-body-util = "0.1"        # Phase 6 新增

[dev-dependencies]
mysql = "25"                  # Phase 12 新增：集成测试客户端
```
