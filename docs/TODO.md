# TODO

## Pending Tasks

### Shard 计算方式问题
- **问题**: 当前的 shard 计算方式存在问题（待具体分析）
- **位置**: `src/router/shard.rs` 中的 `ShardCalculator`
- **状态**: 待修复

---

# Completed

## Pool Manager Topology Refactor (COMPLETED)

- 改用 `home_group` + `shard_indices` 配置模式
- `DBGroupConfig` 增加 `shard_indices: Vec<usize>` 字段
- `GroupConfig` 增加 `home_group` 字段
- `build_pool_manager` 按 `shard_indices` 注册分片索引到对应 db_group

---

# Routing Refactor: Index-based Mapping (COMPLETED)

## Goal
1. `ShardId` -> `DbGroupId`, clearer semantics
2. Route by index mapping (`[0,1,2,3]`) instead of name matching (`shard_0`)
3. db_group names can be arbitrary, no more `shard_N` convention required
4. Simplify routing logic, reduce string operations

## Status: COMPLETED

All phases completed. Legacy `ShardId` code removed.

## Completed Tasks

### Phase 1: Type Definitions
- [x] Add `ShardIndex = usize` type alias
- [x] Add `DbGroupId` enum with `Shard(ShardIndex)` and `Home` variants
- [x] Removed `ShardId` (no backward compatibility needed)

### Phase 2: PoolManager Refactor
- [x] Vec-based storage only:
  - `shard_master_pools: RwLock<Vec<Arc<StatelessPool>>>`
  - `shard_slave_pools: RwLock<Vec<Option<SlavePoolGroup>>>`
  - `home_master_pool: RwLock<Option<Arc<StatelessPool>>>`
  - `home_slave_pool: RwLock<Option<SlavePoolGroup>>`
- [x] Implemented `get_master(&DbGroupId)` / `put_master(&DbGroupId)` methods
- [x] Implemented `add_shard(idx, backend)` / `set_home(backend)` methods
- [x] Removed all legacy HashMap-based storage and methods

### Phase 3: Router Update
- [x] `RouteType` enum: `Shards(Vec<ShardIndex>)`, `Home`
- [x] `RouteResult` uses only:
  - `route_type: RouteType`
  - `rewritten_sqls: Vec<String>`
  - `target: RouteTarget`
  - `empty_intersection: bool`
- [x] Removed legacy fields (`shards`, `rewritten_sqls_map`, `is_scatter`, `is_home`)

### Phase 4: Session Update
- [x] `SessionState.transaction_target: Option<DbGroupId>` (removed `transaction_shard`)
- [x] Updated `handle_query()` to use `RouteResult.route_type`
- [x] Updated `execute_with_pooled()` / `execute_scatter()` to use `DbGroupId`

### Phase 5: GroupManager
- [x] `build_pool_manager()` uses `add_shard(index, backend)`
- [x] No legacy ShardId methods called

### Phase 6: Cleanup
- [x] Removed `ShardId` and `ShardBackend` types
- [x] Removed all legacy exports from `pool/mod.rs`
- [x] `cargo build` - passed
- [x] `cargo test` - 94 unit tests + 8 integration tests passed

## Code Optimizations (Completed)

- [x] `src/router/rule.rs`: Use `Cow<str>` in `find_rule()`/`find_calculator()` to avoid `to_lowercase()` allocation
- [x] `src/pool/manager.rs`: Added `addr_to_index` HashMap to `SlavePoolGroup` for O(1) lookup in `put_slave()`
- [x] `src/session/mod.rs`: Use inline `starts_with_ignore_case()` helper in `parse_set_statement()` to avoid `to_uppercase()` allocation
- [x] `src/router/shard.rs`: Use binary search (`partition_point`) for range shard calculation
- [x] `src/router/mod.rs`: Use two-pointer algorithm for shard intersection (O(n) vs O(n log n) with HashSet)

## Phase 7: Remove Legacy Fallback (Completed)

- [x] Fixed transaction ending bug: cache `transaction_target` before calling `state.end_transaction()`
- [x] Removed `Config.backend` field (legacy backend no longer supported)
- [x] Removed `GroupManager.default_group` field and `build_default_group()` method
- [x] Groups are now required - `GroupManager::new()` panics if no groups configured
- [x] Updated `get_by_database()` and `get_for_user()` to not fallback to default
- [x] Updated `config/athena.toml` to remove `[backend]` section
- [x] Updated all tests to use groups-based configuration
