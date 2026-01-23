# 实现评审记录（2026-01-23）

以下为当前实现的审查结果，按严重度排序，重点聚焦 bug、风险点与可优化项。

## 发现的问题

### Critical

- **客户端鉴权未验证**：代理仅解析客户端握手并发送 OK，没有校验用户名/密码，也未将客户端身份透传给后端。任何客户端都可直接连接。
````80:130:src/session/mod.rs
        // Step 2: Receive handshake response from client
        let response_packet = client
            .next()
            .await
            .ok_or(SessionError::ClientDisconnected)??;

        let response = HandshakeResponse::parse(&response_packet.payload)
            .ok_or_else(|| SessionError::Protocol("Invalid handshake response".into()))?;

        // Store session info
        self.state.set_from_handshake(
            response.username.clone(),
            response.database.clone(),
            response.capability_flags,
            response.character_set,
        );

        // Step 3: Validate connection by getting one from pool
        let shard_id = ShardId::default_shard();
        let test_conn = self
            .pool_manager
            .get_master(&shard_id, self.state.database.clone())
            .await
            .map_err(|e| SessionError::BackendConnect(e.to_string()))?;

        // Step 4: Send OK to client (authentication successful)
        let ok = OkPacket::new();
        client
            .send(ok.encode(2, self.state.capability_flags))
            .await?;
````

### High

- **多语句能力已宣告但只处理第一条语句**：`CLIENT_MULTI_STATEMENTS` 已在默认能力中启用，但 `SqlAnalyzer` 仅解析 `statements[0]`，多语句时会误路由或漏执行。
````114:123:src/parser/analyzer.rs
        let statements = Parser::parse_sql(&self.dialect, sql_trimmed)
            .map_err(|e| AnalyzerError::ParseError(e.to_string()))?;

        if statements.is_empty() {
            return Err(AnalyzerError::EmptyStatement);
        }

        let stmt = &statements[0];
        self.analyze_statement(stmt)
````

- **事务仍绑定默认分片**：`BEGIN` 时固定使用 `ShardId::default_shard()`，后续事务内 SQL 可能被错误地绑定到默认分片。
````255:263:src/session/mod.rs
        if analysis.stmt_type == StatementType::Begin {
            self.state.begin_transaction();
            let shard_id = ShardId::default_shard();
            self.pool_manager
                .begin_transaction(self.id, &shard_id, self.state.database.clone())
                .await
                .map_err(|e| SessionError::BackendConnect(e.to_string()))?;
            self.execute_with_transaction(client, original_packet.clone())
                .await?;
            return Ok(());
        }
````

- **EOF 判定使用客户端能力而非后端能力**：结果集结束判断基于 `self.state.capability_flags`，但后端连接可能未启用 `CLIENT_DEPRECATE_EOF`，会导致响应边界解析错误。
````696:720:src/session/mod.rs
        loop {
            let packet = conn
                .recv()
                .await
                .map_err(|_| SessionError::BackendDisconnected)?;

            let is_end = is_ok_packet(&packet.payload)
                || is_err_packet(&packet.payload)
                || is_eof_packet(&packet.payload, self.state.capability_flags);

            client.send(packet).await?;

            if is_end {
                break;
            }
        }
````

- **Scatter 查询协议与语义风险**：仅最后一个分片返回 OK/ERR，且未重排响应序列号；中途失败会产生“部分结果 + 错误”混合响应。
````496:506:src/session/mod.rs
        if is_ok_packet(&first.payload) || is_err_packet(&first.payload) {
            // For OK/ERR packets, only forward on last shard to avoid confusing client
            if is_last_shard {
                client.send(first).await?;
            }
            return Ok(());
        }
````

### Medium

- **数据库切换状态可能不一致**：`InitDb` 先更新本地状态，再将命令发往后端；如果后端失败，本地状态会与真实连接不一致。
````177:180:src/session/mod.rs
            // Handle InitDb
            if let ClientCommand::InitDb(ref db) = cmd {
                self.state.change_database(db.clone());
            }
````

- **PoolManager 接收 database 参数但未生效**：`get_master/get_slave` 忽略了 `database` 入参，导致每次取连接时无法基于会话数据库切换。
````127:136:src/pool/manager.rs
    pub async fn get_master(
        &self,
        shard_id: &ShardId,
        database: Option<String>,
    ) -> Result<PooledConnection, ConnectionError> {
        let pools = self.stateless_pools.read().await;
        if let Some(pool) = pools.get(shard_id) {
            pool.get().await
        } else {
            // Fall back to default if shard not found
            pools
                .get(&ShardId::default_shard())
                .ok_or(ConnectionError::Disconnected)?
                .get()
                .await
        }
    }
````

- **连接读取错误未标记为不可用**：`recv()` 失败不会把连接标记为 `Closed`，可能被错误地归还池复用。
````246:258:src/pool/connection.rs
    pub async fn recv(&mut self) -> Result<Packet, ConnectionError> {
        self.framed
            .next()
            .await
            .ok_or(ConnectionError::Disconnected)?
            .map_err(|e| ConnectionError::Io(e.to_string()))
    }
````

- **INSERT 多行值仅使用首行做分片**：多行插入若跨分片，会错误路由到单一分片。
````138:151:src/parser/analyzer.rs
                if let Some(src) = source {
                    if let SetExpr::Values(values) = src.body.as_ref() {
                        // Get column positions
                        let cols: Vec<String> =
                            columns.iter().map(|c| c.value.clone()).collect();

                        // Extract values from first row
                        if let Some(first_row) = values.rows.first() {
                            for (idx, expr) in first_row.iter().enumerate() {
                                if idx < cols.len() {
                                    if let Some(val) = self.extract_literal_value(expr) {
                                        shard_keys.push((cols[idx].clone(), ShardKeyValue::Single(val)));
                                    }
                                }
                            }
                        }
                    }
                }
````

- **SQL 重写可能误改字符串字面量**：重写逻辑不识别单引号/注释，仅基于字符边界替换。
````52:82:src/parser/rewriter.rs
        while i < sql_bytes.len() {
            // Check for backtick-quoted identifier
            if sql_bytes[i] == b'`' {
                let start = i;
                i += 1;
                while i < sql_bytes.len() && sql_bytes[i] != b'`' {
                    i += 1;
                }
                if i < sql_bytes.len() {
                    i += 1; // include closing backtick
                }
                let quoted = &sql[start..i];
                let inner = &quoted[1..quoted.len() - 1];
                if inner.eq_ignore_ascii_case(old_name) {
                    result.push('`');
                    result.push_str(new_name);
                    result.push('`');
                } else {
                    result.push_str(quoted);
                }
                continue;
            }

            // Check for unquoted identifier
            if i + old_len <= sql_bytes.len() {
                let slice = &sql[i..i + old_len];
                if slice.eq_ignore_ascii_case(old_name) {
                    // Check boundaries
                    let before_ok = i == 0 || !is_identifier_char(sql_bytes[i - 1]);
                    let after_ok = i + old_len >= sql_bytes.len()
````

## 可优化方向

- **Scatter 并行化与统一结果合并**：并行执行各分片并统一生成受控的响应流（包含统一 sequence_id）。
- **事务延迟绑定**：`BEGIN` 不立即绑定分片，在第一条可路由语句时绑定，或直接拒绝跨分片事务。
- **分片与后端配置联动**：引入 shard → backend 映射配置，避免 `get_master` 回退到 default 后端。
- **连接生命周期更稳健**：`recv()`/`forward_*` 失败后强制关闭连接并从池中移除。
- **会话级 SET/USE 语义**：为无状态池建立会话级变量隔离机制或显式标记“不支持”。

## 测试与验证建议

- **集成测试**：真实 MySQL 环境下验证 `USE db`、`SET`、多语句、Scatter 查询返回序列号一致性。
- **安全测试**：不同用户名/密码连接验证（应拒绝非法用户）。
- **跨分片事务测试**：`BEGIN` 后首条路由到非默认分片的场景。
