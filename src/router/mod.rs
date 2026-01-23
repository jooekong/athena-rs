mod rule;
mod rw_split;
mod shard;

pub use rule::{RouterConfig, ShardingRule};
pub use rw_split::{RouteTarget, RwSplitter};
pub use shard::{ShardAlgorithm, ShardCalculator};

use std::collections::HashMap;

use crate::parser::{ShardKeyValue, SqlAnalysis, SqlRewriter};
use crate::pool::ShardId;

/// Routing result
#[derive(Debug, Clone)]
pub struct RouteResult {
    /// Target shards for this query
    pub shards: Vec<ShardId>,
    /// Read-write routing target
    pub target: RouteTarget,
    /// Rewritten SQL for each shard (shard_id -> sql)
    pub rewritten_sqls: HashMap<ShardId, String>,
    /// Whether this is a scatter query (hits multiple shards)
    pub is_scatter: bool,
}

/// SQL Router
pub struct Router {
    config: RouterConfig,
}

impl Router {
    pub fn new(config: RouterConfig) -> Self {
        Self { config }
    }

    /// Route a SQL query based on analysis result
    ///
    /// # Arguments
    /// * `sql` - Original SQL string
    /// * `analysis` - SQL analysis result from parser
    /// * `in_transaction` - Whether the session is in a transaction
    ///
    /// # Returns
    /// Routing result with target shards and rewritten SQL
    pub fn route(&self, sql: &str, analysis: &SqlAnalysis, in_transaction: bool) -> RouteResult {
        // Determine read-write target
        let target = RwSplitter::route(analysis.stmt_type, in_transaction);

        // Find shardable tables
        let mut target_shards: Option<Vec<usize>> = None;
        let mut table_to_rule: HashMap<&str, &ShardingRule> = HashMap::new();

        for table in &analysis.tables {
            if let Some(rule) = self.config.find_rule(table) {
                table_to_rule.insert(table.as_str(), rule);

                // Find shard key value for this table
                let shard_value = analysis
                    .shard_keys
                    .iter()
                    .find(|(col, _)| col.eq_ignore_ascii_case(&rule.shard_column))
                    .map(|(_, v)| v);

                let calc = self.config.find_calculator(table).unwrap();

                let shards = match shard_value {
                    Some(ShardKeyValue::Single(v)) => vec![calc.calculate(*v)],
                    Some(ShardKeyValue::Multiple(values)) => calc.calculate_all(values),
                    Some(ShardKeyValue::Range { start, end }) => {
                        calc.calculate_range_values(*start, *end)
                    }
                    Some(ShardKeyValue::Unknown) | None => calc.all_shards(),
                };

                // Intersect with existing shards if any
                target_shards = Some(match target_shards {
                    Some(existing) => {
                        let shards_set: std::collections::HashSet<_> = shards.into_iter().collect();
                        existing
                            .into_iter()
                            .filter(|s| shards_set.contains(s))
                            .collect()
                    }
                    None => shards,
                });
            }
        }

        // If no sharded tables, use default shard
        let shards = target_shards.unwrap_or_else(|| vec![0]);
        let is_scatter = shards.len() > 1;

        // Generate rewritten SQL for each shard
        let mut rewritten_sqls = HashMap::new();
        for &shard_idx in &shards {
            let shard_id = ShardId(format!("shard_{}", shard_idx));

            // Build table name mappings
            let mut table_mappings = HashMap::new();
            for (table, rule) in &table_to_rule {
                let physical_name =
                    SqlRewriter::generate_physical_table_name(&rule.table_pattern, shard_idx);
                table_mappings.insert((*table).to_string(), physical_name);
            }

            let rewritten = if table_mappings.is_empty() {
                sql.to_string()
            } else {
                SqlRewriter::rewrite(sql, &table_mappings)
            };

            rewritten_sqls.insert(shard_id, rewritten);
        }

        // Convert shard indices to ShardIds
        let shard_ids: Vec<ShardId> = shards
            .into_iter()
            .map(|idx| ShardId(format!("shard_{}", idx)))
            .collect();

        RouteResult {
            shards: shard_ids,
            target,
            rewritten_sqls,
            is_scatter,
        }
    }

    /// Add a sharding rule
    pub fn add_rule(&mut self, rule: ShardingRule) {
        self.config.add_rule(rule);
    }

    /// Check if a table is sharded
    pub fn is_sharded(&self, table_name: &str) -> bool {
        self.config.is_sharded(table_name)
    }
}

impl Default for Router {
    fn default() -> Self {
        Self::new(RouterConfig::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::{SqlAnalyzer, StatementType};

    fn setup_router() -> Router {
        let mut config = RouterConfig::new();
        config.add_rule(ShardingRule {
            name: "user_shard".to_string(),
            table_pattern: "users".to_string(),
            shard_column: "user_id".to_string(),
            algorithm: "mod".to_string(),
            shard_count: 4,
            range_boundaries: vec![],
        });
        Router::new(config)
    }

    #[test]
    fn test_route_with_shard_key() {
        let router = setup_router();
        let analyzer = SqlAnalyzer::new();

        let sql = "SELECT * FROM users WHERE user_id = 5";
        let analysis = analyzer.analyze(sql).unwrap();
        let result = router.route(sql, &analysis, false);

        assert_eq!(result.shards.len(), 1);
        assert_eq!(result.shards[0].0, "shard_1"); // 5 % 4 = 1
        assert!(!result.is_scatter);
        assert_eq!(result.target, RouteTarget::Slave);

        // Check rewritten SQL
        let rewritten = result.rewritten_sqls.get(&result.shards[0]).unwrap();
        assert!(rewritten.contains("users_1"));
    }

    #[test]
    fn test_route_without_shard_key() {
        let router = setup_router();
        let analyzer = SqlAnalyzer::new();

        let sql = "SELECT * FROM users WHERE name = 'test'";
        let analysis = analyzer.analyze(sql).unwrap();
        let result = router.route(sql, &analysis, false);

        // Should hit all shards
        assert_eq!(result.shards.len(), 4);
        assert!(result.is_scatter);
    }

    #[test]
    fn test_route_in_transaction() {
        let router = setup_router();
        let analyzer = SqlAnalyzer::new();

        let sql = "SELECT * FROM users WHERE user_id = 5";
        let analysis = analyzer.analyze(sql).unwrap();
        let result = router.route(sql, &analysis, true);

        // Should still route correctly but use master
        assert_eq!(result.target, RouteTarget::Master);
    }

    #[test]
    fn test_route_non_sharded_table() {
        let router = setup_router();
        let analyzer = SqlAnalyzer::new();

        let sql = "SELECT * FROM orders WHERE id = 1";
        let analysis = analyzer.analyze(sql).unwrap();
        let result = router.route(sql, &analysis, false);

        // Should use default shard
        assert_eq!(result.shards.len(), 1);
        assert!(!result.is_scatter);
    }
}
