mod rule;
mod rw_split;
mod selector;
mod shard;

pub use rule::{RouterConfig, ShardingRule};
pub use rw_split::{RouteTarget, RwSplitter};
pub use selector::{FirstSelector, InstanceSelector, ReadWriteRouter, RoundRobinSelector};
pub use shard::{ShardAlgorithm, ShardCalculator, ShardValue};

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
    /// Whether the shard intersection was empty (multiple sharded tables with no common shard)
    pub empty_intersection: bool,
    /// Whether this query is routed to "home" db_group (for non-sharded tables)
    pub is_home: bool,
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
                    Some(ShardKeyValue::Single(v)) => vec![calc.calculate(v)],
                    Some(ShardKeyValue::Multiple(values)) => calc.calculate_all(values),
                    Some(ShardKeyValue::Range { start, end }) => {
                        calc.calculate_range_values(*start, *end)
                    }
                    Some(ShardKeyValue::Unknown) | None => calc.all_shards(),
                };

                // Intersect with existing shards if any
                // Use sorted vectors for O(n) intersection without HashSet allocation
                target_shards = Some(match target_shards {
                    Some(mut existing) => {
                        let mut shards = shards;
                        existing.sort_unstable();
                        shards.sort_unstable();
                        // Two-pointer intersection
                        let mut result = Vec::new();
                        let (mut i, mut j) = (0, 0);
                        while i < existing.len() && j < shards.len() {
                            match existing[i].cmp(&shards[j]) {
                                std::cmp::Ordering::Less => i += 1,
                                std::cmp::Ordering::Greater => j += 1,
                                std::cmp::Ordering::Equal => {
                                    result.push(existing[i]);
                                    i += 1;
                                    j += 1;
                                }
                            }
                        }
                        result
                    }
                    None => shards,
                });
            }
        }

        // Determine if we have an empty intersection from sharded tables
        let (shards, empty_intersection, use_home) = match &target_shards {
            Some(s) if !s.is_empty() => (s.clone(), false, false),
            Some(_) => {
                // Empty intersection: multiple sharded tables with no common shard
                // Return empty shards - caller MUST check empty_intersection flag
                (vec![], true, false)
            }
            None => {
                // No sharded tables - use "home" db_group if available
                (vec![], false, true)
            }
        };
        let is_scatter = shards.len() > 1;

        // Generate rewritten SQL for each shard
        let mut rewritten_sqls = HashMap::new();
        
        // For non-sharded tables, use "home" db_group
        if use_home {
            let shard_id = ShardId("home".to_string());
            // No table name rewriting for non-sharded tables
            rewritten_sqls.insert(shard_id.clone(), sql.to_string());
            
            return RouteResult {
                shards: vec![shard_id],
                target,
                rewritten_sqls,
                is_scatter: false,
                empty_intersection: false,
                is_home: true,
            };
        }
        
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
            empty_intersection,
            is_home: false,
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

        // Should route to exactly one shard
        assert_eq!(result.shards.len(), 1);
        assert!(!result.is_scatter);
        assert_eq!(result.target, RouteTarget::Slave);

        // Shard ID should be in expected format
        assert!(result.shards[0].0.starts_with("shard_"));

        // Check rewritten SQL contains the physical table name
        let rewritten = result.rewritten_sqls.get(&result.shards[0]).unwrap();
        let shard_idx: usize = result.shards[0].0.strip_prefix("shard_").unwrap().parse().unwrap();
        assert!(rewritten.contains(&format!("users_{}", shard_idx)));

        // Same value should always route to same shard (consistency check)
        let result2 = router.route(sql, &analysis, false);
        assert_eq!(result.shards[0], result2.shards[0]);
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

    #[test]
    fn test_route_empty_intersection_detected() {
        // Setup router with two tables that have different sharding
        let mut config = RouterConfig::new();
        config.add_rule(ShardingRule {
            name: "users_shard".to_string(),
            table_pattern: "users".to_string(),
            shard_column: "user_id".to_string(),
            algorithm: "mod".to_string(),
            shard_count: 4,
            range_boundaries: vec![],
        });
        config.add_rule(ShardingRule {
            name: "orders_shard".to_string(),
            table_pattern: "orders".to_string(),
            shard_column: "order_id".to_string(),
            algorithm: "mod".to_string(),
            shard_count: 4,
            range_boundaries: vec![],
        });
        let router = Router::new(config);
        let analyzer = SqlAnalyzer::new();

        // Find two values that hash to different shards to create an empty intersection
        let calc = ShardCalculator::new(ShardAlgorithm::Mod, 4);

        // Try many combinations to find values that hash to different shards
        let mut user_id = 1i64;
        let mut order_id = 2i64;
        let mut found = false;

        for u in 1..1000 {
            let user_shard = calc.calculate_i64(u);
            for o in 1..1000 {
                let order_shard = calc.calculate_i64(o);
                if user_shard != order_shard {
                    user_id = u;
                    order_id = o;
                    found = true;
                    break;
                }
            }
            if found {
                break;
            }
        }

        assert!(found, "Could not find values that hash to different shards");

        // Query joining two tables with incompatible shard keys
        let sql = format!(
            "SELECT * FROM users u JOIN orders o ON u.id = o.user_id WHERE u.user_id = {} AND o.order_id = {}",
            user_id, order_id
        );
        let analysis = analyzer.analyze(&sql).unwrap();
        let result = router.route(&sql, &analysis, false);

        // Should flag as empty intersection
        assert!(
            result.empty_intersection,
            "Expected empty_intersection=true for user_id={} (shard {}) and order_id={} (shard {})",
            user_id,
            calc.calculate_i64(user_id),
            order_id,
            calc.calculate_i64(order_id)
        );
        // Shards should be empty when intersection is empty
        assert!(result.shards.is_empty());
    }

    #[test]
    fn test_route_non_sharded_not_empty_intersection() {
        let router = setup_router();
        let analyzer = SqlAnalyzer::new();

        // Query on non-sharded table should NOT be flagged as empty intersection
        let sql = "SELECT * FROM products WHERE id = 1";
        let analysis = analyzer.analyze(sql).unwrap();
        let result = router.route(sql, &analysis, false);

        assert!(!result.empty_intersection);
        assert_eq!(result.shards.len(), 1);
    }

    #[test]
    fn test_route_in_values() {
        let router = setup_router();
        let analyzer = SqlAnalyzer::new();

        // Test scatter query with IN clause
        let sql = "SELECT * FROM users WHERE user_id IN (1, 2, 3, 4, 5, 6, 7, 8)";
        let analysis = analyzer.analyze(sql).unwrap();
        let result = router.route(sql, &analysis, false);

        // Should hit multiple shards (scatter)
        assert!(result.shards.len() >= 1);
        assert!(!result.empty_intersection);
    }

    #[test]
    fn test_route_string_shard_key() {
        // Setup router with string-based sharding
        let mut config = RouterConfig::new();
        config.add_rule(ShardingRule {
            name: "tenant_shard".to_string(),
            table_pattern: "orders".to_string(),
            shard_column: "tenant_id".to_string(),
            algorithm: "hash".to_string(),
            shard_count: 4,
            range_boundaries: vec![],
        });
        let router = Router::new(config);
        let analyzer = SqlAnalyzer::new();

        let sql = "SELECT * FROM orders WHERE tenant_id = 'acme_corp'";
        let analysis = analyzer.analyze(sql).unwrap();
        let result = router.route(sql, &analysis, false);

        // Should route to exactly one shard
        assert_eq!(result.shards.len(), 1);
        assert!(!result.is_scatter);
        assert!(!result.empty_intersection);

        // Same query should always route to same shard
        let result2 = router.route(sql, &analysis, false);
        assert_eq!(result.shards[0], result2.shards[0]);
    }

    #[test]
    fn test_route_string_shard_key_in_list() {
        let mut config = RouterConfig::new();
        config.add_rule(ShardingRule {
            name: "tenant_shard".to_string(),
            table_pattern: "orders".to_string(),
            shard_column: "tenant_id".to_string(),
            algorithm: "hash".to_string(),
            shard_count: 4,
            range_boundaries: vec![],
        });
        let router = Router::new(config);
        let analyzer = SqlAnalyzer::new();

        let sql = "SELECT * FROM orders WHERE tenant_id IN ('acme', 'beta', 'gamma', 'delta')";
        let analysis = analyzer.analyze(sql).unwrap();
        let result = router.route(sql, &analysis, false);

        // Should route to one or more shards depending on hash distribution
        assert!(!result.shards.is_empty());
        assert!(!result.empty_intersection);
    }

    #[test]
    fn test_route_write_to_master() {
        let router = setup_router();
        let analyzer = SqlAnalyzer::new();

        let sql = "INSERT INTO users (user_id, name) VALUES (5, 'test')";
        let analysis = analyzer.analyze(sql).unwrap();
        let result = router.route(sql, &analysis, false);

        assert_eq!(result.target, RouteTarget::Master);
        assert_eq!(result.shards.len(), 1);
    }
}
