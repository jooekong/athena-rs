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
use crate::pool::{DbGroupId, ShardIndex};

/// Route type indicating where the query should be executed
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteType {
    /// Route to specific shards by index
    Shards(Vec<ShardIndex>),
    /// Route to home group (non-sharded tables)
    Home,
}

impl RouteType {
    pub fn is_scatter(&self) -> bool {
        matches!(self, RouteType::Shards(shards) if shards.len() > 1)
    }

    pub fn is_home(&self) -> bool {
        matches!(self, RouteType::Home)
    }

    pub fn shard_indices(&self) -> Option<&[ShardIndex]> {
        match self {
            RouteType::Shards(indices) => Some(indices),
            RouteType::Home => None,
        }
    }

    /// Convert to DbGroupId for single-target routes
    pub fn to_group_id(&self) -> Option<DbGroupId> {
        match self {
            RouteType::Shards(indices) if indices.len() == 1 => Some(DbGroupId::Shard(indices[0])),
            RouteType::Home => Some(DbGroupId::Home),
            _ => None,
        }
    }

    /// Get all DbGroupIds for this route
    pub fn to_group_ids(&self) -> Vec<DbGroupId> {
        match self {
            RouteType::Shards(indices) => indices.iter().map(|&idx| DbGroupId::Shard(idx)).collect(),
            RouteType::Home => vec![DbGroupId::Home],
        }
    }
}

/// Routing result
#[derive(Debug, Clone)]
pub struct RouteResult {
    /// Route type (shards or home)
    pub route_type: RouteType,
    /// Read-write routing target
    pub target: RouteTarget,
    /// Rewritten SQL for each target (index corresponds to route_type shards order)
    pub rewritten_sqls: Vec<String>,
    /// Whether the shard intersection was empty
    pub empty_intersection: bool,
}

impl RouteResult {
    /// Get the single target DbGroupId (for non-scatter queries)
    pub fn target_group(&self) -> Option<DbGroupId> {
        self.route_type.to_group_id()
    }

    /// Get rewritten SQL by index
    pub fn get_sql(&self, index: usize) -> Option<&str> {
        self.rewritten_sqls.get(index).map(|s| s.as_str())
    }

    /// Get rewritten SQL for a DbGroupId
    pub fn get_sql_for_group(&self, group_id: &DbGroupId) -> Option<&str> {
        match (&self.route_type, group_id) {
            (RouteType::Home, DbGroupId::Home) => self.rewritten_sqls.first().map(|s| s.as_str()),
            (RouteType::Shards(indices), DbGroupId::Shard(idx)) => {
                indices.iter().position(|&i| i == *idx)
                    .and_then(|pos| self.rewritten_sqls.get(pos))
                    .map(|s| s.as_str())
            }
            _ => None,
        }
    }

    /// Check if this is a scatter query
    pub fn is_scatter(&self) -> bool {
        self.route_type.is_scatter()
    }

    /// Check if this routes to home
    pub fn is_home(&self) -> bool {
        self.route_type.is_home()
    }

    /// Get shard count
    pub fn shard_count(&self) -> usize {
        match &self.route_type {
            RouteType::Shards(indices) => indices.len(),
            RouteType::Home => 1,
        }
    }
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
    pub fn route(&self, sql: &str, analysis: &SqlAnalysis, in_transaction: bool) -> RouteResult {
        let target = RwSplitter::route(analysis.stmt_type, in_transaction);

        // Find shardable tables
        let mut target_shards: Option<Vec<usize>> = None;
        let mut table_to_rule: HashMap<&str, &ShardingRule> = HashMap::new();

        for table in &analysis.tables {
            if let Some(rule) = self.config.find_rule(table) {
                table_to_rule.insert(table.as_str(), rule);

                let shard_value = analysis
                    .shard_keys
                    .iter()
                    .find(|(col, _)| col.eq_ignore_ascii_case(&rule.shard_column))
                    .map(|(_, v)| v);

                let calc = self.config.find_calculator(table).unwrap();

                let shards = match shard_value {
                    Some(ShardKeyValue::Single(v)) => vec![calc.calculate(v)],
                    Some(ShardKeyValue::Multiple(values)) => calc.calculate_all(values),
                    Some(ShardKeyValue::Range { start, end }) => calc.calculate_range_values(*start, *end),
                    Some(ShardKeyValue::Unknown) | None => calc.all_shards(),
                };

                // Intersect with existing shards
                target_shards = Some(match target_shards {
                    Some(existing) => {
                        // Two-pointer intersection (O(n))
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

        // Determine route type and empty intersection
        let (route_type, empty_intersection) = match &target_shards {
            Some(s) if !s.is_empty() => (RouteType::Shards(s.clone()), false),
            Some(_) => (RouteType::Shards(vec![]), true),
            None => (RouteType::Home, false),
        };

        // Generate rewritten SQL
        let rewritten_sqls = match &route_type {
            RouteType::Home => vec![sql.to_string()],
            RouteType::Shards(indices) => {
                indices.iter().map(|&shard_idx| {
                    let mut table_mappings = HashMap::new();
                    for (table, rule) in &table_to_rule {
                        let physical_name = SqlRewriter::generate_physical_table_name(&rule.table_pattern, shard_idx);
                        table_mappings.insert((*table).to_string(), physical_name);
                    }

                    if table_mappings.is_empty() {
                        sql.to_string()
                    } else {
                        SqlRewriter::rewrite(sql, &table_mappings)
                    }
                }).collect()
            }
        };

        RouteResult {
            route_type,
            target,
            rewritten_sqls,
            empty_intersection,
        }
    }

    pub fn add_rule(&mut self, rule: ShardingRule) {
        self.config.add_rule(rule);
    }

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
    use crate::parser::SqlAnalyzer;

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

        assert_eq!(result.shard_count(), 1);
        assert!(!result.is_scatter());
        assert_eq!(result.target, RouteTarget::Slave);

        // Same value should always route to same shard
        let result2 = router.route(sql, &analysis, false);
        assert_eq!(result.route_type, result2.route_type);
    }

    #[test]
    fn test_route_without_shard_key() {
        let router = setup_router();
        let analyzer = SqlAnalyzer::new();

        let sql = "SELECT * FROM users WHERE name = 'test'";
        let analysis = analyzer.analyze(sql).unwrap();
        let result = router.route(sql, &analysis, false);

        assert_eq!(result.shard_count(), 4);
        assert!(result.is_scatter());
    }

    #[test]
    fn test_route_in_transaction() {
        let router = setup_router();
        let analyzer = SqlAnalyzer::new();

        let sql = "SELECT * FROM users WHERE user_id = 5";
        let analysis = analyzer.analyze(sql).unwrap();
        let result = router.route(sql, &analysis, true);

        assert_eq!(result.target, RouteTarget::Master);
    }

    #[test]
    fn test_route_non_sharded_table() {
        let router = setup_router();
        let analyzer = SqlAnalyzer::new();

        let sql = "SELECT * FROM orders WHERE id = 1";
        let analysis = analyzer.analyze(sql).unwrap();
        let result = router.route(sql, &analysis, false);

        assert!(result.is_home());
        assert!(!result.is_scatter());
    }

    #[test]
    fn test_route_empty_intersection_detected() {
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

        let calc = ShardCalculator::new(ShardAlgorithm::Mod, 4);

        // Find values that hash to different shards
        let mut user_id = 1i64;
        let mut order_id = 2i64;
        for u in 1..1000 {
            let user_shard = calc.calculate_i64(u);
            for o in 1..1000 {
                let order_shard = calc.calculate_i64(o);
                if user_shard != order_shard {
                    user_id = u;
                    order_id = o;
                    break;
                }
            }
            if calc.calculate_i64(user_id) != calc.calculate_i64(order_id) {
                break;
            }
        }

        let sql = format!(
            "SELECT * FROM users u JOIN orders o ON u.id = o.user_id WHERE u.user_id = {} AND o.order_id = {}",
            user_id, order_id
        );
        let analysis = analyzer.analyze(&sql).unwrap();
        let result = router.route(&sql, &analysis, false);

        assert!(result.empty_intersection);
        assert_eq!(result.shard_count(), 0);
    }

    #[test]
    fn test_route_non_sharded_not_empty_intersection() {
        let router = setup_router();
        let analyzer = SqlAnalyzer::new();

        let sql = "SELECT * FROM products WHERE id = 1";
        let analysis = analyzer.analyze(sql).unwrap();
        let result = router.route(sql, &analysis, false);

        assert!(!result.empty_intersection);
        assert!(result.is_home());
    }

    #[test]
    fn test_route_in_values() {
        let router = setup_router();
        let analyzer = SqlAnalyzer::new();

        let sql = "SELECT * FROM users WHERE user_id IN (1, 2, 3, 4, 5, 6, 7, 8)";
        let analysis = analyzer.analyze(sql).unwrap();
        let result = router.route(sql, &analysis, false);

        assert!(result.shard_count() >= 1);
        assert!(!result.empty_intersection);
    }

    #[test]
    fn test_route_string_shard_key() {
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

        assert_eq!(result.shard_count(), 1);
        assert!(!result.is_scatter());
        assert!(!result.empty_intersection);

        let result2 = router.route(sql, &analysis, false);
        assert_eq!(result.route_type, result2.route_type);
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

        assert!(result.shard_count() >= 1);
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
        assert_eq!(result.shard_count(), 1);
    }
}
