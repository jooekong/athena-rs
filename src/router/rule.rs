use serde::Deserialize;
use std::borrow::Cow;
use std::collections::HashMap;

use super::shard::{ShardAlgorithm, ShardCalculator};

/// Convert string to lowercase, avoiding allocation if already lowercase.
/// Returns Cow::Borrowed if input is already lowercase (no allocation),
/// or Cow::Owned if conversion was needed.
#[inline]
fn to_lowercase_cow(s: &str) -> Cow<'_, str> {
    // Check if already lowercase (common case for SQL table names)
    if s.bytes().all(|b| !b.is_ascii_uppercase()) {
        Cow::Borrowed(s)
    } else {
        Cow::Owned(s.to_lowercase())
    }
}

/// Sharding rule configuration
#[derive(Debug, Clone, Deserialize)]
pub struct ShardingRule {
    /// Rule name (for logging/debugging)
    pub name: String,
    /// Table name pattern (exact match or pattern)
    pub table_pattern: String,
    /// Column name used as shard key
    pub shard_column: String,
    /// Sharding algorithm
    #[serde(default = "default_algorithm")]
    pub algorithm: String,
    /// Number of shards (for mod/hash algorithms)
    #[serde(default = "default_shard_count")]
    pub shard_count: usize,
    /// Range boundaries (for range algorithm)
    #[serde(default)]
    pub range_boundaries: Vec<i64>,
}

fn default_algorithm() -> String {
    "mod".to_string()
}

fn default_shard_count() -> usize {
    16
}

impl ShardingRule {
    /// Check if this rule matches a table name
    pub fn matches_table(&self, table_name: &str) -> bool {
        // Simple exact match (case-insensitive)
        // Could be extended to support wildcards
        self.table_pattern.eq_ignore_ascii_case(table_name)
    }

    /// Create a shard calculator from this rule
    pub fn create_calculator(&self) -> ShardCalculator {
        let algorithm = ShardAlgorithm::from_str(&self.algorithm)
            .unwrap_or(ShardAlgorithm::Mod);

        match algorithm {
            ShardAlgorithm::Range => {
                ShardCalculator::new_range(self.range_boundaries.clone())
            }
            _ => ShardCalculator::new(algorithm, self.shard_count),
        }
    }
}

/// Router configuration holding all sharding rules
#[derive(Debug, Default, Clone)]
pub struct RouterConfig {
    /// Sharding rules indexed by table pattern
    rules: HashMap<String, ShardingRule>,
    /// Pre-built calculators for each rule
    calculators: HashMap<String, ShardCalculator>,
}

impl RouterConfig {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a sharding rule
    pub fn add_rule(&mut self, rule: ShardingRule) {
        let pattern = rule.table_pattern.to_lowercase();
        let calculator = rule.create_calculator();
        self.calculators.insert(pattern.clone(), calculator);
        self.rules.insert(pattern, rule);
    }

    /// Find the rule for a table
    pub fn find_rule(&self, table_name: &str) -> Option<&ShardingRule> {
        // Use Cow to avoid allocation when input is already lowercase
        let key = to_lowercase_cow(table_name);
        self.rules.get(key.as_ref())
    }

    /// Find the calculator for a table
    pub fn find_calculator(&self, table_name: &str) -> Option<&ShardCalculator> {
        // Use Cow to avoid allocation when input is already lowercase
        let key = to_lowercase_cow(table_name);
        self.calculators.get(key.as_ref())
    }

    // Intentionally no public rule enumeration helpers yet.
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rule_matching() {
        let rule = ShardingRule {
            name: "user_shard".to_string(),
            table_pattern: "users".to_string(),
            shard_column: "user_id".to_string(),
            algorithm: "mod".to_string(),
            shard_count: 16,
            range_boundaries: vec![],
        };

        assert!(rule.matches_table("users"));
        assert!(rule.matches_table("USERS"));
        assert!(rule.matches_table("Users"));
        assert!(!rule.matches_table("user"));
        assert!(!rule.matches_table("users2"));
    }

    #[test]
    fn test_router_config() {
        let mut config = RouterConfig::new();

        config.add_rule(ShardingRule {
            name: "user_shard".to_string(),
            table_pattern: "users".to_string(),
            shard_column: "user_id".to_string(),
            algorithm: "mod".to_string(),
            shard_count: 16,
            range_boundaries: vec![],
        });

        assert!(config.find_rule("users").is_some());
        assert!(config.find_rule("USERS").is_some());
        assert!(config.find_rule("orders").is_none());

        let rule = config.find_rule("users").unwrap();
        assert_eq!(rule.shard_column, "user_id");

        let calc = config.find_calculator("users").unwrap();
        assert_eq!(calc.all_shards().len(), 16);
    }
}
