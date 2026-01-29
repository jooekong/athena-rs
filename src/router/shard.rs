use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Shard key value - supports both integer and string types
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ShardValue {
    Integer(i64),
    String(String),
}

impl ShardValue {
    /// Compute hash code for this value
    pub fn hash_code(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }

    /// Try to convert to i64 (for range algorithm)
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            ShardValue::Integer(v) => Some(*v),
            ShardValue::String(_) => None,
        }
    }
}

impl From<i64> for ShardValue {
    fn from(v: i64) -> Self {
        ShardValue::Integer(v)
    }
}

impl From<String> for ShardValue {
    fn from(v: String) -> Self {
        ShardValue::String(v)
    }
}

impl From<&str> for ShardValue {
    fn from(v: &str) -> Self {
        ShardValue::String(v.to_string())
    }
}

/// Sharding algorithm
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardAlgorithm {
    /// Modulo-based sharding: shard_id = hash(value) % shard_count
    /// Note: Uses hashcode to support both integer and string keys
    Mod,
    /// Hash-based sharding: shard_id = hash(value) % shard_count
    Hash,
    /// Range-based sharding: defined by range boundaries (integer only)
    Range,
}

impl ShardAlgorithm {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "mod" | "modulo" => Some(Self::Mod),
            "hash" => Some(Self::Hash),
            "range" => Some(Self::Range),
            _ => None,
        }
    }
}

/// Shard calculator
#[derive(Debug, Clone)]
pub struct ShardCalculator {
    algorithm: ShardAlgorithm,
    shard_count: usize,
    /// Range boundaries for range-based sharding (sorted)
    range_boundaries: Vec<i64>,
}

impl ShardCalculator {
    /// Create a new mod/hash shard calculator
    pub fn new(algorithm: ShardAlgorithm, shard_count: usize) -> Self {
        Self {
            algorithm,
            shard_count,
            range_boundaries: vec![],
        }
    }

    /// Create a range-based shard calculator
    ///
    /// # Arguments
    /// * `boundaries` - Sorted list of range boundaries
    ///   For example, [100, 200, 300] means:
    ///   - shard 0: value < 100
    ///   - shard 1: 100 <= value < 200
    ///   - shard 2: 200 <= value < 300
    ///   - shard 3: value >= 300
    pub fn new_range(boundaries: Vec<i64>) -> Self {
        Self {
            algorithm: ShardAlgorithm::Range,
            shard_count: boundaries.len() + 1,
            range_boundaries: boundaries,
        }
    }

    /// Calculate shard index for a given ShardValue
    pub fn calculate(&self, value: &ShardValue) -> usize {
        match self.algorithm {
            ShardAlgorithm::Mod | ShardAlgorithm::Hash => {
                // Both mod and hash use hashcode for consistency and string support
                let hash = value.hash_code();
                (hash % self.shard_count as u64) as usize
            }
            ShardAlgorithm::Range => {
                // Range only works with integers
                match value.as_i64() {
                    Some(v) => self.calculate_range_int(v),
                    None => 0, // Default to shard 0 for non-integer values
                }
            }
        }
    }

    /// Calculate shard index for an i64 value (convenience method)
    pub fn calculate_i64(&self, value: i64) -> usize {
        self.calculate(&ShardValue::Integer(value))
    }

    /// Calculate all shard indices for a list of ShardValues
    pub fn calculate_all(&self, values: &[ShardValue]) -> Vec<usize> {
        let mut shards: Vec<usize> = values.iter().map(|v| self.calculate(v)).collect();
        shards.sort();
        shards.dedup();
        shards
    }

    /// Calculate shard indices for a range of integer values
    pub fn calculate_range_values(&self, start: i64, end: i64) -> Vec<usize> {
        match self.algorithm {
            ShardAlgorithm::Range => {
                // For range algorithm, find all shards that overlap with [start, end]
                let start_shard = self.calculate_range_int(start);
                let end_shard = self.calculate_range_int(end);
                (start_shard..=end_shard).collect()
            }
            ShardAlgorithm::Mod | ShardAlgorithm::Hash => {
                // For mod/hash based on hashcode, we need to check all shards if range is large
                let range_size = (end - start).abs() as usize;
                if range_size >= self.shard_count {
                    // Range covers all shards
                    (0..self.shard_count).collect()
                } else {
                    // Check each value in range
                    let mut shards: Vec<usize> = (start..=end)
                        .map(|v| self.calculate_i64(v))
                        .collect();
                    shards.sort();
                    shards.dedup();
                    shards
                }
            }
        }
    }

    /// Get all shard indices (for full table scan)
    pub fn all_shards(&self) -> Vec<usize> {
        (0..self.shard_count).collect()
    }

    fn calculate_range_int(&self, value: i64) -> usize {
        // Use binary search for O(log n) lookup
        // partition_point returns the index of the first boundary > value
        // which is exactly the shard index we need
        self.range_boundaries.partition_point(|&b| b <= value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mod_sharding_integers() {
        let calc = ShardCalculator::new(ShardAlgorithm::Mod, 16);

        // Mod now uses hashcode, so results are different from simple modulo
        // But same value should always map to same shard
        let shard0 = calc.calculate_i64(0);
        let shard0_again = calc.calculate_i64(0);
        assert_eq!(shard0, shard0_again);

        // Different values may map to different shards
        let shard123 = calc.calculate_i64(123);
        assert!(shard123 < 16);
    }

    #[test]
    fn test_mod_sharding_strings() {
        let calc = ShardCalculator::new(ShardAlgorithm::Mod, 16);

        // String sharding should be consistent
        let shard1 = calc.calculate(&ShardValue::String("user_abc".to_string()));
        let shard2 = calc.calculate(&ShardValue::String("user_abc".to_string()));
        assert_eq!(shard1, shard2);

        // Different strings may map to different shards
        let shard_a = calc.calculate(&ShardValue::String("alice".to_string()));
        let shard_b = calc.calculate(&ShardValue::String("bob".to_string()));
        assert!(shard_a < 16);
        assert!(shard_b < 16);
    }

    #[test]
    fn test_hash_sharding_integers() {
        let calc = ShardCalculator::new(ShardAlgorithm::Hash, 16);

        // Hash should be consistent
        let shard1 = calc.calculate_i64(123);
        let shard2 = calc.calculate_i64(123);
        assert_eq!(shard1, shard2);

        // Shard should be within range
        assert!(calc.calculate_i64(999) < 16);
    }

    #[test]
    fn test_hash_sharding_strings() {
        let calc = ShardCalculator::new(ShardAlgorithm::Hash, 16);

        // String hashing should be consistent
        let shard1 = calc.calculate(&ShardValue::String("test_key".to_string()));
        let shard2 = calc.calculate(&ShardValue::String("test_key".to_string()));
        assert_eq!(shard1, shard2);

        assert!(calc.calculate(&ShardValue::String("another_key".to_string())) < 16);
    }

    #[test]
    fn test_mod_and_hash_produce_same_results() {
        // Since both mod and hash now use hashcode, they should produce same results
        let mod_calc = ShardCalculator::new(ShardAlgorithm::Mod, 8);
        let hash_calc = ShardCalculator::new(ShardAlgorithm::Hash, 8);

        // Integer values
        assert_eq!(mod_calc.calculate_i64(100), hash_calc.calculate_i64(100));
        assert_eq!(mod_calc.calculate_i64(999), hash_calc.calculate_i64(999));

        // String values
        let str_val = ShardValue::String("test".to_string());
        assert_eq!(mod_calc.calculate(&str_val), hash_calc.calculate(&str_val));
    }

    #[test]
    fn test_range_sharding() {
        let calc = ShardCalculator::new_range(vec![100, 200, 300]);

        assert_eq!(calc.calculate_i64(50), 0);   // < 100
        assert_eq!(calc.calculate_i64(100), 1);  // >= 100, < 200
        assert_eq!(calc.calculate_i64(150), 1);
        assert_eq!(calc.calculate_i64(200), 2);  // >= 200, < 300
        assert_eq!(calc.calculate_i64(300), 3);  // >= 300
        assert_eq!(calc.calculate_i64(999), 3);
    }

    #[test]
    fn test_range_sharding_with_string_fallback() {
        let calc = ShardCalculator::new_range(vec![100, 200, 300]);

        // String values should fallback to shard 0 for range algorithm
        let shard = calc.calculate(&ShardValue::String("not_a_number".to_string()));
        assert_eq!(shard, 0);
    }

    #[test]
    fn test_calculate_all_integers() {
        let calc = ShardCalculator::new(ShardAlgorithm::Hash, 4);

        // Test with values that hash to same shard
        let values: Vec<ShardValue> = vec![
            ShardValue::Integer(100),
            ShardValue::Integer(100), // duplicate
        ];
        let shards = calc.calculate_all(&values);
        assert_eq!(shards.len(), 1); // Deduped
    }

    #[test]
    fn test_calculate_all_mixed() {
        let calc = ShardCalculator::new(ShardAlgorithm::Hash, 4);

        let values: Vec<ShardValue> = vec![
            ShardValue::Integer(1),
            ShardValue::Integer(2),
            ShardValue::String("a".to_string()),
            ShardValue::String("b".to_string()),
        ];
        let shards = calc.calculate_all(&values);
        // All shards should be within range
        for shard in &shards {
            assert!(*shard < 4);
        }
    }

    #[test]
    fn test_all_shards() {
        let calc = ShardCalculator::new(ShardAlgorithm::Mod, 4);
        assert_eq!(calc.all_shards(), vec![0, 1, 2, 3]);
    }

    #[test]
    fn test_shard_value_hash_consistency() {
        // Verify that ShardValue hash is consistent
        let v1 = ShardValue::Integer(42);
        let v2 = ShardValue::Integer(42);
        assert_eq!(v1.hash_code(), v2.hash_code());

        let s1 = ShardValue::String("hello".to_string());
        let s2 = ShardValue::String("hello".to_string());
        assert_eq!(s1.hash_code(), s2.hash_code());

        // Different values should (usually) have different hashes
        let v3 = ShardValue::Integer(43);
        assert_ne!(v1.hash_code(), v3.hash_code());
    }
}
