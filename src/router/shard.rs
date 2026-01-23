use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Sharding algorithm
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardAlgorithm {
    /// Modulo-based sharding: shard_id = value % shard_count
    Mod,
    /// Hash-based sharding: shard_id = hash(value) % shard_count
    Hash,
    /// Range-based sharding: defined by range boundaries
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
#[derive(Debug)]
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

    /// Calculate shard index for a given value
    pub fn calculate(&self, value: i64) -> usize {
        match self.algorithm {
            ShardAlgorithm::Mod => self.calculate_mod(value),
            ShardAlgorithm::Hash => self.calculate_hash(value),
            ShardAlgorithm::Range => self.calculate_range(value),
        }
    }

    /// Calculate all shard indices for a list of values
    pub fn calculate_all(&self, values: &[i64]) -> Vec<usize> {
        let mut shards: Vec<usize> = values.iter().map(|v| self.calculate(*v)).collect();
        shards.sort();
        shards.dedup();
        shards
    }

    /// Calculate shard indices for a range of values
    pub fn calculate_range_values(&self, start: i64, end: i64) -> Vec<usize> {
        match self.algorithm {
            ShardAlgorithm::Range => {
                // For range algorithm, find all shards that overlap with [start, end]
                let start_shard = self.calculate_range(start);
                let end_shard = self.calculate_range(end);
                (start_shard..=end_shard).collect()
            }
            ShardAlgorithm::Mod | ShardAlgorithm::Hash => {
                // For mod/hash, we need to check all shards if range is large
                let range_size = (end - start).abs() as usize;
                if range_size >= self.shard_count {
                    // Range covers all shards
                    (0..self.shard_count).collect()
                } else {
                    // Check each value in range
                    let mut shards: Vec<usize> = (start..=end)
                        .map(|v| self.calculate(v))
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

    /// Get shard count
    pub fn shard_count(&self) -> usize {
        self.shard_count
    }

    fn calculate_mod(&self, value: i64) -> usize {
        let unsigned = if value >= 0 {
            value as u64
        } else {
            // Handle negative values by converting to positive
            (value.wrapping_abs() as u64).wrapping_neg()
        };
        (unsigned % self.shard_count as u64) as usize
    }

    fn calculate_hash(&self, value: i64) -> usize {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        let hash = hasher.finish();
        (hash % self.shard_count as u64) as usize
    }

    fn calculate_range(&self, value: i64) -> usize {
        for (i, &boundary) in self.range_boundaries.iter().enumerate() {
            if value < boundary {
                return i;
            }
        }
        self.range_boundaries.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mod_sharding() {
        let calc = ShardCalculator::new(ShardAlgorithm::Mod, 16);

        assert_eq!(calc.calculate(0), 0);
        assert_eq!(calc.calculate(16), 0);
        assert_eq!(calc.calculate(17), 1);
        assert_eq!(calc.calculate(123), 123 % 16);
    }

    #[test]
    fn test_hash_sharding() {
        let calc = ShardCalculator::new(ShardAlgorithm::Hash, 16);

        // Hash should be consistent
        let shard1 = calc.calculate(123);
        let shard2 = calc.calculate(123);
        assert_eq!(shard1, shard2);

        // Shard should be within range
        assert!(calc.calculate(999) < 16);
    }

    #[test]
    fn test_range_sharding() {
        let calc = ShardCalculator::new_range(vec![100, 200, 300]);

        assert_eq!(calc.calculate(50), 0);   // < 100
        assert_eq!(calc.calculate(100), 1);  // >= 100, < 200
        assert_eq!(calc.calculate(150), 1);
        assert_eq!(calc.calculate(200), 2);  // >= 200, < 300
        assert_eq!(calc.calculate(300), 3);  // >= 300
        assert_eq!(calc.calculate(999), 3);
    }

    #[test]
    fn test_calculate_all() {
        let calc = ShardCalculator::new(ShardAlgorithm::Mod, 4);

        let shards = calc.calculate_all(&[1, 5, 9, 13]); // All map to shard 1
        assert_eq!(shards, vec![1]);

        let shards = calc.calculate_all(&[1, 2, 3, 4]);
        assert_eq!(shards, vec![0, 1, 2, 3]);
    }

    #[test]
    fn test_all_shards() {
        let calc = ShardCalculator::new(ShardAlgorithm::Mod, 4);
        assert_eq!(calc.all_shards(), vec![0, 1, 2, 3]);
    }
}
