use std::collections::HashMap;

/// SQL Rewriter for table name replacement
pub struct SqlRewriter;

impl SqlRewriter {
    /// Rewrite SQL by replacing logical table names with physical table names
    ///
    /// # Arguments
    /// * `sql` - Original SQL string
    /// * `table_mappings` - Map of logical table name to physical table name
    ///
    /// # Returns
    /// Rewritten SQL string
    pub fn rewrite(sql: &str, table_mappings: &HashMap<String, String>) -> String {
        if table_mappings.is_empty() {
            return sql.to_string();
        }

        let mut result = sql.to_string();

        for (logical, physical) in table_mappings {
            // Skip if no change needed
            if logical == physical {
                continue;
            }

            // Replace table names (case-insensitive matching, but preserve case in replacement)
            result = Self::replace_table_name(&result, logical, physical);
        }

        result
    }

    /// Replace a single table name in SQL
    fn replace_table_name(sql: &str, old_name: &str, new_name: &str) -> String {
        // Build regex-like patterns for different contexts where table name can appear
        // We need to handle:
        // - FROM table_name
        // - JOIN table_name
        // - INTO table_name
        // - UPDATE table_name
        // - table_name.column
        // - `table_name`

        let mut result = String::with_capacity(sql.len() + 32);
        let sql_bytes = sql.as_bytes();
        let old_bytes = old_name.as_bytes();
        let old_len = old_bytes.len();
        let mut i = 0;

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
                        || !is_identifier_char(sql_bytes[i + old_len]);

                    if before_ok && after_ok {
                        result.push_str(new_name);
                        i += old_len;
                        continue;
                    }
                }
            }

            result.push(sql_bytes[i] as char);
            i += 1;
        }

        result
    }

    /// Generate physical table name from logical table name and shard index
    ///
    /// # Examples
    /// - `users` + shard 3 -> `users_3`
    /// - `order` + shard 15 -> `order_15`
    pub fn generate_physical_table_name(logical_name: &str, shard_index: usize) -> String {
        format!("{}_{}", logical_name, shard_index)
    }

    /// Generate physical table name with custom suffix pattern
    ///
    /// # Arguments
    /// * `logical_name` - Logical table name
    /// * `shard_index` - Shard index
    /// * `pad_width` - Zero-padding width (e.g., 4 for 0001, 0002, ...)
    ///
    /// # Examples
    /// - `users`, 3, 4 -> `users_0003`
    pub fn generate_physical_table_name_padded(
        logical_name: &str,
        shard_index: usize,
        pad_width: usize,
    ) -> String {
        format!("{}_{:0>width$}", logical_name, shard_index, width = pad_width)
    }
}

/// Check if a byte is a valid identifier character
fn is_identifier_char(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_rewrite() {
        let mut mappings = HashMap::new();
        mappings.insert("users".to_string(), "users_3".to_string());

        let sql = "SELECT * FROM users WHERE user_id = 123";
        let result = SqlRewriter::rewrite(sql, &mappings);
        assert_eq!(result, "SELECT * FROM users_3 WHERE user_id = 123");
    }

    #[test]
    fn test_rewrite_with_join() {
        let mut mappings = HashMap::new();
        mappings.insert("users".to_string(), "users_3".to_string());
        mappings.insert("orders".to_string(), "orders_3".to_string());

        let sql = "SELECT * FROM users JOIN orders ON users.id = orders.user_id";
        let result = SqlRewriter::rewrite(sql, &mappings);
        assert_eq!(
            result,
            "SELECT * FROM users_3 JOIN orders_3 ON users_3.id = orders_3.user_id"
        );
    }

    #[test]
    fn test_rewrite_backtick() {
        let mut mappings = HashMap::new();
        mappings.insert("users".to_string(), "users_3".to_string());

        let sql = "SELECT * FROM `users` WHERE user_id = 123";
        let result = SqlRewriter::rewrite(sql, &mappings);
        assert_eq!(result, "SELECT * FROM `users_3` WHERE user_id = 123");
    }

    #[test]
    fn test_rewrite_insert() {
        let mut mappings = HashMap::new();
        mappings.insert("users".to_string(), "users_5".to_string());

        let sql = "INSERT INTO users (id, name) VALUES (1, 'test')";
        let result = SqlRewriter::rewrite(sql, &mappings);
        assert_eq!(result, "INSERT INTO users_5 (id, name) VALUES (1, 'test')");
    }

    #[test]
    fn test_generate_physical_table_name() {
        assert_eq!(
            SqlRewriter::generate_physical_table_name("users", 3),
            "users_3"
        );
        assert_eq!(
            SqlRewriter::generate_physical_table_name("order", 15),
            "order_15"
        );
    }

    #[test]
    fn test_generate_physical_table_name_padded() {
        assert_eq!(
            SqlRewriter::generate_physical_table_name_padded("users", 3, 4),
            "users_0003"
        );
        assert_eq!(
            SqlRewriter::generate_physical_table_name_padded("users", 123, 4),
            "users_0123"
        );
    }
}
