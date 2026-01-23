use sqlparser::ast::{
    BinaryOperator, Expr, Query, Select, SelectItem, SetExpr, Statement, TableFactor,
    TableWithJoins, Value,
};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
use tracing::debug;

/// SQL analysis result
#[derive(Debug, Clone)]
pub struct SqlAnalysis {
    /// Type of SQL statement
    pub stmt_type: StatementType,
    /// Tables referenced in the query
    pub tables: Vec<String>,
    /// Extracted shard key values (column_name -> values)
    pub shard_keys: Vec<(String, ShardKeyValue)>,
    /// Whether this is a read-only query
    pub is_read_only: bool,
}

/// Type of SQL statement
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatementType {
    Select,
    Insert,
    Update,
    Delete,
    Begin,
    Commit,
    Rollback,
    Set,
    Show,
    Use,
    Other,
}

impl StatementType {
    pub fn is_read_only(&self) -> bool {
        matches!(self, StatementType::Select | StatementType::Show)
    }

    pub fn is_transaction_control(&self) -> bool {
        matches!(
            self,
            StatementType::Begin | StatementType::Commit | StatementType::Rollback
        )
    }

    pub fn is_write(&self) -> bool {
        matches!(
            self,
            StatementType::Insert | StatementType::Update | StatementType::Delete
        )
    }
}

/// Shard key value extracted from SQL
#[derive(Debug, Clone)]
pub enum ShardKeyValue {
    /// Single value (e.g., user_id = 123)
    Single(i64),
    /// Multiple values (e.g., user_id IN (1, 2, 3))
    Multiple(Vec<i64>),
    /// Range (e.g., user_id BETWEEN 1 AND 100)
    Range { start: i64, end: i64 },
    /// Unknown/not extractable
    Unknown,
}

/// SQL Analyzer
pub struct SqlAnalyzer {
    dialect: MySqlDialect,
}

impl SqlAnalyzer {
    pub fn new() -> Self {
        Self {
            dialect: MySqlDialect {},
        }
    }

    /// Parse and analyze SQL
    pub fn analyze(&self, sql: &str) -> Result<SqlAnalysis, AnalyzerError> {
        let sql_trimmed = sql.trim();

        // Quick check for transaction control statements
        let sql_upper = sql_trimmed.to_uppercase();
        if sql_upper.starts_with("BEGIN") || sql_upper.starts_with("START TRANSACTION") {
            return Ok(SqlAnalysis {
                stmt_type: StatementType::Begin,
                tables: vec![],
                shard_keys: vec![],
                is_read_only: false,
            });
        }
        if sql_upper.starts_with("COMMIT") {
            return Ok(SqlAnalysis {
                stmt_type: StatementType::Commit,
                tables: vec![],
                shard_keys: vec![],
                is_read_only: false,
            });
        }
        if sql_upper.starts_with("ROLLBACK") {
            return Ok(SqlAnalysis {
                stmt_type: StatementType::Rollback,
                tables: vec![],
                shard_keys: vec![],
                is_read_only: false,
            });
        }

        // Parse SQL
        let statements = Parser::parse_sql(&self.dialect, sql_trimmed)
            .map_err(|e| AnalyzerError::ParseError(e.to_string()))?;

        if statements.is_empty() {
            return Err(AnalyzerError::EmptyStatement);
        }

        let stmt = &statements[0];
        self.analyze_statement(stmt)
    }

    fn analyze_statement(&self, stmt: &Statement) -> Result<SqlAnalysis, AnalyzerError> {
        match stmt {
            Statement::Query(query) => self.analyze_query(query),
            Statement::Insert {
                table_name,
                columns,
                source,
                ..
            } => {
                let table = table_name.to_string();
                let mut shard_keys = vec![];

                // Try to extract shard key from VALUES
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

                Ok(SqlAnalysis {
                    stmt_type: StatementType::Insert,
                    tables: vec![table],
                    shard_keys,
                    is_read_only: false,
                })
            }
            Statement::Update { table, selection, .. } => {
                let table_name = self.extract_table_name_from_table_with_joins(table);
                let shard_keys = selection
                    .as_ref()
                    .map(|expr| self.extract_shard_keys_from_expr(expr))
                    .unwrap_or_default();

                Ok(SqlAnalysis {
                    stmt_type: StatementType::Update,
                    tables: table_name.into_iter().collect(),
                    shard_keys,
                    is_read_only: false,
                })
            }
            Statement::Delete {
                from,
                selection,
                ..
            } => {
                let tables: Vec<String> = from
                    .iter()
                    .flat_map(|t| self.extract_table_name_from_table_with_joins(t))
                    .collect();

                let shard_keys = selection
                    .as_ref()
                    .map(|expr| self.extract_shard_keys_from_expr(expr))
                    .unwrap_or_default();

                Ok(SqlAnalysis {
                    stmt_type: StatementType::Delete,
                    tables,
                    shard_keys,
                    is_read_only: false,
                })
            }
            Statement::SetVariable { .. } => Ok(SqlAnalysis {
                stmt_type: StatementType::Set,
                tables: vec![],
                shard_keys: vec![],
                is_read_only: false,
            }),
            Statement::ShowTables { .. }
            | Statement::ShowColumns { .. } => Ok(SqlAnalysis {
                stmt_type: StatementType::Show,
                tables: vec![],
                shard_keys: vec![],
                is_read_only: true,
            }),
            Statement::Use { db_name } => Ok(SqlAnalysis {
                stmt_type: StatementType::Use,
                tables: vec![db_name.to_string()],
                shard_keys: vec![],
                is_read_only: false,
            }),
            _ => Ok(SqlAnalysis {
                stmt_type: StatementType::Other,
                tables: vec![],
                shard_keys: vec![],
                is_read_only: false,
            }),
        }
    }

    fn analyze_query(&self, query: &Query) -> Result<SqlAnalysis, AnalyzerError> {
        let mut tables = vec![];
        let mut shard_keys = vec![];

        if let SetExpr::Select(select) = query.body.as_ref() {
            // Extract table names from FROM clause
            for table_with_joins in &select.from {
                tables.extend(self.extract_table_name_from_table_with_joins(table_with_joins));
            }

            // Extract shard keys from WHERE clause
            if let Some(selection) = &select.selection {
                shard_keys = self.extract_shard_keys_from_expr(selection);
            }
        }

        Ok(SqlAnalysis {
            stmt_type: StatementType::Select,
            tables,
            shard_keys,
            is_read_only: true,
        })
    }

    fn extract_table_name_from_table_with_joins(
        &self,
        table_with_joins: &TableWithJoins,
    ) -> Vec<String> {
        let mut tables = vec![];

        // Main table
        if let TableFactor::Table { name, .. } = &table_with_joins.relation {
            tables.push(name.to_string());
        }

        // Joined tables
        for join in &table_with_joins.joins {
            if let TableFactor::Table { name, .. } = &join.relation {
                tables.push(name.to_string());
            }
        }

        tables
    }

    fn extract_shard_keys_from_expr(&self, expr: &Expr) -> Vec<(String, ShardKeyValue)> {
        let mut result = vec![];
        self.extract_shard_keys_recursive(expr, &mut result);
        result
    }

    fn extract_shard_keys_recursive(
        &self,
        expr: &Expr,
        result: &mut Vec<(String, ShardKeyValue)>,
    ) {
        match expr {
            // Handle: column = value
            Expr::BinaryOp { left, op, right } => {
                match op {
                    BinaryOperator::Eq => {
                        if let Some((col, val)) = self.extract_column_value_pair(left, right) {
                            result.push((col, ShardKeyValue::Single(val)));
                        }
                    }
                    BinaryOperator::And => {
                        // Recurse into both sides
                        self.extract_shard_keys_recursive(left, result);
                        self.extract_shard_keys_recursive(right, result);
                    }
                    _ => {}
                }
            }
            // Handle: column IN (v1, v2, v3)
            Expr::InList { expr, list, negated } if !negated => {
                if let Expr::Identifier(ident) = expr.as_ref() {
                    let values: Vec<i64> = list
                        .iter()
                        .filter_map(|e| self.extract_literal_value(e))
                        .collect();
                    if !values.is_empty() {
                        result.push((ident.value.clone(), ShardKeyValue::Multiple(values)));
                    }
                }
            }
            // Handle: column BETWEEN start AND end
            Expr::Between {
                expr,
                low,
                high,
                negated,
            } if !negated => {
                if let Expr::Identifier(ident) = expr.as_ref() {
                    if let (Some(start), Some(end)) = (
                        self.extract_literal_value(low),
                        self.extract_literal_value(high),
                    ) {
                        result.push((
                            ident.value.clone(),
                            ShardKeyValue::Range { start, end },
                        ));
                    }
                }
            }
            // Handle nested expressions
            Expr::Nested(inner) => {
                self.extract_shard_keys_recursive(inner, result);
            }
            _ => {}
        }
    }

    fn extract_column_value_pair(&self, left: &Expr, right: &Expr) -> Option<(String, i64)> {
        // Try column = value
        if let Expr::Identifier(ident) = left {
            if let Some(val) = self.extract_literal_value(right) {
                return Some((ident.value.clone(), val));
            }
        }
        // Try value = column
        if let Expr::Identifier(ident) = right {
            if let Some(val) = self.extract_literal_value(left) {
                return Some((ident.value.clone(), val));
            }
        }
        None
    }

    fn extract_literal_value(&self, expr: &Expr) -> Option<i64> {
        match expr {
            Expr::Value(Value::Number(n, _)) => n.parse().ok(),
            Expr::UnaryOp { op: sqlparser::ast::UnaryOperator::Minus, expr } => {
                if let Expr::Value(Value::Number(n, _)) = expr.as_ref() {
                    n.parse::<i64>().ok().map(|v| -v)
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

impl Default for SqlAnalyzer {
    fn default() -> Self {
        Self::new()
    }
}

/// Analyzer errors
#[derive(Debug, thiserror::Error)]
pub enum AnalyzerError {
    #[error("Failed to parse SQL: {0}")]
    ParseError(String),

    #[error("Empty statement")]
    EmptyStatement,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_select_analysis() {
        let analyzer = SqlAnalyzer::new();

        let sql = "SELECT * FROM users WHERE user_id = 123";
        let result = analyzer.analyze(sql).unwrap();

        assert_eq!(result.stmt_type, StatementType::Select);
        assert_eq!(result.tables, vec!["users"]);
        assert!(result.is_read_only);
        assert_eq!(result.shard_keys.len(), 1);
        assert_eq!(result.shard_keys[0].0, "user_id");
    }

    #[test]
    fn test_insert_analysis() {
        let analyzer = SqlAnalyzer::new();

        let sql = "INSERT INTO users (user_id, name) VALUES (123, 'test')";
        let result = analyzer.analyze(sql).unwrap();

        assert_eq!(result.stmt_type, StatementType::Insert);
        assert_eq!(result.tables, vec!["users"]);
        assert!(!result.is_read_only);
    }

    #[test]
    fn test_transaction_control() {
        let analyzer = SqlAnalyzer::new();

        assert_eq!(
            analyzer.analyze("BEGIN").unwrap().stmt_type,
            StatementType::Begin
        );
        assert_eq!(
            analyzer.analyze("COMMIT").unwrap().stmt_type,
            StatementType::Commit
        );
        assert_eq!(
            analyzer.analyze("ROLLBACK").unwrap().stmt_type,
            StatementType::Rollback
        );
    }
}
