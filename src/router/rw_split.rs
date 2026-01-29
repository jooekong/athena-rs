use crate::parser::StatementType;

/// Read-write routing decision
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteTarget {
    /// Route to master (for writes or when in transaction)
    Master,
    /// Route to slave (for reads outside transaction)
    Slave,
}

/// Read-write splitter
pub struct RwSplitter;

impl RwSplitter {
    /// Determine routing target based on statement type and transaction state
    ///
    /// # Arguments
    /// * `stmt_type` - Type of SQL statement
    /// * `in_transaction` - Whether the session is in a transaction
    ///
    /// # Returns
    /// Routing target (Master or Slave)
    pub fn route(stmt_type: StatementType, in_transaction: bool) -> RouteTarget {
        // Always use master when in transaction
        if in_transaction {
            return RouteTarget::Master;
        }

        if stmt_type.is_read_only() {
            return RouteTarget::Slave;
        }

        RouteTarget::Master
    }

}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_select_outside_transaction() {
        assert_eq!(
            RwSplitter::route(StatementType::Select, false),
            RouteTarget::Slave
        );
    }

    #[test]
    fn test_select_inside_transaction() {
        assert_eq!(
            RwSplitter::route(StatementType::Select, true),
            RouteTarget::Master
        );
    }

    #[test]
    fn test_write_operations() {
        assert_eq!(
            RwSplitter::route(StatementType::Insert, false),
            RouteTarget::Master
        );
        assert_eq!(
            RwSplitter::route(StatementType::Update, false),
            RouteTarget::Master
        );
        assert_eq!(
            RwSplitter::route(StatementType::Delete, false),
            RouteTarget::Master
        );
    }

    #[test]
    fn test_transaction_control() {
        assert_eq!(
            RwSplitter::route(StatementType::Begin, false),
            RouteTarget::Master
        );
        assert_eq!(
            RwSplitter::route(StatementType::Commit, true),
            RouteTarget::Master
        );
        assert_eq!(
            RwSplitter::route(StatementType::Rollback, true),
            RouteTarget::Master
        );
    }
}
