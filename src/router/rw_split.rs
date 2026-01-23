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

        // Route based on statement type
        match stmt_type {
            // Read-only operations can go to slave
            StatementType::Select | StatementType::Show => RouteTarget::Slave,

            // Write operations must go to master
            StatementType::Insert
            | StatementType::Update
            | StatementType::Delete
            | StatementType::Begin
            | StatementType::Commit
            | StatementType::Rollback => RouteTarget::Master,

            // Other operations go to master for safety
            StatementType::Set | StatementType::Use | StatementType::Other => RouteTarget::Master,
        }
    }

    /// Check if a statement should be routed to slave
    pub fn can_use_slave(stmt_type: StatementType, in_transaction: bool) -> bool {
        Self::route(stmt_type, in_transaction) == RouteTarget::Slave
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
