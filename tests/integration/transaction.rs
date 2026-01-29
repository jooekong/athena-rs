//! Transaction integration tests

use crate::{
    assert_query_error, find_different_shard_user_ids, get_proxy_config, get_shard_count,
    skip_if_not_enabled,
};
use mysql::prelude::*;

const TEST_PREFIX: &str = "it_tx_";

fn cleanup_user_id(conn: &mut mysql::PooledConn, user_id: &str) {
    let sql = format!("DELETE FROM orders WHERE user_id = '{}'", user_id);
    let _ = conn.query_drop(&sql);
}

fn parse_amount(s: &str) -> i64 {
    s.parse::<f64>().unwrap_or(0.0) as i64
}

#[test]
fn test_transaction_rollback() {
    skip_if_not_enabled!();

    let config = get_proxy_config();
    let mut conn = config.conn();

    let user_id = format!("{}rollback", TEST_PREFIX);
    let order_no = format!("ORD_{}rb", TEST_PREFIX);
    cleanup_user_id(&mut conn, &user_id);

    conn.query_drop("BEGIN").expect("BEGIN should succeed");

    let sql = format!(
        "INSERT INTO orders (user_id, order_no, amount) VALUES ('{}', '{}', 100.00)",
        user_id, order_no
    );
    conn.query_drop(&sql).expect("INSERT should succeed");

    let sql = format!("SELECT amount FROM orders WHERE user_id = '{}'", user_id);
    let result: Option<String> = conn.query_first(&sql).expect("SELECT should succeed");
    assert_eq!(result.map(|s| parse_amount(&s)), Some(100));

    conn.query_drop("ROLLBACK").expect("ROLLBACK should succeed");

    let result: Option<String> = conn.query_first(&sql).expect("SELECT should succeed");
    assert_eq!(result, None, "Data should be rolled back");
}

#[test]
fn test_transaction_commit() {
    skip_if_not_enabled!();

    let config = get_proxy_config();
    let mut conn = config.conn();

    let user_id = format!("{}commit", TEST_PREFIX);
    let order_no = format!("ORD_{}cm", TEST_PREFIX);
    cleanup_user_id(&mut conn, &user_id);

    conn.query_drop("BEGIN").expect("BEGIN should succeed");

    let sql = format!(
        "INSERT INTO orders (user_id, order_no, amount) VALUES ('{}', '{}', 200.00)",
        user_id, order_no
    );
    conn.query_drop(&sql).expect("INSERT should succeed");
    conn.query_drop("COMMIT").expect("COMMIT should succeed");

    let sql = format!("SELECT amount FROM orders WHERE user_id = '{}'", user_id);
    let result: Option<String> = conn.query_first(&sql).expect("SELECT should succeed");
    assert_eq!(result.map(|s| parse_amount(&s)), Some(200));

    cleanup_user_id(&mut conn, &user_id);
}

#[test]
fn test_transaction_isolation() {
    skip_if_not_enabled!();

    let config = get_proxy_config();
    let mut conn = config.conn();

    let user_id = format!("{}isolation", TEST_PREFIX);
    let order_no = format!("ORD_{}iso", TEST_PREFIX);
    cleanup_user_id(&mut conn, &user_id);

    conn.query_drop("BEGIN").expect("BEGIN should succeed");

    let sql = format!(
        "INSERT INTO orders (user_id, order_no, amount) VALUES ('{}', '{}', 300.00)",
        user_id, order_no
    );
    conn.query_drop(&sql).expect("INSERT should succeed");

    let sql = format!("SELECT amount FROM orders WHERE user_id = '{}'", user_id);
    let result: Option<String> = conn.query_first(&sql).expect("SELECT should succeed");
    assert_eq!(result.map(|s| parse_amount(&s)), Some(300));

    conn.query_drop("ROLLBACK").expect("ROLLBACK should succeed");
}

#[test]
fn test_empty_transaction() {
    skip_if_not_enabled!();

    let config = get_proxy_config();
    let mut conn = config.conn();

    conn.query_drop("BEGIN").expect("BEGIN should succeed");
    conn.query_drop("COMMIT").expect("COMMIT should succeed");

    conn.query_drop("BEGIN").expect("BEGIN should succeed");
    conn.query_drop("ROLLBACK").expect("ROLLBACK should succeed");
}

#[test]
fn test_cross_shard_query_in_transaction_rejected() {
    skip_if_not_enabled!();

    let config = get_proxy_config();
    let mut conn = config.conn();
    let shard_count = get_shard_count();

    let (user_a, user_b, shard_a, shard_b) = find_different_shard_user_ids(shard_count);
    eprintln!("user_a={} (shard {}), user_b={} (shard {})", user_a, shard_a, user_b, shard_b);

    conn.query_drop("BEGIN").expect("BEGIN should succeed");

    let sql = format!("SELECT COUNT(*) FROM orders WHERE user_id = '{}'", user_a);
    let _: Option<i64> = conn.query_first(&sql).expect("First SELECT should succeed");

    let sql = format!("SELECT COUNT(*) FROM orders WHERE user_id = '{}'", user_b);
    let result: Result<Option<i64>, _> = conn.query_first(&sql);

    assert_query_error(result, 1105, "Cross-shard query in transaction not allowed");
    let _ = conn.query_drop("ROLLBACK");
}

#[test]
fn test_scatter_query_in_transaction_rejected() {
    skip_if_not_enabled!();

    let config = get_proxy_config();
    let mut conn = config.conn();

    conn.query_drop("BEGIN").expect("BEGIN should succeed");

    let result: Result<Option<i64>, _> = conn.query_first("SELECT COUNT(*) FROM orders");
    assert_query_error(result, 1105, "Scatter queries not allowed in transaction");

    let _ = conn.query_drop("ROLLBACK");
}

#[test]
fn test_scatter_after_binding_in_transaction_rejected() {
    skip_if_not_enabled!();

    let config = get_proxy_config();
    let mut conn = config.conn();

    let user_id = format!("{}scatter_bind", TEST_PREFIX);

    conn.query_drop("BEGIN").expect("BEGIN should succeed");

    let sql = format!("SELECT COUNT(*) FROM orders WHERE user_id = '{}'", user_id);
    let _: Option<i64> = conn.query_first(&sql).expect("First SELECT should succeed");

    let result: Result<Option<i64>, _> = conn.query_first("SELECT COUNT(*) FROM orders");
    assert_query_error(result, 1105, "Scatter queries not allowed in transaction");

    let _ = conn.query_drop("ROLLBACK");
}

#[test]
fn test_same_shard_queries_in_transaction() {
    skip_if_not_enabled!();

    let config = get_proxy_config();
    let mut conn = config.conn();

    let user_id = format!("{}same_shard", TEST_PREFIX);
    let order_no = format!("ORD_{}ss", TEST_PREFIX);
    cleanup_user_id(&mut conn, &user_id);

    conn.query_drop("BEGIN").expect("BEGIN should succeed");

    let sql = format!(
        "INSERT INTO orders (user_id, order_no, amount) VALUES ('{}', '{}', 100.00)",
        user_id, order_no
    );
    conn.query_drop(&sql).expect("INSERT should succeed");

    let sql = format!("SELECT amount FROM orders WHERE user_id = '{}'", user_id);
    let v1: Option<String> = conn.query_first(&sql).expect("SELECT should succeed");
    assert_eq!(v1.map(|s| parse_amount(&s)), Some(100));

    let sql = format!("UPDATE orders SET amount = 200.00 WHERE user_id = '{}'", user_id);
    conn.query_drop(&sql).expect("UPDATE should succeed");

    let sql = format!("SELECT amount FROM orders WHERE user_id = '{}'", user_id);
    let v2: Option<String> = conn.query_first(&sql).expect("SELECT should succeed");
    assert_eq!(v2.map(|s| parse_amount(&s)), Some(200));

    conn.query_drop("ROLLBACK").expect("ROLLBACK should succeed");
}

#[test]
fn test_transaction_recovery_after_cross_shard_error() {
    skip_if_not_enabled!();

    let config = get_proxy_config();
    let mut conn = config.conn();
    let shard_count = get_shard_count();

    let (user_a, user_b, _, _) = find_different_shard_user_ids(shard_count);

    conn.query_drop("BEGIN").expect("BEGIN should succeed");

    let sql = format!("SELECT COUNT(*) FROM orders WHERE user_id = '{}'", user_a);
    let _: Option<i64> = conn.query_first(&sql).expect("First SELECT should succeed");

    let sql = format!("SELECT COUNT(*) FROM orders WHERE user_id = '{}'", user_b);
    let result: Result<Option<i64>, _> = conn.query_first(&sql);
    assert!(result.is_err(), "Cross-shard should fail");

    let sql = format!("SELECT COUNT(*) FROM orders WHERE user_id = '{}'", user_a);
    let result: Option<i64> = conn.query_first(&sql).expect("Bound shard should work");
    assert!(result.is_some());

    conn.query_drop("ROLLBACK").expect("ROLLBACK should succeed");
}
