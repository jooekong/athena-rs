//! Read-write split integration tests

use crate::{get_proxy_config, skip_if_not_enabled};
use mysql::prelude::*;

const TEST_PREFIX: &str = "it_rw_";

fn cleanup_user_id(conn: &mut mysql::PooledConn, user_id: &str) {
    let sql = format!("DELETE FROM orders WHERE user_id = '{}'", user_id);
    let _ = conn.query_drop(&sql);
}

fn parse_amount(s: &str) -> i64 {
    s.parse::<f64>().unwrap_or(0.0) as i64
}

#[test]
fn test_read_can_route_to_slave() {
    skip_if_not_enabled!();

    let config = get_proxy_config();
    let mut conn = config.conn();

    let mut server_ids = std::collections::HashSet::new();
    for _ in 0..10 {
        let server_id: Option<u64> = conn
            .query_first("SELECT @@server_id")
            .expect("SELECT @@server_id should succeed");
        if let Some(id) = server_id {
            server_ids.insert(id);
        }
    }

    if server_ids.len() > 1 {
        eprintln!("Read-write split detected: {} servers", server_ids.len());
    } else {
        eprintln!("No slave routing detected (expected if no slaves)");
    }
}

#[test]
fn test_write_routes_to_master() {
    skip_if_not_enabled!();

    let config = get_proxy_config();
    let mut conn = config.conn();

    let user_id = format!("{}write_test", TEST_PREFIX);
    let order_no = format!("ORD_{}wr", TEST_PREFIX);
    cleanup_user_id(&mut conn, &user_id);

    let sql = format!(
        "INSERT INTO orders (user_id, order_no, amount) VALUES ('{}', '{}', 100.00)",
        user_id, order_no
    );
    conn.query_drop(&sql).expect("INSERT should succeed");

    let sql = format!("SELECT amount FROM orders WHERE user_id = '{}'", user_id);
    let result: Option<String> = conn.query_first(&sql).expect("SELECT should succeed");
    assert_eq!(result.map(|s| parse_amount(&s)), Some(100));

    cleanup_user_id(&mut conn, &user_id);
}

#[test]
fn test_transaction_reads_route_to_master() {
    skip_if_not_enabled!();

    let config = get_proxy_config();
    let mut conn = config.conn();

    let user_id = format!("{}tx_read", TEST_PREFIX);
    let order_no = format!("ORD_{}txr", TEST_PREFIX);
    cleanup_user_id(&mut conn, &user_id);

    conn.query_drop("BEGIN").expect("BEGIN should succeed");

    let sql = format!(
        "INSERT INTO orders (user_id, order_no, amount) VALUES ('{}', '{}', 200.00)",
        user_id, order_no
    );
    conn.query_drop(&sql).expect("INSERT should succeed");

    let sql = format!("SELECT amount FROM orders WHERE user_id = '{}'", user_id);
    let result: Option<String> = conn.query_first(&sql).expect("SELECT should succeed");
    assert_eq!(result.map(|s| parse_amount(&s)), Some(200));

    conn.query_drop("ROLLBACK").expect("ROLLBACK should succeed");

    let result: Option<String> = conn.query_first(&sql).expect("SELECT should succeed");
    assert_eq!(result, None, "Data should be rolled back");
}

#[test]
fn test_transaction_control_routes_to_master() {
    skip_if_not_enabled!();

    let config = get_proxy_config();
    let mut conn = config.conn();

    let user_id = format!("{}tx_commit", TEST_PREFIX);
    let order_no = format!("ORD_{}txc", TEST_PREFIX);
    cleanup_user_id(&mut conn, &user_id);

    conn.query_drop("BEGIN").expect("BEGIN should succeed");

    let sql = format!(
        "INSERT INTO orders (user_id, order_no, amount) VALUES ('{}', '{}', 300.00)",
        user_id, order_no
    );
    conn.query_drop(&sql).expect("INSERT should succeed");
    conn.query_drop("COMMIT").expect("COMMIT should succeed");

    let sql = format!("SELECT amount FROM orders WHERE user_id = '{}'", user_id);
    let result: Option<String> = conn.query_first(&sql).expect("SELECT should succeed");
    assert_eq!(result.map(|s| parse_amount(&s)), Some(300));

    cleanup_user_id(&mut conn, &user_id);
}

#[test]
fn test_read_your_writes_in_transaction() {
    skip_if_not_enabled!();

    let config = get_proxy_config();
    let mut conn = config.conn();

    let user_id = format!("{}ryw", TEST_PREFIX);
    let order_no = format!("ORD_{}ryw", TEST_PREFIX);
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
