-- Athena-RS Proxy CRUD Test SQL
-- 测试日期: 2026-01-27
-- 测试数据库: athena_rs_test
-- 代理地址: 127.0.0.1:3307

-- ============================================
-- 1. CREATE - 创建测试表
-- ============================================
CREATE TABLE IF NOT EXISTS users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(200),
    age INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- 2. INSERT - 插入数据
-- ============================================
INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@example.com', 25);
INSERT INTO users (name, email, age) VALUES ('Bob', 'bob@example.com', 30);
INSERT INTO users (name, email, age) VALUES ('Charlie', 'charlie@example.com', 28);

-- ============================================
-- 3. SELECT - 查询数据
-- ============================================
-- 查询所有数据
SELECT * FROM users;

-- 条件查询
SELECT name, age FROM users WHERE age > 26;

-- 聚合查询
SELECT COUNT(*) as total FROM users;

-- ============================================
-- 4. UPDATE - 更新数据
-- ============================================
UPDATE users SET age = 26 WHERE name = 'Alice';

-- 验证更新
SELECT * FROM users WHERE name = 'Alice';

-- ============================================
-- 5. DELETE - 删除数据
-- ============================================
DELETE FROM users WHERE name = 'Charlie';

-- 验证删除
SELECT * FROM users;

-- ============================================
-- 测试结果总结
-- ============================================
-- CREATE: ✓ 表创建成功
-- INSERT: ✓ 插入3条数据成功
-- SELECT: ✓ 查询正常返回结果
-- UPDATE: ✓ 更新 Alice 的年龄从 25 改为 26
-- DELETE: ✓ 删除 Charlie 记录成功
