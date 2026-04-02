import type { SqlDialect } from './types';

const POSTGRES_SAMPLE_SQL = `SELECT
  o.id,
  o.created_at,
  c.name AS customer_name,
  SUM(oi.quantity * oi.unit_price) AS total_revenue,
  COUNT(DISTINCT p.id) AS product_count
FROM orders o
INNER JOIN customers c ON c.id = o.customer_id
LEFT JOIN order_items oi ON oi.order_id = o.id
LEFT JOIN products p ON p.id = oi.product_id
WHERE o.created_at >= DATE_TRUNC('month', CURRENT_DATE)
  AND LOWER(c.country) = 'romania'
GROUP BY o.id, o.created_at, c.name
ORDER BY total_revenue DESC
LIMIT 50;`;

const SAMPLE_SQL_BY_DIALECT: Record<SqlDialect, string> = {
  postgres: POSTGRES_SAMPLE_SQL,
  mysql: `SELECT
  o.id,
  o.created_at,
  c.name AS customer_name,
  SUM(oi.quantity * oi.unit_price) AS total_revenue
FROM orders o
INNER JOIN customers c ON c.id = o.customer_id
LEFT JOIN order_items oi ON oi.order_id = o.id
WHERE o.created_at >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
  AND LOWER(c.country) = 'romania'
GROUP BY o.id, o.created_at, c.name
ORDER BY total_revenue DESC
LIMIT 50 OFFSET 0;`,
  mariadb: `SELECT
  o.id,
  c.name AS customer_name,
  SUM(oi.quantity) AS total_qty
FROM orders o
STRAIGHT_JOIN customers c ON c.id = o.customer_id
LEFT JOIN order_items oi ON oi.order_id = o.id
WHERE o.created_at >= DATE_SUB(CURDATE(), INTERVAL 14 DAY)
GROUP BY o.id, c.name
ORDER BY total_qty DESC
LIMIT 25;`,
  sqlite: `SELECT
  o.id,
  strftime('%Y-%m-%d', o.created_at) AS order_day,
  c.name AS customer_name,
  COUNT(oi.id) AS item_count
FROM orders o
INNER JOIN customers c ON c.id = o.customer_id
LEFT JOIN order_items oi ON oi.order_id = o.id
WHERE date(o.created_at) >= date('now', '-30 day')
GROUP BY o.id, order_day, c.name
ORDER BY item_count DESC
LIMIT 50;`,
  bigquery: `SELECT
  o.id,
  item.product_id,
  c.name AS customer_name
FROM \`analytics.orders\` o
INNER JOIN \`analytics.customers\` c ON c.id = o.customer_id
LEFT JOIN UNNEST(o.items) AS item
QUALIFY ROW_NUMBER() OVER (PARTITION BY o.id ORDER BY item.product_id) = 1;`,
  sqlserver: `SELECT TOP (25)
  o.id,
  o.created_at,
  c.name AS customer_name
FROM [dbo].[Orders] o WITH (NOLOCK)
INNER JOIN [dbo].[Customers] c ON c.id = o.customer_id
LEFT JOIN [dbo].[OrderItems] oi ON oi.order_id = o.id
WHERE o.created_at >= DATEADD(day, -30, CAST(GETDATE() AS date))
ORDER BY o.created_at DESC
OFFSET 0 ROWS FETCH NEXT 25 ROWS ONLY;`,
  oracle: `SELECT
  o.id,
  o.created_at,
  c.name AS customer_name
FROM orders o
INNER JOIN customers c ON c.id = o.customer_id
LEFT JOIN order_items oi ON oi.order_id = o.id
WHERE TRUNC(o.created_at) >= TRUNC(SYSDATE) - 30
  AND ROWNUM <= 25
ORDER BY o.created_at DESC;`,
  snowflake: `SELECT
  o.id,
  c.name AS customer_name,
  oi.order_id
FROM orders o
INNER JOIN customers c ON c.id = o.customer_id
LEFT JOIN order_items oi ON oi.order_id = o.id
QUALIFY ROW_NUMBER() OVER (PARTITION BY o.id ORDER BY o.created_at DESC) = 1;`,
  duckdb: `SELECT
  o.id,
  c.name AS customer_name
FROM read_parquet('orders.parquet') o
LEFT JOIN customers c ON c.id = o.customer_id
WHERE date_trunc('month', o.created_at) = date_trunc('month', current_date)
LIMIT 25;`,
  redshift: `SELECT
  o.id,
  c.name AS customer_name
FROM orders o
INNER JOIN customers c ON c.id = o.customer_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY o.id ORDER BY o.created_at DESC) = 1;`,
  trino: `SELECT
  o.id,
  item.item_id
FROM hive.sales.orders o
CROSS JOIN UNNEST(o.item_ids) AS item(item_id)
FETCH FIRST 25 ROWS ONLY;`,
};

export const SAMPLE_SCHEMA_SQL = `CREATE TABLE customers (
  id BIGINT PRIMARY KEY,
  name TEXT,
  country TEXT
);

CREATE TABLE orders (
  id BIGINT PRIMARY KEY,
  customer_id BIGINT NOT NULL REFERENCES customers(id),
  created_at TIMESTAMP
);

CREATE INDEX idx_orders_customer_id ON orders(customer_id);

CREATE TABLE order_items (
  id BIGINT PRIMARY KEY,
  order_id BIGINT NOT NULL REFERENCES orders(id),
  product_id BIGINT,
  quantity INTEGER,
  unit_price NUMERIC
);

CREATE INDEX idx_order_items_order_id ON order_items(order_id);
`;

export const getDialectSampleSql = (dialect: SqlDialect) => SAMPLE_SQL_BY_DIALECT[dialect] ?? POSTGRES_SAMPLE_SQL;
