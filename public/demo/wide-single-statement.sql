WITH base_orders AS (
  SELECT
    o.id,
    o.customer_id,
    o.session_id,
    o.billing_address_id,
    o.shipping_address_id,
    o.currency_id,
    o.status_id,
    o.sales_rep_id,
    o.warehouse_id,
    o.created_at,
    o.total_amount,
    o.discount_amount,
    o.tax_amount,
    o.shipping_amount,
    o.net_amount,
    o.gross_amount
  FROM orders o
  WHERE o.created_at >= DATE_TRUNC('year', CURRENT_DATE) - INTERVAL '18 months'
    AND o.deleted_at IS NULL
),
customer_rollup AS (
  SELECT
    c.id AS customer_id,
    c.name AS customer_name,
    c.email AS customer_email,
    c.phone AS customer_phone,
    c.segment AS customer_segment,
    c.country_code AS customer_country_code,
    c.marketing_status AS marketing_status,
    COUNT(DISTINCT o.id) AS lifetime_order_count,
    SUM(o.total_amount) AS lifetime_revenue,
    MAX(o.created_at) AS last_order_at
  FROM customers c
  LEFT JOIN orders o ON o.customer_id = c.id
  GROUP BY
    c.id,
    c.name,
    c.email,
    c.phone,
    c.segment,
    c.country_code,
    c.marketing_status
),
line_rollup AS (
  SELECT
    oi.order_id,
    COUNT(*) AS line_count,
    SUM(oi.quantity) AS total_units,
    SUM(oi.quantity * oi.unit_price) AS gross_line_value,
    SUM(oi.discount_amount) AS line_discount_value,
    COUNT(DISTINCT oi.product_id) AS distinct_product_count,
    MAX(oi.unit_price) AS highest_unit_price
  FROM order_items oi
  GROUP BY oi.order_id
),
refund_rollup AS (
  SELECT
    r.order_id,
    COUNT(*) AS refund_count,
    SUM(r.amount) AS refunded_amount,
    MAX(r.created_at) AS last_refund_at
  FROM refunds r
  WHERE r.status IN ('approved', 'completed')
  GROUP BY r.order_id
),
payment_rollup AS (
  SELECT
    p.order_id,
    COUNT(*) AS payment_attempts,
    SUM(CASE WHEN p.status = 'captured' THEN p.amount ELSE 0 END) AS captured_amount,
    MAX(CASE WHEN p.status = 'captured' THEN p.created_at END) AS latest_capture_at
  FROM payments p
  GROUP BY p.order_id
),
shipment_rollup AS (
  SELECT
    s.order_id,
    COUNT(*) AS shipment_count,
    MAX(s.shipped_at) AS last_shipped_at,
    SUM(CASE WHEN s.status = 'delivered' THEN 1 ELSE 0 END) AS delivered_shipments
  FROM shipments s
  GROUP BY s.order_id
),
support_rollup AS (
  SELECT
    t.order_id,
    COUNT(*) AS ticket_count,
    MAX(t.created_at) AS last_ticket_at,
    SUM(CASE WHEN t.priority = 'high' THEN 1 ELSE 0 END) AS high_priority_tickets
  FROM support_tickets t
  GROUP BY t.order_id
),
channel_rollup AS (
  SELECT
    cs.id AS session_id,
    cs.channel,
    cs.campaign,
    cs.source,
    cs.medium,
    cs.device_type,
    cs.landing_page
  FROM channel_sessions cs
),
login_anomalies AS (
  SELECT
    la.customer_id,
    COUNT(*) AS suspicious_login_count,
    MAX(la.created_at) AS latest_suspicious_login_at
  FROM login_audits la
  WHERE la.severity IN ('high', 'critical')
  GROUP BY la.customer_id
)
SELECT
  bo.id AS order_id,
  bo.created_at AS order_created_at,
  bo.total_amount,
  bo.discount_amount,
  bo.tax_amount,
  bo.shipping_amount,
  bo.net_amount,
  bo.gross_amount,
  c.id AS customer_id,
  c.name AS customer_name,
  c.email AS customer_email,
  c.phone AS customer_phone,
  c.segment AS customer_segment,
  ct.code AS customer_country_code,
  c.marketing_status AS customer_marketing_status,
  sr.name AS sales_rep_name,
  sr.email AS sales_rep_email,
  wh.name AS warehouse_name,
  wh.code AS warehouse_code,
  st.name AS status_name,
  cur.code AS currency_code,
  cur.symbol AS currency_symbol,
  ba.city AS billing_city,
  ba.country AS billing_country,
  sa.city AS shipping_city,
  sa.country AS shipping_country,
  lr.total_units,
  lr.gross_line_value,
  lr.line_discount_value,
  lr.distinct_product_count,
  lr.highest_unit_price,
  rr.refund_count,
  rr.refunded_amount,
  rr.last_refund_at,
  pr.payment_attempts,
  pr.captured_amount,
  pr.latest_capture_at,
  shr.shipment_count,
  shr.last_shipped_at,
  shr.delivered_shipments,
  spr.ticket_count,
  spr.last_ticket_at,
  spr.high_priority_tickets,
  cr.channel,
  cr.campaign,
  cr.source AS acquisition_source,
  cr.medium AS acquisition_medium,
  cr.device_type,
  cr.landing_page,
  cro.lifetime_order_count,
  cro.lifetime_revenue,
  cro.last_order_at AS customer_last_order_at,
  la.suspicious_login_count,
  la.latest_suspicious_login_at,
  (
    SELECT COUNT(*)
    FROM notes n
    WHERE n.order_id = bo.id
      AND n.visibility = 'internal'
  ) AS internal_note_count,
  (
    SELECT MAX(a.created_at)
    FROM order_audits a
    WHERE a.order_id = bo.id
      AND a.event_type = 'status_change'
  ) AS last_status_change_at,
  (
    SELECT COUNT(*)
    FROM support_tickets st2
    WHERE st2.order_id = bo.id
      AND st2.status NOT IN ('resolved', 'closed')
  ) AS open_ticket_count,
  CASE
    WHEN rr.refunded_amount > bo.total_amount * 0.5 THEN 'high_refund_risk'
    WHEN spr.high_priority_tickets >= 2 THEN 'support_attention'
    WHEN la.suspicious_login_count >= 3 THEN 'security_review'
    ELSE 'normal'
  END AS review_bucket
FROM base_orders bo
INNER JOIN customers c ON c.id = bo.customer_id
LEFT JOIN sales_reps sr ON sr.id = bo.sales_rep_id
LEFT JOIN warehouses wh ON wh.id = bo.warehouse_id
LEFT JOIN statuses st ON st.id = bo.status_id
LEFT JOIN currencies cur ON cur.id = bo.currency_id
LEFT JOIN addresses ba ON ba.id = bo.billing_address_id
LEFT JOIN addresses sa ON sa.id = bo.shipping_address_id
LEFT JOIN customer_rollup cro ON cro.customer_id = bo.customer_id
LEFT JOIN line_rollup lr ON lr.order_id = bo.id
LEFT JOIN refund_rollup rr ON rr.order_id = bo.id
LEFT JOIN payment_rollup pr ON pr.order_id = bo.id
LEFT JOIN shipment_rollup shr ON shr.order_id = bo.id
LEFT JOIN support_rollup spr ON spr.order_id = bo.id
LEFT JOIN channel_rollup cr ON cr.session_id = bo.session_id
LEFT JOIN countries ct ON ct.id = c.country_id
LEFT JOIN login_anomalies la ON la.customer_id = bo.customer_id
WHERE bo.total_amount > 25
  AND LOWER(sa.country) IN ('romania', 'germany', 'france', 'italy', 'spain')
  AND (
    lr.total_units > 1
    OR spr.ticket_count > 0
    OR rr.refund_count > 0
  )
ORDER BY bo.created_at DESC, bo.total_amount DESC
LIMIT 250;
