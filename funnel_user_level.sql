WITH base_orders AS (
  SELECT
    c.customer_unique_id AS user_id,
    c.customer_state,
    SAFE_CAST(o.order_purchase_timestamp AS TIMESTAMP) AS order_purchase_ts,
    SAFE_CAST(o.order_approved_at AS TIMESTAMP) AS order_approved_ts,
    SAFE_CAST(o.order_delivered_carrier_date AS TIMESTAMP) AS order_shipped_ts,
    SAFE_CAST(o.order_delivered_customer_date AS TIMESTAMP) AS order_delivered_ts
  FROM `olist-funnel-analysis.funnel_project.orders` o
  JOIN `olist-funnel-analysis.funnel_project.customers` c
    ON o.customer_id = c.customer_id
),

signup AS (
  SELECT
    user_id,
    ANY_VALUE(customer_state) AS customer_state,
    MIN(order_purchase_ts) AS signup_time
  FROM base_orders
  GROUP BY 1
),

funnel AS (
  SELECT
    s.user_id,
    s.customer_state,
    DATE_TRUNC(DATE(s.signup_time), MONTH) AS cohort_month,

    MIN(IF(
      b.order_approved_ts IS NOT NULL
      AND b.order_approved_ts < TIMESTAMP_ADD(s.signup_time, INTERVAL 7 DAY),
      b.order_approved_ts, NULL)) AS approved_time,

    MIN(IF(
      b.order_shipped_ts IS NOT NULL
      AND b.order_shipped_ts < TIMESTAMP_ADD(s.signup_time, INTERVAL 7 DAY),
      b.order_shipped_ts, NULL)) AS shipped_time,

    MIN(IF(
      b.order_delivered_ts IS NOT NULL
      AND b.order_delivered_ts < TIMESTAMP_ADD(s.signup_time, INTERVAL 7 DAY),
      b.order_delivered_ts, NULL)) AS purchase_time

  FROM signup s
  LEFT JOIN base_orders b
    ON s.user_id = b.user_id
  GROUP BY 1,2,3
)

SELECT * FROM funnel;
