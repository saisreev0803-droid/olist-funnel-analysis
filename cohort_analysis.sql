WITH base_orders AS (
  SELECT
    c.customer_unique_id AS user_id,
    SAFE_CAST(o.order_purchase_timestamp AS TIMESTAMP) AS order_purchase_ts,
    SAFE_CAST(o.order_delivered_customer_date AS TIMESTAMP) AS order_delivered_ts
  FROM `olist-funnel-analysis.funnel_project.orders` o
  JOIN `olist-funnel-analysis.funnel_project.customers` c
    ON o.customer_id = c.customer_id
),

signup AS (
  SELECT
    user_id,
    MIN(order_purchase_ts) AS signup_time
  FROM base_orders
  GROUP BY 1
),

funnel AS (
  SELECT
    s.user_id,
    DATE_TRUNC(DATE(s.signup_time), MONTH) AS cohort_month,
    MIN(IF(
      b.order_delivered_ts IS NOT NULL
      AND b.order_delivered_ts >= s.signup_time
      AND b.order_delivered_ts < TIMESTAMP_ADD(s.signup_time, INTERVAL 7 DAY),
      b.order_delivered_ts, NULL)) AS purchase_time
  FROM signup s
  LEFT JOIN base_orders b
    ON s.user_id = b.user_id
  GROUP BY 1,2
)
select cohort_month,count(*) as signup_users, countif(purchase_time is not null) as delivered_users, safe_divide(countif(purchase_time is not null), count(*)) as signup_to_delivery_rate from funnel 
group by 1
order by 1;
