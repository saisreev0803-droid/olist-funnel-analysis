WITH base_orders AS (
  SELECT
    c.customer_unique_id AS user_id,
    o.order_id,
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
    MIN(order_purchase_ts) AS signup_time
  FROM base_orders
  WHERE order_purchase_ts IS NOT NULL
  GROUP BY 1
),
funnel as (select s.user_id, s.signup_time,MIN(
      IF(
        b.order_approved_ts IS NOT NULL
        AND b.order_approved_ts >= s.signup_time
        AND b.order_approved_ts < TIMESTAMP_ADD(s.signup_time, INTERVAL 7 DAY),
        b.order_approved_ts,
        NULL
      )
    ) AS approved_time,

    MIN(
      IF(
        b.order_shipped_ts IS NOT NULL
        AND b.order_shipped_ts >= s.signup_time
        AND b.order_shipped_ts < TIMESTAMP_ADD(s.signup_time, INTERVAL 7 DAY),
        b.order_shipped_ts,
        NULL
      )
    ) AS shipped_time,

    MIN(
      IF(
        b.order_delivered_ts IS NOT NULL
        AND b.order_delivered_ts >= s.signup_time
        AND b.order_delivered_ts < TIMESTAMP_ADD(s.signup_time, INTERVAL 7 DAY),
        b.order_delivered_ts,
        NULL
      )
    ) AS purchase_time

  FROM signup s
  LEFT JOIN base_orders b
    ON s.user_id = b.user_id
  GROUP BY 1, 2
) select count(*) as signup_users, countif(approved_time is not null) as approved_users, countif(shipped_time is not null) as shipped_users, countif(purchase_time is not null) as purchased_users from funnel;