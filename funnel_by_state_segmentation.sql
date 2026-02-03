with base_orders as (select c.customer_unique_id as user_id, c.customer_state as customer_state, safe_cast(o.order_purchase_timestamp AS TIMESTAMP)  AS order_purchase_ts,
    SAFE_CAST(o.order_approved_at AS TIMESTAMP) AS order_approved_ts,
    SAFE_CAST(o.order_delivered_carrier_date AS TIMESTAMP) AS order_shipped_ts,
    SAFE_CAST(o.order_delivered_customer_date AS TIMESTAMP) AS order_delivered_ts
  FROM `olist-funnel-analysis.funnel_project.orders` o
  JOIN `olist-funnel-analysis.funnel_project.customers` c
    ON o.customer_id = c.customer_id),
    signup as(select user_id, any_value(customer_state) as customer_state, min(order_purchase_ts) as signup_time from base_orders where order_purchase_ts is not null group by 1), 
    funnel as(select s.user_id,s.customer_state,s.signup_time,
    min(if(b.order_shipped_ts IS NOT NULL
      AND b.order_shipped_ts >= s.signup_time
      AND b.order_shipped_ts < TIMESTAMP_ADD(s.signup_time, INTERVAL 7 DAY),
      b.order_shipped_ts, NULL)) as shipped_time,
       MIN(IF(
      b.order_delivered_ts IS NOT NULL
      AND b.order_delivered_ts >= s.signup_time
      AND b.order_delivered_ts < TIMESTAMP_ADD(s.signup_time, INTERVAL 7 DAY),
      b.order_delivered_ts, NULL)) AS purchase_time
      from signup s left join base_orders b on s.user_id = b.user_id group by 1,2,3),
      state_metrics as(select customer_state, countif(shipped_time is not null) as shipped_users, countif(purchase_time is not null) as purchased_users from funnel group by 1)
      SELECT
  customer_state,
  shipped_users,
  purchased_users,
  SAFE_DIVIDE(purchased_users, shipped_users) AS shipped_to_purchased_rate
FROM state_metrics
ORDER BY shipped_to_purchased_rate ASC;

