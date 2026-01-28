SELECT COUNT(*) FROM olist-funnel-analysis.funnel_project.orders;
SELECT COUNT(*) FROM olist-funnel-analysis.funnel_project.customers;
SELECT COUNT(*) FROM olist-funnel-analysis.funnel_project.payments;
SELECT *FROM olist-funnel-analysis.funnel_project.orders;
SELECT 
  COUNT(*) AS total_rows,
  COUNT(DISTINCT customer_unique_id) AS unique_users
FROM olist-funnel-analysis.funnel_project.customers;
SELECT 
  c.customer_unique_id,
  COUNT(o.order_id) AS orders_count
FROM `olist-funnel-analysis.funnel_project.orders` o
JOIN `olist-funnel-analysis.funnel_project.customers` c
  ON o.customer_id = c.customer_id
GROUP BY 1
ORDER BY orders_count DESC
LIMIT 10;
SELECT *
FROM `olist-funnel-analysis.funnel_project.payments`
LIMIT 5;

