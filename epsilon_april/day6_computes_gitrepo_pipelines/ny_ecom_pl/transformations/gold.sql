
create materialized view naval_silver.customers_active as 
  (select * except (__START_AT,__END_AT )from naval_silver.customers_cleaned_pl where __END_AT is null);

-- Sales enriched with customer and product details
CREATE OR REFRESH MATERIALIZED VIEW naval_silver.sales_enriched AS
SELECT
  s.order_id,
  s.order_date,
  s.customer_id,
  c.customer_name,
  c.customer_city,
  c.customer_state,
  s.product_id,
  p.product_name,
  p.product_category,
  p.product_price,
  s.quantity,
  s.discount_amount,
  s.total_amount
FROM naval_silver.sales_cleaned_pl s
JOIN naval_silver.customers_active c ON s.customer_id = c.customer_id
JOIN naval_silver.products_cleaned_pl p ON s.product_id = p.product_id;


-- Revenue insights per customer
CREATE OR REFRESH MATERIALIZED VIEW naval_silver.revenue_by_customer AS
SELECT
  customer_id,
  customer_name,
  customer_city,
  customer_state,
  COUNT(order_id) AS total_orders,
  SUM(quantity) AS total_items,
  ROUND(SUM(total_amount), 2) AS total_revenue,
  ROUND(AVG(total_amount), 2) AS avg_order_value,
  ROUND(SUM(discount_amount), 2) AS total_discount
FROM naval_silver.sales_enriched
GROUP BY ALL;


-- Revenue insights per product category
CREATE OR REFRESH MATERIALIZED VIEW naval_silver.revenue_by_category AS
SELECT
  product_category,
  COUNT(DISTINCT product_id) AS product_count,
  COUNT(order_id) AS total_orders,
  SUM(quantity) AS total_items_sold,
  ROUND(SUM(total_amount), 2) AS total_revenue,
  ROUND(AVG(total_amount), 2) AS avg_order_value,
  ROUND(AVG(product_price), 2) AS avg_product_price
FROM naval_silver.sales_enriched
GROUP BY ALL;


-- Monthly sales trend summary
CREATE OR REFRESH MATERIALIZED VIEW naval_silver.monthly_sales_summary AS
SELECT
  DATE_TRUNC('MONTH', order_date) AS sales_month,
  COUNT(order_id) AS total_orders,
  COUNT(DISTINCT customer_id) AS unique_customers,
  SUM(quantity) AS total_items,
  ROUND(SUM(total_amount), 2) AS total_revenue,
  ROUND(AVG(total_amount), 2) AS avg_order_value
FROM naval_silver.sales_enriched
GROUP BY ALL
ORDER BY sales_month;
