-- Booked revenue excludes cancelled orders and counts each line once.
SELECT d.calendar_date, SUM(s.line_amount) AS booked_revenue
FROM analytics.fact_sales s JOIN analytics.dim_date d ON d.date_key = s.date_key
WHERE s.order_status = 'COMPLETE'
GROUP BY d.calendar_date ORDER BY d.calendar_date;

-- Product performance and current inventory.
SELECT p.product_id, p.product_name, p.stock_quantity,
       COALESCE(SUM(s.quantity), 0) AS units_sold
FROM analytics.dim_product p
LEFT JOIN analytics.fact_sales s ON s.product_id = p.product_id AND s.order_status = 'COMPLETE'
GROUP BY p.product_id, p.product_name, p.stock_quantity ORDER BY units_sold DESC, p.product_id;

-- Repeat customers count distinct orders, since an order may have multiple lines.
SELECT customer_id, COUNT(DISTINCT order_id) AS completed_orders
FROM analytics.fact_sales
WHERE order_status = 'COMPLETE'
GROUP BY customer_id HAVING COUNT(DISTINCT order_id) > 1;

-- Success rate at the payment-attempt grain.
SELECT COUNT(*) AS attempts,
       SUM(CASE WHEN payment_status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_attempts,
       AVG(CASE WHEN payment_status = 'SUCCESS' THEN 1.0 ELSE 0.0 END) AS success_rate
FROM analytics.fact_payment;
