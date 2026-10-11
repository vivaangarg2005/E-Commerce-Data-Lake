-- Query 1: Total orders, units, and net revenue by year and month
-- Excludes cancelled orders to reflect true realized top-line revenue
SELECT 
    year,
    month,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(quantity) AS total_units_sold,
    ROUND(SUM(gross_amount), 2) AS total_gross_revenue,
    ROUND(SUM(discount_amount), 2) AS total_discounts,
    ROUND(SUM(net_amount), 2) AS total_net_revenue
FROM fact_order_items
WHERE is_cancelled = false
GROUP BY year, month
ORDER BY year ASC, month ASC;
