-- Query 2: Net revenue, unit sales, and average selling price by product category
SELECT 
    category,
    COUNT(DISTINCT order_id) AS distinct_orders,
    SUM(quantity) AS units_sold,
    ROUND(SUM(net_amount), 2) AS category_net_revenue,
    ROUND(AVG(unit_price), 2) AS avg_unit_price,
    ROUND(SUM(net_amount) / NULLIF(SUM(quantity), 0), 2) AS effective_price_per_unit
FROM fact_order_items
WHERE is_cancelled = false
GROUP BY category
ORDER BY category_net_revenue DESC;
