-- Query 3: Top 10 revenue-generating products with sales volume and category
SELECT 
    product_id,
    product_name,
    category,
    SUM(quantity) AS units_sold,
    ROUND(SUM(net_amount), 2) AS total_net_revenue,
    DENSE_RANK() OVER (ORDER BY SUM(net_amount) DESC) AS revenue_rank
FROM fact_order_items
WHERE is_cancelled = false
GROUP BY product_id, product_name, category
ORDER BY total_net_revenue DESC
LIMIT 10;
