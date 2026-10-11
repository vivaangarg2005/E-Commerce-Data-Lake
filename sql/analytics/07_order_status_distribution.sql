-- Query 7: Breakdown of distinct orders and value by fulfillment status
WITH order_status_totals AS (
    SELECT 
        order_status,
        COUNT(DISTINCT order_id) AS order_count,
        ROUND(SUM(net_amount), 2) AS status_net_value
    FROM fact_order_items
    GROUP BY order_status
)
SELECT 
    order_status,
    order_count,
    ROUND(100.0 * order_count / SUM(order_count) OVER (), 2) AS percentage_of_orders,
    status_net_value,
    ROUND(100.0 * status_net_value / NULLIF(SUM(status_net_value) OVER (), 0), 2) AS percentage_of_value
FROM order_status_totals
ORDER BY order_count DESC;
