-- Query 5: Customer purchase frequency bucket distribution
WITH customer_order_counts AS (
    SELECT 
        customer_id,
        total_orders,
        lifetime_net_spend
    FROM dim_customers
    WHERE total_orders > 0
),
frequency_buckets AS (
    SELECT 
        customer_id,
        lifetime_net_spend,
        CASE 
            WHEN total_orders = 1 THEN '1 Order (One-Time)'
            WHEN total_orders BETWEEN 2 AND 4 THEN '2-4 Orders (Repeat)'
            WHEN total_orders BETWEEN 5 AND 9 THEN '5-9 Orders (Loyal)'
            ELSE '10+ Orders (Power Buyer)'
        END AS frequency_segment
    FROM customer_order_counts
)
SELECT 
    frequency_segment,
    COUNT(customer_id) AS customer_count,
    ROUND(SUM(lifetime_net_spend), 2) AS total_segment_spend,
    ROUND(AVG(lifetime_net_spend), 2) AS avg_spend_per_customer
FROM frequency_buckets
GROUP BY frequency_segment
ORDER BY customer_count DESC;
