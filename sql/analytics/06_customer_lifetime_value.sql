-- Query 6: Top 15 customers by lifetime net spending with purchase activity
SELECT 
    c.customer_id,
    CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
    c.email,
    c.city,
    c.state,
    c.signup_date,
    c.total_orders,
    c.lifetime_units_purchased,
    c.lifetime_net_spend,
    p.customer_tier
FROM dim_customers c
LEFT JOIN agg_customer_performance p ON c.customer_id = p.customer_id
WHERE c.total_orders > 0
ORDER BY c.lifetime_net_spend DESC
LIMIT 15;
