-- Query 8: Product return volume, refund amounts, and return rate by category
SELECT 
    category,
    COUNT(order_item_id) AS total_items_sold,
    SUM(CASE WHEN is_returned = true THEN 1 ELSE 0 END) AS returned_items_count,
    ROUND(100.0 * SUM(CASE WHEN is_returned = true THEN 1 ELSE 0 END) / NULLIF(COUNT(order_item_id), 0), 2) AS return_rate_pct,
    ROUND(SUM(refund_amount), 2) AS total_refunded_amount,
    ROUND(SUM(refund_amount) / NULLIF(SUM(gross_amount), 0) * 100.0, 2) AS refund_to_gross_pct
FROM fact_order_items
WHERE is_cancelled = false
GROUP BY category
ORDER BY return_rate_pct DESC;
