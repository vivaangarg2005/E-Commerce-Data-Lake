-- Query 4: Daily sales trends showing order volume, units sold, and net sales
SELECT 
    order_date,
    SUM(order_count) AS total_orders,
    SUM(units_sold) AS total_units_sold,
    ROUND(SUM(gross_sales), 2) AS total_gross_sales,
    ROUND(SUM(total_discounts), 2) AS total_discounts,
    ROUND(SUM(net_sales), 2) AS total_net_sales,
    ROUND(SUM(total_refunds), 2) AS total_refunds
FROM agg_daily_sales
GROUP BY order_date
ORDER BY order_date ASC;
