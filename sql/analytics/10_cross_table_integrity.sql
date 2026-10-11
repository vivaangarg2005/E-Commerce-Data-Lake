-- Query 10: Cross-table reconciliation verifying mathematical harmony across layers:
-- Compares sum of Fact Order Items net revenue against Dim Customers net revenue and Agg Daily Sales net revenue
WITH fact_totals AS (
    SELECT 
        ROUND(SUM(net_amount), 2) AS fact_net_sales,
        COUNT(DISTINCT order_id) AS fact_distinct_orders
    FROM fact_order_items
    WHERE is_cancelled = false
),
dim_cust_totals AS (
    SELECT 
        ROUND(SUM(lifetime_net_spend), 2) AS dim_cust_net_sales,
        SUM(total_orders) AS dim_cust_total_orders
    FROM dim_customers
),
daily_agg_totals AS (
    SELECT 
        ROUND(SUM(net_sales), 2) AS agg_net_sales,
        SUM(order_count) AS agg_order_count
    FROM agg_daily_sales
)
SELECT 
    f.fact_net_sales,
    c.dim_cust_net_sales,
    d.agg_net_sales,
    ROUND(f.fact_net_sales - c.dim_cust_net_sales, 2) AS fact_vs_cust_discrepancy,
    ROUND(f.fact_net_sales - d.agg_net_sales, 2) AS fact_vs_daily_discrepancy,
    CASE 
        WHEN ABS(f.fact_net_sales - c.dim_cust_net_sales) < 0.01 
         AND ABS(f.fact_net_sales - d.agg_net_sales) < 0.01 
        THEN 'RECONCILED'
        ELSE 'DISCREPANCY_DETECTED'
    END AS reconciliation_status
FROM fact_totals f
CROSS JOIN dim_cust_totals c
CROSS JOIN daily_agg_totals d;
