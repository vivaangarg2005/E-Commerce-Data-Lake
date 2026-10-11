-- =============================================================================
-- Amazon Athena External Table DDL Definitions (v3 Engine Compatible)
-- Database: ecommerce_data_lake
-- =============================================================================

CREATE DATABASE IF NOT EXISTS ecommerce_data_lake
COMMENT 'E-Commerce Data Lake Curated Analytics Catalog';

-- 1. Fact Order Items (Grain: 1 row per order item, partitioned by year and month)
CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce_data_lake.fact_order_items (
    order_item_id STRING,
    order_id STRING,
    customer_id STRING,
    product_id STRING,
    product_name STRING,
    category STRING,
    order_date DATE,
    order_status STRING,
    payment_method STRING,
    quantity INT,
    unit_price DECIMAL(10, 2),
    gross_amount DECIMAL(10, 2),
    discount_amount DECIMAL(10, 2),
    net_amount DECIMAL(10, 2),
    is_returned BOOLEAN,
    return_id STRING,
    return_date DATE,
    return_reason STRING,
    refund_amount DECIMAL(10, 2),
    is_cancelled BOOLEAN
)
PARTITIONED BY (
    year INT,
    month INT
)
STORED AS PARQUET
LOCATION 's3://YOUR_BUCKET/ecommerce/gold/fact_order_items/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- 2. Customer Dimension
CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce_data_lake.dim_customers (
    customer_id STRING,
    first_name STRING,
    last_name STRING,
    email STRING,
    city STRING,
    state STRING,
    signup_date DATE,
    customer_status STRING,
    first_order_date DATE,
    latest_order_date DATE,
    total_orders INT,
    lifetime_units_purchased INT,
    lifetime_gross_spend DECIMAL(12, 2),
    lifetime_net_spend DECIMAL(12, 2),
    updated_at TIMESTAMP
)
STORED AS PARQUET
LOCATION 's3://YOUR_BUCKET/ecommerce/gold/dim_customers/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- 3. Product Dimension
CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce_data_lake.dim_products (
    product_id STRING,
    product_name STRING,
    category STRING,
    unit_price DECIMAL(10, 2),
    product_status STRING,
    total_units_sold INT,
    total_revenue DECIMAL(12, 2),
    updated_at TIMESTAMP
)
STORED AS PARQUET
LOCATION 's3://YOUR_BUCKET/ecommerce/gold/dim_products/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- 4. Date Dimension
CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce_data_lake.dim_dates (
    date_key INT,
    full_date DATE,
    year INT,
    quarter INT,
    month INT,
    month_name STRING,
    day INT,
    day_of_week INT,
    day_name STRING,
    is_weekend BOOLEAN
)
STORED AS PARQUET
LOCATION 's3://YOUR_BUCKET/ecommerce/gold/dim_dates/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- 5. Aggregate Daily Sales (Partitioned by year)
CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce_data_lake.agg_daily_sales (
    order_date DATE,
    category STRING,
    order_count INT,
    units_sold INT,
    gross_sales DECIMAL(12, 2),
    total_discounts DECIMAL(12, 2),
    net_sales DECIMAL(12, 2),
    total_refunds DECIMAL(12, 2)
)
PARTITIONED BY (
    year INT
)
STORED AS PARQUET
LOCATION 's3://YOUR_BUCKET/ecommerce/gold/agg_daily_sales/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- 6. Aggregate Customer Performance Mart
CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce_data_lake.agg_customer_performance (
    customer_id STRING,
    first_name STRING,
    last_name STRING,
    email STRING,
    state STRING,
    first_order_date DATE,
    latest_order_date DATE,
    total_orders INT,
    lifetime_net_spend DECIMAL(12, 2),
    customer_tier STRING
)
STORED AS PARQUET
LOCATION 's3://YOUR_BUCKET/ecommerce/gold/agg_customer_performance/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- 7. Quarantine Exceptions
CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce_data_lake.quarantine_exceptions (
    quarantine_id STRING,
    entity_name STRING,
    source_record_id STRING,
    batch_id STRING,
    source_filename STRING,
    rejection_stage STRING,
    rule_code STRING,
    rejection_reason STRING,
    raw_record_json STRING,
    quarantine_timestamp TIMESTAMP
)
STORED AS PARQUET
LOCATION 's3://YOUR_BUCKET/ecommerce/quarantine/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');
