"""Unit tests for validation engine and quarantine routing."""

import pytest
from pyspark.sql import SparkSession

from ecommerce_data_lake.validation import DataValidator


@pytest.mark.unit
def test_validate_customers_quarantines_invalid_email(spark: SparkSession):
    validator = DataValidator(spark)
    data = [
        ("CUST-001", "Alice", "Smith", "alice@example.com", "NYC", "NY", "2024-01-01", "ACTIVE", "2024-01-01 00:00:00", "2024-01-01 00:00:00", "test.csv", "b1", "rec1"),
        ("CUST-002", "Bob", "Jones", "invalid-email-string", "LA", "CA", "2024-01-01", "ACTIVE", "2024-01-01 00:00:00", "2024-01-01 00:00:00", "test.csv", "b1", "rec2"),
    ]
    cols = [
        "customer_id", "first_name", "last_name", "email", "city", "state",
        "signup_date", "customer_status", "updated_at", "_ingestion_timestamp",
        "_source_filename", "_batch_id", "_source_record_id",
    ]
    df = spark.createDataFrame(data, cols)

    clean_df, q_df, summary = validator.validate_customers(df)

    assert clean_df.count() == 1
    assert clean_df.first()["customer_id"] == "CUST-001"
    assert q_df.count() == 1
    assert q_df.first()["source_record_id"] == "CUST-002"
    assert q_df.first()["rule_code"] == "CUST_INVALID_EMAIL"


@pytest.mark.unit
def test_validate_order_items_quarantines_negative_qty(spark: SparkSession):
    validator = DataValidator(spark)

    # Dummy valid orders and products
    orders_df = spark.createDataFrame([("ORD-001",)], ["order_id"])
    products_df = spark.createDataFrame([("PROD-001",)], ["product_id"])

    items_data = [
        ("ITEM-001", "ORD-001", "PROD-001", "2", "50.00", "5.00", "2024-01-01 00:00:00", "2024-01-01 00:00:00", "items.csv", "b1", "rec1"),
        ("ITEM-002", "ORD-001", "PROD-001", "-3", "50.00", "0.00", "2024-01-01 00:00:00", "2024-01-01 00:00:00", "items.csv", "b1", "rec2"),
    ]
    cols = [
        "order_item_id", "order_id", "product_id", "quantity", "unit_price", "discount_amount",
        "updated_at", "_ingestion_timestamp", "_source_filename", "_batch_id", "_source_record_id",
    ]
    df = spark.createDataFrame(items_data, cols)

    clean_df, q_df, summary = validator.validate_order_items(df, orders_df, products_df)

    assert clean_df.count() == 1
    assert clean_df.first()["order_item_id"] == "ITEM-001"
    assert q_df.count() == 1
    assert q_df.first()["rule_code"] == "ITEM_INVALID_QTY"


@pytest.mark.unit
def test_validate_orders_quarantines_orphan_customer(spark: SparkSession):
    validator = DataValidator(spark)
    valid_customers_df = spark.createDataFrame([("CUST-001",)], ["customer_id"])

    orders_data = [
        ("ORD-001", "CUST-001", "2024-01-01", "DELIVERED", "CREDIT_CARD", "2024-01-01 00:00:00", "2024-01-01 00:00:00", "orders.csv", "b1", "rec1"),
        ("ORD-002", "CUST-NONEXISTENT", "2024-01-01", "DELIVERED", "CREDIT_CARD", "2024-01-01 00:00:00", "2024-01-01 00:00:00", "orders.csv", "b1", "rec2"),
    ]
    cols = [
        "order_id", "customer_id", "order_date", "order_status", "payment_method",
        "updated_at", "_ingestion_timestamp", "_source_filename", "_batch_id", "_source_record_id",
    ]
    df = spark.createDataFrame(orders_data, cols)

    clean_df, q_df, summary = validator.validate_orders(df, valid_customers_df)

    assert clean_df.count() == 1
    assert clean_df.first()["order_id"] == "ORD-001"
    assert q_df.count() == 1
    assert q_df.first()["rule_code"] == "ORD_ORPHAN_CUSTOMER"
