"""Unit tests for Silver and Gold dimensional transformations."""

from datetime import date
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession

from ecommerce_data_lake.transformations import GoldTransformer


@pytest.mark.unit
def test_fact_order_items_calculation(spark: SparkSession):
    transformer = GoldTransformer(spark)

    items = spark.createDataFrame([
        ("ITEM-1", "ORD-1", "PROD-1", 2, Decimal("25.00"), Decimal("5.00")),
    ], ["order_item_id", "order_id", "product_id", "quantity", "unit_price", "discount_amount"])

    orders = spark.createDataFrame([
        ("ORD-1", "CUST-1", date(2024, 5, 10), "DELIVERED", "CREDIT_CARD"),
    ], ["order_id", "customer_id", "order_date", "order_status", "payment_method"])

    prods = spark.createDataFrame([
        ("PROD-1", "Product A", "Electronics"),
    ], ["product_id", "product_name", "category"])

    fact_df = transformer.build_fact_order_items(items, orders, prods, None)

    row = fact_df.first()
    assert row["gross_amount"] == Decimal("50.00")
    assert row["discount_amount"] == Decimal("5.00")
    assert row["net_amount"] == Decimal("45.00")
    assert row["is_cancelled"] is False
    assert row["year"] == 2024
    assert row["month"] == 5


@pytest.mark.unit
def test_fact_order_items_no_fanout(spark: SparkSession):
    transformer = GoldTransformer(spark)

    items = spark.createDataFrame([
        ("ITEM-1", "ORD-1", "PROD-1", 1, Decimal("10.00"), Decimal("0.00")),
        ("ITEM-2", "ORD-1", "PROD-2", 2, Decimal("20.00"), Decimal("0.00")),
    ], ["order_item_id", "order_id", "product_id", "quantity", "unit_price", "discount_amount"])

    orders = spark.createDataFrame([
        ("ORD-1", "CUST-1", date(2024, 5, 10), "DELIVERED", "CREDIT_CARD"),
    ], ["order_id", "customer_id", "order_date", "order_status", "payment_method"])

    prods = spark.createDataFrame([
        ("PROD-1", "Product A", "Electronics"),
        ("PROD-2", "Product B", "Electronics"),
    ], ["product_id", "product_name", "category"])

    fact_df = transformer.build_fact_order_items(items, orders, prods, None)

    # Exactly 2 rows output matching the 2 input items
    assert fact_df.count() == 2
