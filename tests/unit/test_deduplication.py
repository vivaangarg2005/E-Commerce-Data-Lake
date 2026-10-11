"""Unit tests for deterministic deduplication."""

import pytest
from pyspark.sql import SparkSession

from ecommerce_data_lake.deduplication import Deduplicator


@pytest.mark.unit
def test_deduplicate_keeps_latest_record(spark: SparkSession):
    data = [
        ("ORD-001", "CUST-001", "PENDING", "2024-01-01 10:00:00", "rec_1"),
        ("ORD-001", "CUST-001", "SHIPPED", "2024-01-01 15:00:00", "rec_2"),  # Latest wins
        ("ORD-002", "CUST-002", "DELIVERED", "2024-01-01 12:00:00", "rec_3"),
    ]
    cols = ["order_id", "customer_id", "order_status", "updated_at", "_source_record_id"]
    df = spark.createDataFrame(data, cols)

    deduped_df, metrics = Deduplicator.deduplicate(df, entity_name="orders", business_key="order_id")

    assert metrics.input_rows == 3
    assert metrics.unique_rows == 2
    assert metrics.duplicate_rows_removed == 1

    ord1 = deduped_df.filter(deduped_df.order_id == "ORD-001").first()
    assert ord1["order_status"] == "SHIPPED"
    assert ord1["updated_at"] == "2024-01-01 15:00:00"


@pytest.mark.unit
def test_deduplicate_is_idempotent(spark: SparkSession):
    data = [
        ("CUST-001", "Alice", "2024-01-01 10:00:00", "rec_1"),
        ("CUST-001", "Alice", "2024-01-01 12:00:00", "rec_2"),
    ]
    cols = ["customer_id", "first_name", "updated_at", "_source_record_id"]
    df = spark.createDataFrame(data, cols)

    # First run
    run1, m1 = Deduplicator.deduplicate(df, "customers")
    # Second run on output
    run2, m2 = Deduplicator.deduplicate(run1, "customers")

    assert run1.count() == 1
    assert run2.count() == 1
    assert m2.duplicate_rows_removed == 0
