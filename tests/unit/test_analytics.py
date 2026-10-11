"""Unit tests for SQL analytics engine."""

from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from ecommerce_data_lake.analytics import AnalyticsEngine


@pytest.mark.unit
def test_analytics_engine_execution(spark: SparkSession, tmp_path: Path):
    gold_dir = tmp_path / "gold"
    gold_dir.mkdir(parents=True)

    # Write dummy dim_products parquet
    df = spark.createDataFrame([
        ("PROD-1", "Laptop", "Electronics", 999.99, "ACTIVE"),
        ("PROD-2", "Phone", "Electronics", 499.99, "ACTIVE"),
    ], ["product_id", "product_name", "category", "unit_price", "product_status"])
    df.write.parquet(str(gold_dir / "dim_products"))

    engine = AnalyticsEngine(gold_dir=str(gold_dir))
    res = engine.execute_query("SELECT COUNT(*) AS total, AVG(unit_price) AS avg_price FROM dim_products")

    assert res.row_count == 1
    assert res.sample_rows[0]["total"] == 2
    assert round(res.sample_rows[0]["avg_price"], 2) == 749.99
