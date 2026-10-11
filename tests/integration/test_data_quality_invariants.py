"""Integration tests for Data Quality Invariants and financial reconciliations."""

from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from ecommerce_data_lake.analytics import AnalyticsEngine
from ecommerce_data_lake.config import PipelineConfig
from ecommerce_data_lake.data_generation import SyntheticDataGenerator
from ecommerce_data_lake.pipeline import EcommercePipeline


@pytest.mark.integration
def test_financial_reconciliation_and_invariants(test_config: PipelineConfig, spark: SparkSession):
    # 1. Generate & Run Pipeline
    generator = SyntheticDataGenerator(seed=999)
    profile = test_config.generator.profiles["dev"]
    profile.orders = 60
    profile.customers = 25
    profile.products = 12

    batch_data = generator.generate_batch(profile, batch_id="batch_invariants")
    generator.write_batch_to_csv(batch_data, Path(test_config.local.raw_dir), "batch_invariants")

    pipeline = EcommercePipeline(config=test_config)
    report = pipeline.run_pipeline(batch_id="batch_invariants")
    assert report.status in ("SUCCESS", "SUCCESS_WITH_WARNINGS")

    # 2. Run Query 10 (Cross Table Integrity)
    analytics = AnalyticsEngine(
        gold_dir=test_config.local.gold_dir,
        quarantine_dir=test_config.local.quarantine_dir,
    )

    query_10_path = Path(__file__).resolve().parent.parent.parent / "sql" / "analytics" / "10_cross_table_integrity.sql"
    result = analytics.execute_sql_file(str(query_10_path))

    assert result.row_count == 1
    reconciliation = result.sample_rows[0]

    # Verify 100% exact reconciliation between fact, customer dim, and daily aggregates
    assert reconciliation["reconciliation_status"] == "RECONCILED"
    assert reconciliation["fact_vs_cust_discrepancy"] == 0.0
    assert reconciliation["fact_vs_daily_discrepancy"] == 0.0
