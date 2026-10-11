"""Integration tests for the local pipeline execution."""

from pathlib import Path

import pytest

from ecommerce_data_lake.config import PipelineConfig
from ecommerce_data_lake.data_generation import SyntheticDataGenerator
from ecommerce_data_lake.pipeline import EcommercePipeline


@pytest.mark.integration
def test_full_local_pipeline_run(test_config: PipelineConfig):
    # 1. Generate small test dataset
    generator = SyntheticDataGenerator(seed=123)
    profile = test_config.generator.profiles["dev"]
    profile.orders = 50
    profile.customers = 20
    profile.products = 10

    batch_data = generator.generate_batch(profile, batch_id="batch_test")
    generator.write_batch_to_csv(batch_data, Path(test_config.local.raw_dir), "batch_test")

    # 2. Run Pipeline
    pipeline = EcommercePipeline(config=test_config)
    report = pipeline.run_pipeline(batch_id="batch_test")

    # 3. Assertions
    assert report.status in ("SUCCESS", "SUCCESS_WITH_WARNINGS")
    assert report.total_rows_read > 0
    assert report.total_silver_rows > 0
    assert report.data_quality_score > 90.0

    # Verify Parquet outputs exist on disk
    gold_fact = Path(test_config.local.gold_dir) / "fact_order_items"
    assert gold_fact.exists()
    assert any(gold_fact.rglob("*.parquet"))

    dim_cust = Path(test_config.local.gold_dir) / "dim_customers"
    assert dim_cust.exists()
    assert any(dim_cust.rglob("*.parquet"))

    # 4. Rerun the same batch (Test Idempotency)
    report_rerun = pipeline.run_pipeline(batch_id="batch_test")
    assert report_rerun.status in ("SUCCESS", "SUCCESS_WITH_WARNINGS")
