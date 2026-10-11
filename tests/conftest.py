"""Pytest fixtures for E-Commerce Data Lake test suite."""

import shutil
import tempfile
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from ecommerce_data_lake.config import LocalStorageConfig, PipelineConfig
from ecommerce_data_lake.storage import get_spark_session


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Session-scoped Apache Spark session for testing."""
    return get_spark_session()


@pytest.fixture
def temp_lake_dir():
    """Creates a temporary workspace directory for local test runs."""
    tmp = tempfile.mkdtemp(prefix="test_datalake_")
    yield Path(tmp)
    shutil.rmtree(tmp, ignore_errors=True)


@pytest.fixture
def test_config(temp_lake_dir: Path) -> PipelineConfig:
    """Configures a temporary PipelineConfig pointing to isolated test directories."""
    cfg = PipelineConfig.load()
    cfg.local = LocalStorageConfig(
        base_dir=str(temp_lake_dir),
        raw_dir=str(temp_lake_dir / "raw"),
        bronze_dir=str(temp_lake_dir / "bronze"),
        silver_dir=str(temp_lake_dir / "silver"),
        gold_dir=str(temp_lake_dir / "gold"),
        quarantine_dir=str(temp_lake_dir / "quarantine"),
        audit_dir=str(temp_lake_dir / "audit"),
        state_dir=str(temp_lake_dir / "state"),
    )
    return cfg
