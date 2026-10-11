"""Unit tests for AWS Glue Data Catalog definitions."""

import pytest

from ecommerce_data_lake.glue_catalog import GLUE_TABLE_DEFINITIONS


@pytest.mark.aws
def test_glue_table_definitions_match_gold_tables():
    expected_tables = {
        "fact_order_items",
        "dim_customers",
        "dim_products",
        "dim_dates",
        "agg_daily_sales",
        "agg_customer_performance",
    }
    assert set(GLUE_TABLE_DEFINITIONS.keys()) == expected_tables

    # Check fact table partitioning
    fact_def = GLUE_TABLE_DEFINITIONS["fact_order_items"]
    partition_keys = [pk["Name"] for pk in fact_def["partition_keys"]]
    assert partition_keys == ["year", "month"]
