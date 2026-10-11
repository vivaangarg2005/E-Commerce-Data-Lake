"""Unit tests for schemas and domain definitions."""

import pytest

from ecommerce_data_lake.schemas import (
    ALLOWED_CUSTOMER_STATUSES,
    ALLOWED_ORDER_STATUSES,
    ALLOWED_PAYMENT_METHODS,
    ALLOWED_PRODUCT_STATUSES,
    PRIMARY_KEYS,
    RAW_SCHEMAS,
    SILVER_SCHEMAS,
)


@pytest.mark.unit
def test_all_entities_have_registered_schemas():
    entities = ["customers", "products", "orders", "order_items", "returns"]
    for entity in entities:
        assert entity in RAW_SCHEMAS, f"Missing RAW schema for {entity}"
        assert entity in SILVER_SCHEMAS, f"Missing SILVER schema for {entity}"
        assert entity in PRIMARY_KEYS, f"Missing primary key for {entity}"


@pytest.mark.unit
def test_allowed_domains_contain_expected_values():
    assert "ACTIVE" in ALLOWED_CUSTOMER_STATUSES
    assert "SUSPENDED" in ALLOWED_CUSTOMER_STATUSES
    assert "DELIVERED" in ALLOWED_ORDER_STATUSES
    assert "CANCELLED" in ALLOWED_ORDER_STATUSES
    assert "CREDIT_CARD" in ALLOWED_PAYMENT_METHODS
    assert "ACTIVE" in ALLOWED_PRODUCT_STATUSES
