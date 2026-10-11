"""Schema definitions, types, domains, and data contracts for all data lake entities."""

from typing import Dict, List, Set

from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ==============================================================================
# ALLOWED DOMAIN VALUES & ENUMS
# ==============================================================================
ALLOWED_CUSTOMER_STATUSES: Set[str] = {"ACTIVE", "INACTIVE", "SUSPENDED"}
ALLOWED_PRODUCT_STATUSES: Set[str] = {"ACTIVE", "DISCONTINUED", "OUT_OF_STOCK"}
ALLOWED_ORDER_STATUSES: Set[str] = {
    "PENDING",
    "PROCESSING",
    "SHIPPED",
    "DELIVERED",
    "CANCELLED",
    "RETURNED",
}
ALLOWED_PAYMENT_METHODS: Set[str] = {
    "CREDIT_CARD",
    "DEBIT_CARD",
    "PAYPAL",
    "APPLE_PAY",
    "BANK_TRANSFER",
}
ALLOWED_RETURN_STATUSES: Set[str] = {"REQUESTED", "APPROVED", "COMPLETED", "REJECTED"}
ALLOWED_RETURN_REASONS: Set[str] = {
    "DEFECTIVE",
    "WRONG_ITEM",
    "NOT_AS_DESCRIBED",
    "CHANGED_MIND",
    "LATE_DELIVERY",
}
VALID_CATEGORIES: List[str] = [
    "Electronics",
    "Clothing",
    "Home & Kitchen",
    "Books",
    "Beauty & Health",
    "Sports & Outdoors",
    "Toys & Games",
]

# Ingestion Metadata Column Names
METADATA_COLS: List[str] = [
    "_ingestion_timestamp",
    "_source_filename",
    "_batch_id",
    "_source_record_id",
]

# Primary and Business Keys per Entity
PRIMARY_KEYS: Dict[str, str] = {
    "customers": "customer_id",
    "products": "product_id",
    "orders": "order_id",
    "order_items": "order_item_id",
    "returns": "return_id",
}

# ==============================================================================
# RAW / INGESTION SCHEMAS (String-based permissive parsing for Bronze)
# ==============================================================================
RAW_CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", StringType(), nullable=False),
    StructField("first_name", StringType(), nullable=True),
    StructField("last_name", StringType(), nullable=True),
    StructField("email", StringType(), nullable=True),
    StructField("city", StringType(), nullable=True),
    StructField("state", StringType(), nullable=True),
    StructField("signup_date", StringType(), nullable=True),
    StructField("customer_status", StringType(), nullable=True),
    StructField("updated_at", StringType(), nullable=True),
])

RAW_PRODUCTS_SCHEMA = StructType([
    StructField("product_id", StringType(), nullable=False),
    StructField("product_name", StringType(), nullable=True),
    StructField("category", StringType(), nullable=True),
    StructField("unit_price", StringType(), nullable=True),
    StructField("product_status", StringType(), nullable=True),
    StructField("updated_at", StringType(), nullable=True),
])

RAW_ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType(), nullable=False),
    StructField("customer_id", StringType(), nullable=True),
    StructField("order_date", StringType(), nullable=True),
    StructField("order_status", StringType(), nullable=True),
    StructField("payment_method", StringType(), nullable=True),
    StructField("updated_at", StringType(), nullable=True),
])

RAW_ORDER_ITEMS_SCHEMA = StructType([
    StructField("order_item_id", StringType(), nullable=False),
    StructField("order_id", StringType(), nullable=True),
    StructField("product_id", StringType(), nullable=True),
    StructField("quantity", StringType(), nullable=True),
    StructField("unit_price", StringType(), nullable=True),
    StructField("discount_amount", StringType(), nullable=True),
    StructField("updated_at", StringType(), nullable=True),
])

RAW_RETURNS_SCHEMA = StructType([
    StructField("return_id", StringType(), nullable=False),
    StructField("order_item_id", StringType(), nullable=True),
    StructField("return_date", StringType(), nullable=True),
    StructField("return_reason", StringType(), nullable=True),
    StructField("refund_amount", StringType(), nullable=True),
    StructField("return_status", StringType(), nullable=True),
    StructField("updated_at", StringType(), nullable=True),
])

RAW_SCHEMAS: Dict[str, StructType] = {
    "customers": RAW_CUSTOMERS_SCHEMA,
    "products": RAW_PRODUCTS_SCHEMA,
    "orders": RAW_ORDERS_SCHEMA,
    "order_items": RAW_ORDER_ITEMS_SCHEMA,
    "returns": RAW_RETURNS_SCHEMA,
}

# ==============================================================================
# SILVER SCHEMAS (Strongly typed, cleaned, standardized)
# ==============================================================================
SILVER_CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", StringType(), nullable=False),
    StructField("first_name", StringType(), nullable=False),
    StructField("last_name", StringType(), nullable=False),
    StructField("email", StringType(), nullable=False),
    StructField("city", StringType(), nullable=False),
    StructField("state", StringType(), nullable=False),
    StructField("signup_date", DateType(), nullable=False),
    StructField("customer_status", StringType(), nullable=False),
    StructField("updated_at", TimestampType(), nullable=False),
    StructField("_ingestion_timestamp", TimestampType(), nullable=False),
    StructField("_source_filename", StringType(), nullable=False),
    StructField("_batch_id", StringType(), nullable=False),
    StructField("_source_record_id", StringType(), nullable=False),
])

SILVER_PRODUCTS_SCHEMA = StructType([
    StructField("product_id", StringType(), nullable=False),
    StructField("product_name", StringType(), nullable=False),
    StructField("category", StringType(), nullable=False),
    StructField("unit_price", DecimalType(10, 2), nullable=False),
    StructField("product_status", StringType(), nullable=False),
    StructField("updated_at", TimestampType(), nullable=False),
    StructField("_ingestion_timestamp", TimestampType(), nullable=False),
    StructField("_source_filename", StringType(), nullable=False),
    StructField("_batch_id", StringType(), nullable=False),
    StructField("_source_record_id", StringType(), nullable=False),
])

SILVER_ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType(), nullable=False),
    StructField("customer_id", StringType(), nullable=False),
    StructField("order_date", DateType(), nullable=False),
    StructField("order_status", StringType(), nullable=False),
    StructField("payment_method", StringType(), nullable=False),
    StructField("updated_at", TimestampType(), nullable=False),
    StructField("_ingestion_timestamp", TimestampType(), nullable=False),
    StructField("_source_filename", StringType(), nullable=False),
    StructField("_batch_id", StringType(), nullable=False),
    StructField("_source_record_id", StringType(), nullable=False),
])

SILVER_ORDER_ITEMS_SCHEMA = StructType([
    StructField("order_item_id", StringType(), nullable=False),
    StructField("order_id", StringType(), nullable=False),
    StructField("product_id", StringType(), nullable=False),
    StructField("quantity", IntegerType(), nullable=False),
    StructField("unit_price", DecimalType(10, 2), nullable=False),
    StructField("discount_amount", DecimalType(10, 2), nullable=False),
    StructField("updated_at", TimestampType(), nullable=False),
    StructField("_ingestion_timestamp", TimestampType(), nullable=False),
    StructField("_source_filename", StringType(), nullable=False),
    StructField("_batch_id", StringType(), nullable=False),
    StructField("_source_record_id", StringType(), nullable=False),
])

SILVER_RETURNS_SCHEMA = StructType([
    StructField("return_id", StringType(), nullable=False),
    StructField("order_item_id", StringType(), nullable=False),
    StructField("return_date", DateType(), nullable=False),
    StructField("return_reason", StringType(), nullable=False),
    StructField("refund_amount", DecimalType(10, 2), nullable=False),
    StructField("return_status", StringType(), nullable=False),
    StructField("updated_at", TimestampType(), nullable=False),
    StructField("_ingestion_timestamp", TimestampType(), nullable=False),
    StructField("_source_filename", StringType(), nullable=False),
    StructField("_batch_id", StringType(), nullable=False),
    StructField("_source_record_id", StringType(), nullable=False),
])

SILVER_SCHEMAS: Dict[str, StructType] = {
    "customers": SILVER_CUSTOMERS_SCHEMA,
    "products": SILVER_PRODUCTS_SCHEMA,
    "orders": SILVER_ORDERS_SCHEMA,
    "order_items": SILVER_ORDER_ITEMS_SCHEMA,
    "returns": SILVER_RETURNS_SCHEMA,
}

# ==============================================================================
# QUARANTINE SCHEMA (Standard structure for invalid/rejected records)
# ==============================================================================
QUARANTINE_SCHEMA = StructType([
    StructField("quarantine_id", StringType(), nullable=False),
    StructField("entity_name", StringType(), nullable=False),
    StructField("source_record_id", StringType(), nullable=True),
    StructField("batch_id", StringType(), nullable=False),
    StructField("source_filename", StringType(), nullable=True),
    StructField("rejection_stage", StringType(), nullable=False),  # INGESTION, VALIDATION, INTEGRITY
    StructField("rule_code", StringType(), nullable=False),
    StructField("rejection_reason", StringType(), nullable=False),
    StructField("raw_record_json", StringType(), nullable=False),
    StructField("quarantine_timestamp", TimestampType(), nullable=False),
])
