"""AWS Glue Data Catalog integration: idempotent database and Parquet table registrations."""

from typing import Any, Dict, List

import boto3
from botocore.exceptions import ClientError

from ecommerce_data_lake.config import AWSStorageConfig
from ecommerce_data_lake.logging_config import setup_logger

logger = setup_logger("ecommerce_data_lake.glue_catalog")


# Parquet Storage Descriptor constants
PARQUET_INPUT_FORMAT = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
PARQUET_OUTPUT_FORMAT = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
PARQUET_SERDE = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"


# Table Schema Definitions for Glue Data Catalog
GLUE_TABLE_DEFINITIONS: Dict[str, Dict[str, Any]] = {
    "fact_order_items": {
        "columns": [
            {"Name": "order_item_id", "Type": "string"},
            {"Name": "order_id", "Type": "string"},
            {"Name": "customer_id", "Type": "string"},
            {"Name": "product_id", "Type": "string"},
            {"Name": "product_name", "Type": "string"},
            {"Name": "category", "Type": "string"},
            {"Name": "order_date", "Type": "date"},
            {"Name": "order_status", "Type": "string"},
            {"Name": "payment_method", "Type": "string"},
            {"Name": "quantity", "Type": "int"},
            {"Name": "unit_price", "Type": "decimal(10,2)"},
            {"Name": "gross_amount", "Type": "decimal(10,2)"},
            {"Name": "discount_amount", "Type": "decimal(10,2)"},
            {"Name": "net_amount", "Type": "decimal(10,2)"},
            {"Name": "is_returned", "Type": "boolean"},
            {"Name": "return_id", "Type": "string"},
            {"Name": "return_date", "Type": "date"},
            {"Name": "return_reason", "Type": "string"},
            {"Name": "refund_amount", "Type": "decimal(10,2)"},
            {"Name": "is_cancelled", "Type": "boolean"},
        ],
        "partition_keys": [
            {"Name": "year", "Type": "int"},
            {"Name": "month", "Type": "int"},
        ],
    },
    "dim_customers": {
        "columns": [
            {"Name": "customer_id", "Type": "string"},
            {"Name": "first_name", "Type": "string"},
            {"Name": "last_name", "Type": "string"},
            {"Name": "email", "Type": "string"},
            {"Name": "city", "Type": "string"},
            {"Name": "state", "Type": "string"},
            {"Name": "signup_date", "Type": "date"},
            {"Name": "customer_status", "Type": "string"},
            {"Name": "first_order_date", "Type": "date"},
            {"Name": "latest_order_date", "Type": "date"},
            {"Name": "total_orders", "Type": "int"},
            {"Name": "lifetime_units_purchased", "Type": "int"},
            {"Name": "lifetime_gross_spend", "Type": "decimal(12,2)"},
            {"Name": "lifetime_net_spend", "Type": "decimal(12,2)"},
            {"Name": "updated_at", "Type": "timestamp"},
        ],
        "partition_keys": [],
    },
    "dim_products": {
        "columns": [
            {"Name": "product_id", "Type": "string"},
            {"Name": "product_name", "Type": "string"},
            {"Name": "category", "Type": "string"},
            {"Name": "unit_price", "Type": "decimal(10,2)"},
            {"Name": "product_status", "Type": "string"},
            {"Name": "total_units_sold", "Type": "int"},
            {"Name": "total_revenue", "Type": "decimal(12,2)"},
            {"Name": "updated_at", "Type": "timestamp"},
        ],
        "partition_keys": [],
    },
    "dim_dates": {
        "columns": [
            {"Name": "date_key", "Type": "int"},
            {"Name": "full_date", "Type": "date"},
            {"Name": "year", "Type": "int"},
            {"Name": "quarter", "Type": "int"},
            {"Name": "month", "Type": "int"},
            {"Name": "month_name", "Type": "string"},
            {"Name": "day", "Type": "int"},
            {"Name": "day_of_week", "Type": "int"},
            {"Name": "day_name", "Type": "string"},
            {"Name": "is_weekend", "Type": "boolean"},
        ],
        "partition_keys": [],
    },
    "agg_daily_sales": {
        "columns": [
            {"Name": "order_date", "Type": "date"},
            {"Name": "category", "Type": "string"},
            {"Name": "order_count", "Type": "int"},
            {"Name": "units_sold", "Type": "int"},
            {"Name": "gross_sales", "Type": "decimal(12,2)"},
            {"Name": "total_discounts", "Type": "decimal(12,2)"},
            {"Name": "net_sales", "Type": "decimal(12,2)"},
            {"Name": "total_refunds", "Type": "decimal(12,2)"},
        ],
        "partition_keys": [
            {"Name": "year", "Type": "int"},
        ],
    },
    "agg_customer_performance": {
        "columns": [
            {"Name": "customer_id", "Type": "string"},
            {"Name": "first_name", "Type": "string"},
            {"Name": "last_name", "Type": "string"},
            {"Name": "email", "Type": "string"},
            {"Name": "state", "Type": "string"},
            {"Name": "first_order_date", "Type": "date"},
            {"Name": "latest_order_date", "Type": "date"},
            {"Name": "total_orders", "Type": "int"},
            {"Name": "lifetime_net_spend", "Type": "decimal(12,2)"},
            {"Name": "customer_tier", "Type": "string"},
        ],
        "partition_keys": [],
    },
}


class GlueCatalogManager:
    """Manages AWS Glue Data Catalog database creation and table schema synchronization."""

    def __init__(self, config: AWSStorageConfig) -> None:
        self.config = config
        self.region = config.region
        self.database_name = config.glue_database
        self.table_prefix = config.glue_table_prefix
        self.s3_bucket = config.s3_bucket
        self.gold_prefix = config.s3_prefixes.get("gold", "ecommerce/gold")
        self._glue_client = None

    @property
    def client(self) -> Any:
        if self._glue_client is None:
            self._glue_client = boto3.client("glue", region_name=self.region)
        return self._glue_client

    def ensure_database(self) -> None:
        """Creates the Glue database if it does not already exist."""
        try:
            self.client.get_database(Name=self.database_name)
            logger.info(f"Glue database '{self.database_name}' already exists.")
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "EntityNotFoundException":
                logger.info(f"Creating Glue database '{self.database_name}'...")
                self.client.create_database(
                    DatabaseInput={
                        "Name": self.database_name,
                        "Description": "E-Commerce Data Lake curated analytics catalog",
                    }
                )
                logger.info(f"Created Glue database '{self.database_name}' successfully.")
            else:
                logger.error(f"Error accessing Glue database: {e}")
                raise

    def register_or_update_table(self, table_name: str) -> None:
        """Idempotently registers or updates a Parquet table definition in the Glue Data Catalog."""
        schema_def = GLUE_TABLE_DEFINITIONS.get(table_name)
        if not schema_def:
            raise ValueError(f"No Glue schema definition registered for table: {table_name}")

        full_table_name = f"{self.table_prefix}{table_name}"
        s3_location = f"s3://{self.s3_bucket}/{self.gold_prefix}/{table_name}/"

        table_input: Dict[str, Any] = {
            "Name": full_table_name,
            "Description": f"Curated Parquet table for {table_name}",
            "TableType": "EXTERNAL_TABLE",
            "Parameters": {
                "classification": "parquet",
                "compressionType": "snappy",
                "EXTERNAL": "TRUE",
            },
            "StorageDescriptor": {
                "Columns": schema_def["columns"],
                "Location": s3_location,
                "InputFormat": PARQUET_INPUT_FORMAT,
                "OutputFormat": PARQUET_OUTPUT_FORMAT,
                "Compressed": True,
                "SerdeInfo": {
                    "SerializationLibrary": PARQUET_SERDE,
                    "Parameters": {"serialization.format": "1"},
                },
            },
            "PartitionKeys": schema_def["partition_keys"],
        }

        try:
            self.client.get_table(DatabaseName=self.database_name, Name=full_table_name)
            logger.info(f"Updating existing Glue table '{full_table_name}'...")
            self.client.update_table(DatabaseName=self.database_name, TableInput=table_input)
            logger.info(f"Updated Glue table '{full_table_name}' successfully.")
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "EntityNotFoundException":
                logger.info(f"Creating Glue table '{full_table_name}' at {s3_location}...")
                self.client.create_table(DatabaseName=self.database_name, TableInput=table_input)
                logger.info(f"Created Glue table '{full_table_name}' successfully.")
            else:
                logger.error(f"Failed registering Glue table {full_table_name}: {e}")
                raise

    def sync_all_tables(self) -> List[str]:
        """Ensures database exists and registers all curated Gold tables."""
        self.ensure_database()
        registered: List[str] = []
        for table_name in GLUE_TABLE_DEFINITIONS.keys():
            self.register_or_update_table(table_name)
            registered.append(f"{self.table_prefix}{table_name}")
        return registered
