"""Configuration management for the E-Commerce Data Lake pipeline."""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml


@dataclass
class LocalStorageConfig:
    base_dir: str = "./data"
    raw_dir: str = "./data/raw"
    bronze_dir: str = "./data/bronze"
    silver_dir: str = "./data/silver"
    gold_dir: str = "./data/gold"
    quarantine_dir: str = "./data/quarantine"
    audit_dir: str = "./data/audit"
    state_dir: str = "./data/state"


@dataclass
class AWSStorageConfig:
    region: str = "us-east-1"
    s3_bucket: str = ""
    s3_base_prefix: str = "ecommerce"
    s3_prefixes: Dict[str, str] = field(default_factory=lambda: {
        "raw": "ecommerce/raw",
        "bronze": "ecommerce/bronze",
        "silver": "ecommerce/silver",
        "gold": "ecommerce/gold",
        "quarantine": "ecommerce/quarantine",
        "audit": "ecommerce/audit",
    })
    glue_database: str = "ecommerce_data_lake"
    glue_table_prefix: str = "ecommerce_"
    athena_workgroup: str = "primary"
    athena_output_location: str = ""


@dataclass
class SparkConfig:
    app_name: str = "EcommerceDataLake"
    master: str = "local[*]"
    driver_memory: str = "2g"
    shuffle_partitions: int = 4
    log_level: str = "WARN"


@dataclass
class DatasetProfile:
    customers: int = 100
    products: int = 25
    orders: int = 250
    order_items_multiplier: float = 2.5
    returns_rate: float = 0.10
    duplicate_rate: float = 0.05
    anomaly_rate: float = 0.03


@dataclass
class GeneratorConfig:
    seed: int = 42
    profiles: Dict[str, DatasetProfile] = field(default_factory=lambda: {
        "dev": DatasetProfile(100, 25, 250, 2.5, 0.10, 0.05, 0.03),
        "standard": DatasetProfile(1000, 100, 3000, 3.0, 0.12, 0.04, 0.02),
        "large": DatasetProfile(10000, 500, 25000, 3.5, 0.12, 0.03, 0.01),
    })


@dataclass
class StorageFormatConfig:
    format: str = "parquet"
    compression: str = "snappy"
    partitions: Dict[str, List[str]] = field(default_factory=lambda: {
        "fact_order_items": ["year", "month"],
        "agg_daily_sales": ["year"],
        "dim_customers": [],
        "dim_products": [],
        "dim_dates": [],
        "agg_customer_performance": [],
    })


@dataclass
class LoggingConfig:
    level: str = "INFO"
    console: bool = True
    file: bool = True
    log_dir: str = "./logs"


@dataclass
class PipelineConfig:
    environment: str = "local"  # 'local' or 'aws'
    local: LocalStorageConfig = field(default_factory=LocalStorageConfig)
    aws: AWSStorageConfig = field(default_factory=AWSStorageConfig)
    spark: SparkConfig = field(default_factory=SparkConfig)
    generator: GeneratorConfig = field(default_factory=GeneratorConfig)
    storage: StorageFormatConfig = field(default_factory=StorageFormatConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)

    @classmethod
    def load(cls, config_path: Optional[str] = None) -> "PipelineConfig":
        """Loads configuration from YAML file and applies environment variable overrides."""
        cfg_dict: Dict[str, Any] = {}

        # Search default config paths if none provided
        search_paths = [
            config_path,
            os.environ.get("CONFIG_PATH"),
            "./config/config.yaml",
            "./config/config.example.yaml",
        ]

        found_path: Optional[Path] = None
        for p in search_paths:
            if p and Path(p).is_file():
                found_path = Path(p)
                break

        if found_path:
            with open(found_path, "r", encoding="utf-8") as f:
                loaded = yaml.safe_load(f)
                if isinstance(loaded, dict):
                    cfg_dict = loaded

        # Extract environment (env var override takes precedence)
        env = os.environ.get("PIPELINE_ENV", cfg_dict.get("environment", "local")).lower()

        # Parse local storage
        local_raw = cfg_dict.get("local", {})
        local_cfg = LocalStorageConfig(
            base_dir=os.environ.get("LOCAL_DATA_DIR", local_raw.get("base_dir", "./data")),
            raw_dir=local_raw.get("raw_dir", "./data/raw"),
            bronze_dir=local_raw.get("bronze_dir", "./data/bronze"),
            silver_dir=local_raw.get("silver_dir", "./data/silver"),
            gold_dir=local_raw.get("gold_dir", "./data/gold"),
            quarantine_dir=local_raw.get("quarantine_dir", "./data/quarantine"),
            audit_dir=local_raw.get("audit_dir", "./data/audit"),
            state_dir=os.environ.get("LOCAL_STATE_DIR", local_raw.get("state_dir", "./data/state")),
        )

        # Parse AWS storage
        aws_raw = cfg_dict.get("aws", {})
        aws_cfg = AWSStorageConfig(
            region=os.environ.get("AWS_REGION", aws_raw.get("region", "us-east-1")),
            s3_bucket=os.environ.get("AWS_S3_BUCKET", aws_raw.get("s3_bucket", "")),
            s3_base_prefix=os.environ.get("AWS_S3_PREFIX", aws_raw.get("s3_base_prefix", "ecommerce")),
            s3_prefixes=aws_raw.get("s3_prefixes", {
                "raw": "ecommerce/raw",
                "bronze": "ecommerce/bronze",
                "silver": "ecommerce/silver",
                "gold": "ecommerce/gold",
                "quarantine": "ecommerce/quarantine",
                "audit": "ecommerce/audit",
            }),
            glue_database=os.environ.get("AWS_GLUE_DATABASE", aws_raw.get("glue_database", "ecommerce_data_lake")),
            glue_table_prefix=aws_raw.get("glue_table_prefix", "ecommerce_"),
            athena_workgroup=aws_raw.get("athena_workgroup", "primary"),
            athena_output_location=os.environ.get("AWS_ATHENA_OUTPUT_LOCATION", aws_raw.get("athena_output_location", "")),
        )

        # Parse Spark config
        spark_raw = cfg_dict.get("spark", {})
        spark_cfg = SparkConfig(
            app_name=os.environ.get("SPARK_APP_NAME", spark_raw.get("app_name", "EcommerceDataLake")),
            master=os.environ.get("SPARK_MASTER", spark_raw.get("master", "local[*]")),
            driver_memory=os.environ.get("SPARK_DRIVER_MEMORY", spark_raw.get("driver_memory", "2g")),
            shuffle_partitions=spark_raw.get("shuffle_partitions", 4),
            log_level=spark_raw.get("log_level", "WARN"),
        )

        # Parse Generator profiles
        gen_raw = cfg_dict.get("generator", {})
        profiles_raw = gen_raw.get("profiles", {})
        profiles: Dict[str, DatasetProfile] = {}
        for p_name, p_data in profiles_raw.items():
            profiles[p_name] = DatasetProfile(
                customers=p_data.get("customers", 100),
                products=p_data.get("products", 25),
                orders=p_data.get("orders", 250),
                order_items_multiplier=p_data.get("order_items_multiplier", 2.5),
                returns_rate=p_data.get("returns_rate", 0.10),
                duplicate_rate=p_data.get("duplicate_rate", 0.05),
                anomaly_rate=p_data.get("anomaly_rate", 0.03),
            )
        if not profiles:
            profiles = {
                "dev": DatasetProfile(100, 25, 250, 2.5, 0.10, 0.05, 0.03),
                "standard": DatasetProfile(1000, 100, 3000, 3.0, 0.12, 0.04, 0.02),
            }

        gen_cfg = GeneratorConfig(
            seed=int(os.environ.get("SEED", gen_raw.get("seed", 42))),
            profiles=profiles,
        )

        # Parse Storage format
        storage_raw = cfg_dict.get("storage", {})
        storage_cfg = StorageFormatConfig(
            format=storage_raw.get("format", "parquet"),
            compression=storage_raw.get("compression", "snappy"),
            partitions=storage_raw.get("partitions", {
                "fact_order_items": ["year", "month"],
                "agg_daily_sales": ["year"],
                "dim_customers": [],
                "dim_products": [],
                "dim_dates": [],
                "agg_customer_performance": [],
            }),
        )

        # Parse Logging
        log_raw = cfg_dict.get("logging", {})
        log_cfg = LoggingConfig(
            level=os.environ.get("LOG_LEVEL", log_raw.get("level", "INFO")),
            console=log_raw.get("console", True),
            file=log_raw.get("file", True),
            log_dir=log_raw.get("log_dir", "./logs"),
        )

        config_obj = cls(
            environment=env,
            local=local_cfg,
            aws=aws_cfg,
            spark=spark_cfg,
            generator=gen_cfg,
            storage=storage_cfg,
            logging=log_cfg,
        )
        config_obj.validate()
        return config_obj

    def validate(self) -> None:
        """Validates configuration sanity and reports early, actionable errors."""
        if self.environment not in ("local", "aws"):
            raise ValueError(
                f"Invalid environment '{self.environment}'. Must be either 'local' or 'aws'."
            )

        if self.environment == "aws":
            if not self.aws.s3_bucket:
                raise ValueError(
                    "AWS mode enabled (environment='aws'), but 'aws.s3_bucket' is not configured. "
                    "Please set AWS_S3_BUCKET or configure it in config/config.yaml."
                )
            if not self.aws.glue_database:
                raise ValueError(
                    "AWS mode enabled, but 'aws.glue_database' is not set."
                )
