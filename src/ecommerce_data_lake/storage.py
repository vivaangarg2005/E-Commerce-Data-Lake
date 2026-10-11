"""Storage abstraction and Spark session initialization for Local and AWS environments."""

import os
import sys
from pathlib import Path
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession

from ecommerce_data_lake.config import SparkConfig


def init_spark_environment() -> None:
    """Configures local Python & Hadoop environment variables for stable Windows execution."""
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    # Detect local winutils if on Windows
    if os.name == "nt":
        repo_root = Path(__file__).resolve().parent.parent.parent
        hadoop_dir = repo_root / ".hadoop"
        if hadoop_dir.exists() and (hadoop_dir / "bin" / "winutils.exe").exists():
            os.environ["HADOOP_HOME"] = str(hadoop_dir)
            bin_dir = str(hadoop_dir / "bin")
            current_path = os.environ.get("PATH", "")
            if bin_dir not in current_path:
                os.environ["PATH"] = f"{bin_dir};{current_path}"


def get_spark_session(config: Optional[SparkConfig] = None) -> SparkSession:
    """Creates or retrieves a tuned Apache Spark session."""
    init_spark_environment()

    app_name = config.app_name if config else "EcommerceDataLake"
    master = config.master if config else "local[*]"
    driver_mem = config.driver_memory if config else "2g"
    partitions = config.shuffle_partitions if config else 4
    log_level = config.log_level if config else "WARN"

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.memory", driver_mem)
        .config("spark.sql.shuffle.partitions", str(partitions))
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(log_level)
    return spark


class StorageManager:
    """Handles read/write operations for Parquet datasets across Local and S3 paths."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def write_parquet(
        self,
        df: DataFrame,
        output_path: str,
        partition_by: Optional[List[str]] = None,
        mode: str = "overwrite",
    ) -> None:
        """Writes DataFrame to Parquet format with optional partition columns."""
        writer = df.write.mode(mode).format("parquet")
        if partition_by and len(partition_by) > 0:
            writer = writer.partitionBy(*partition_by)
        writer.save(output_path)

    def read_parquet(self, path: str) -> DataFrame:
        """Reads Parquet dataset from the given path."""
        return self.spark.read.parquet(path)

    def write_csv(
        self,
        df: DataFrame,
        output_path: str,
        header: bool = True,
        mode: str = "overwrite",
    ) -> None:
        """Writes DataFrame to CSV format."""
        df.write.mode(mode).option("header", str(header).lower()).csv(output_path)
