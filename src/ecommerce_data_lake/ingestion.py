"""Data ingestion layer: discovers inputs, attaches lineage metadata, and creates Bronze layer."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from ecommerce_data_lake.logging_config import setup_logger
from ecommerce_data_lake.schemas import PRIMARY_KEYS, RAW_SCHEMAS
from ecommerce_data_lake.storage import StorageManager

logger = setup_logger("ecommerce_data_lake.ingestion")


@dataclass
class IngestionMetrics:
    batch_id: str
    files_discovered: int = 0
    files_processed: int = 0
    files_failed: int = 0
    total_rows_read: int = 0
    entity_counts: Dict[str, int] = field(default_factory=dict)
    quarantine_rows: int = 0
    errors: List[str] = field(default_factory=list)


class IngestionEngine:
    """Discovers source transactional files, attaches audit lineage, and writes Bronze datasets."""

    def __init__(self, spark: SparkSession, storage: StorageManager) -> None:
        self.spark = spark
        self.storage = storage

    def discover_batch_files(self, raw_dir: str, batch_id: str) -> Dict[str, str]:
        """Discovers eligible data files in the batch directory matching known entities."""
        batch_path = Path(raw_dir) / batch_id
        if not batch_path.is_dir():
            raise FileNotFoundError(f"Batch directory does not exist: {batch_path}")

        discovered: Dict[str, str] = {}
        for entity_name in RAW_SCHEMAS.keys():
            # Support both .csv and .json extensions
            csv_file = batch_path / f"{entity_name}.csv"
            json_file = batch_path / f"{entity_name}.json"
            if csv_file.is_file():
                discovered[entity_name] = str(csv_file)
            elif json_file.is_file():
                discovered[entity_name] = str(json_file)

        return discovered

    def ingest_file(
        self,
        file_path: str,
        entity_name: str,
        batch_id: str,
        ingestion_ts: datetime,
    ) -> DataFrame:
        """Reads raw data, validates column presence, and attaches metadata lineage."""
        expected_schema = RAW_SCHEMAS.get(entity_name)
        if not expected_schema:
            raise ValueError(f"No registered schema for entity: {entity_name}")

        ext = Path(file_path).suffix.lower()
        filename = Path(file_path).name

        if ext == ".csv":
            raw_df = (
                self.spark.read.option("header", "true")
                .option("mode", "PERMISSIVE")
                .option("columnNameOfCorruptRecord", "_corrupt_record")
                .csv(file_path)
            )
        elif ext == ".json":
            raw_df = self.spark.read.option("mode", "PERMISSIVE").json(file_path)
        else:
            raise ValueError(f"Unsupported file format: {ext}")

        # Check required primary key column presence
        pk_col = PRIMARY_KEYS.get(entity_name)
        if pk_col and pk_col not in raw_df.columns:
            raise ValueError(
                f"Source file {filename} is missing mandatory primary key column '{pk_col}'."
            )

        # Attach Ingestion Lineage Metadata
        ingest_ts_str = ingestion_ts.strftime("%Y-%m-%d %H:%M:%S")

        enriched_df = (
            raw_df
            .withColumn("_ingestion_timestamp", F.to_timestamp(F.lit(ingest_ts_str)))
            .withColumn("_source_filename", F.lit(filename))
            .withColumn("_batch_id", F.lit(batch_id))
            .withColumn(
                "_source_record_id",
                F.concat_ws("_", F.lit(entity_name), F.lit(batch_id), F.monotonically_increasing_id().cast(StringType())),
            )
        )

        return enriched_df

    def process_batch(
        self,
        raw_dir: str,
        bronze_dir: str,
        batch_id: str,
    ) -> Tuple[Dict[str, DataFrame], IngestionMetrics]:
        """Ingests all discovered entities for a batch and writes them to the Bronze layer."""
        metrics = IngestionMetrics(batch_id=batch_id)
        ingestion_ts = datetime.now(timezone.utc)
        bronze_dfs: Dict[str, DataFrame] = {}

        discovered = self.discover_batch_files(raw_dir, batch_id)
        metrics.files_discovered = len(discovered)
        logger.info(f"Discovered {len(discovered)} files for batch {batch_id}: {list(discovered.keys())}")

        for entity_name, file_path in discovered.items():
            try:
                df = self.ingest_file(file_path, entity_name, batch_id, ingestion_ts)
                count = df.count()
                metrics.total_rows_read += count
                metrics.entity_counts[entity_name] = count
                metrics.files_processed += 1

                # Save faithfully to Bronze (partitioned by batch_id)
                entity_bronze_path = f"{bronze_dir}/{entity_name}/batch_id={batch_id}"
                self.storage.write_parquet(df, entity_bronze_path, mode="overwrite")
                bronze_dfs[entity_name] = df

                logger.info(f"Ingested {count} rows for {entity_name} into Bronze: {entity_bronze_path}")
            except Exception as e:
                metrics.files_failed += 1
                metrics.errors.append(f"{entity_name}: {str(e)}")
                logger.error(f"Failed ingesting {entity_name} from {file_path}: {e}")
                raise

        return bronze_dfs, metrics
