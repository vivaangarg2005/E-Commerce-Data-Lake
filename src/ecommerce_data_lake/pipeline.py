"""End-to-end Pipeline orchestrator: Ingestion -> Bronze -> Validation -> Silver -> Gold -> Audit."""

import json
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

from pyspark.sql import DataFrame

from ecommerce_data_lake.aws_s3 import S3SyncClient
from ecommerce_data_lake.config import PipelineConfig
from ecommerce_data_lake.deduplication import DeduplicationMetrics, Deduplicator
from ecommerce_data_lake.glue_catalog import GlueCatalogManager
from ecommerce_data_lake.incremental import IncrementalStateManager
from ecommerce_data_lake.ingestion import IngestionEngine
from ecommerce_data_lake.logging_config import setup_logger
from ecommerce_data_lake.schemas import QUARANTINE_SCHEMA
from ecommerce_data_lake.storage import StorageManager, get_spark_session
from ecommerce_data_lake.transformations import GoldTransformer, SilverTransformer
from ecommerce_data_lake.validation import DataValidator

logger = setup_logger("ecommerce_data_lake.pipeline")


@dataclass
class AuditReport:
    run_id: str
    batch_id: str
    environment: str
    start_time: str
    end_time: str
    duration_seconds: float
    status: str  # 'SUCCESS', 'SUCCESS_WITH_WARNINGS', 'FAILED'
    total_rows_read: int = 0
    total_silver_rows: int = 0
    total_quarantine_rows: int = 0
    total_duplicates_removed: int = 0
    data_quality_score: float = 100.0
    entity_counts: Dict[str, Dict[str, int]] = field(default_factory=dict)
    quarantine_rule_counts: Dict[str, int] = field(default_factory=dict)
    gold_record_counts: Dict[str, int] = field(default_factory=dict)
    error_details: List[str] = field(default_factory=list)


class EcommercePipeline:
    """Orchestrates end-to-end execution across all Medallion layers."""

    def __init__(self, config: Optional[PipelineConfig] = None) -> None:
        self.config = config or PipelineConfig.load()
        self.spark = get_spark_session(self.config.spark)
        self.storage = StorageManager(self.spark)
        self.state_manager = IncrementalStateManager(self.config.local.state_dir)

    def run_pipeline(
        self,
        batch_id: str = "batch_001",
        run_id: Optional[str] = None,
    ) -> AuditReport:
        """Executes the full data lake ETL pipeline for a single batch."""
        active_run_id = run_id or f"run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
        start_time_dt = datetime.now(timezone.utc)
        start_time_iso = start_time_dt.isoformat()
        start_perf = time.perf_counter()

        logger.info(f"Starting pipeline execution: run_id={active_run_id}, batch_id={batch_id}, env={self.config.environment}")

        report = AuditReport(
            run_id=active_run_id,
            batch_id=batch_id,
            environment=self.config.environment,
            start_time=start_time_iso,
            end_time="",
            duration_seconds=0.0,
            status="IN_PROGRESS",
        )

        try:
            self.state_manager.begin_batch(batch_id)

            # ------------------------------------------------------------------
            # STAGE 1: INGESTION -> BRONZE LAYER
            # ------------------------------------------------------------------
            logger.info("=== STAGE 1: Ingestion to Bronze Layer ===")
            ingestion_engine = IngestionEngine(self.spark, self.storage)
            bronze_dfs, ingest_metrics = ingestion_engine.process_batch(
                raw_dir=self.config.local.raw_dir,
                bronze_dir=self.config.local.bronze_dir,
                batch_id=batch_id,
            )
            report.total_rows_read = ingest_metrics.total_rows_read

            # ------------------------------------------------------------------
            # STAGE 2: VALIDATION & QUARANTINE ROUTING
            # ------------------------------------------------------------------
            logger.info("=== STAGE 2: Data Validation & Quality Checks ===")
            validator = DataValidator(self.spark)
            all_quarantine_dfs: List[DataFrame] = []

            # 2a. Validate Customers
            clean_cust, q_cust, val_cust_sum = validator.validate_customers(bronze_dfs["customers"])
            all_quarantine_dfs.append(q_cust)

            # 2b. Validate Products
            clean_prod, q_prod, val_prod_sum = validator.validate_products(bronze_dfs["products"])
            all_quarantine_dfs.append(q_prod)

            # 2c. Validate Orders (Referential integrity with validated customers)
            clean_ord, q_ord, val_ord_sum = validator.validate_orders(bronze_dfs["orders"], clean_cust)
            all_quarantine_dfs.append(q_ord)

            # 2d. Validate Order Items (Referential integrity with valid orders & products)
            clean_items, q_items, val_items_sum = validator.validate_order_items(
                bronze_dfs["order_items"], clean_ord, clean_prod
            )
            all_quarantine_dfs.append(q_items)

            # 2e. Validate Returns (if present, referential integrity with valid items)
            if "returns" in bronze_dfs and not bronze_dfs["returns"].isEmpty():
                clean_returns, q_returns, val_returns_sum = validator.validate_returns(
                    bronze_dfs["returns"], clean_items
                )
                all_quarantine_dfs.append(q_returns)
            else:
                clean_returns = self.spark.createDataFrame([], bronze_dfs["customers"].schema)
                q_returns = self.spark.createDataFrame([], QUARANTINE_SCHEMA)

            # Combine and save all Quarantine records
            valid_quarantines = [q for q in all_quarantine_dfs if not q.isEmpty()]
            if valid_quarantines:
                quarantine_combined = valid_quarantines[0]
                for q in valid_quarantines[1:]:
                    quarantine_combined = quarantine_combined.unionByName(q)
            else:
                quarantine_combined = self.spark.createDataFrame([], QUARANTINE_SCHEMA)

            quarantine_count = quarantine_combined.count()
            report.total_quarantine_rows = quarantine_count

            if quarantine_count > 0:
                quarantine_out = f"{self.config.local.quarantine_dir}/batch_id={batch_id}"
                self.storage.write_parquet(quarantine_combined, quarantine_out, mode="overwrite")
                logger.info(f"Saved {quarantine_count} invalid records to Quarantine: {quarantine_out}")

            # ------------------------------------------------------------------
            # STAGE 3: DEDUPLICATION & SILVER LAYER
            # ------------------------------------------------------------------
            logger.info("=== STAGE 3: Deterministic Deduplication & Silver Layer ===")
            dedup_cust, m_cust = Deduplicator.deduplicate(clean_cust, "customers")
            dedup_prod, m_prod = Deduplicator.deduplicate(clean_prod, "products")
            dedup_ord, m_ord = Deduplicator.deduplicate(clean_ord, "orders")
            dedup_items, m_items = Deduplicator.deduplicate(clean_items, "order_items")
            dedup_ret, m_ret = (
                Deduplicator.deduplicate(clean_returns, "returns")
                if not clean_returns.isEmpty()
                else (clean_returns, DeduplicationMetrics("returns"))
            )

            total_dups = (
                m_cust.duplicate_rows_removed
                + m_prod.duplicate_rows_removed
                + m_ord.duplicate_rows_removed
                + m_items.duplicate_rows_removed
                + m_ret.duplicate_rows_removed
            )
            report.total_duplicates_removed = total_dups

            # Standardize into Silver Schemas
            silver_cust = SilverTransformer.transform_customers(dedup_cust)
            silver_prod = SilverTransformer.transform_products(dedup_prod)
            silver_ord = SilverTransformer.transform_orders(dedup_ord)
            silver_items = SilverTransformer.transform_order_items(dedup_items)
            silver_ret = (
                SilverTransformer.transform_returns(dedup_ret)
                if not dedup_ret.isEmpty()
                else self.spark.createDataFrame([], clean_returns.schema)
            )

            # Save Silver Datasets
            self.storage.write_parquet(silver_cust, f"{self.config.local.silver_dir}/customers", mode="overwrite")
            self.storage.write_parquet(silver_prod, f"{self.config.local.silver_dir}/products", mode="overwrite")
            self.storage.write_parquet(silver_ord, f"{self.config.local.silver_dir}/orders", mode="overwrite")
            self.storage.write_parquet(silver_items, f"{self.config.local.silver_dir}/order_items", mode="overwrite")
            if not silver_ret.isEmpty():
                self.storage.write_parquet(silver_ret, f"{self.config.local.silver_dir}/returns", mode="overwrite")

            silver_total = (
                silver_cust.count()
                + silver_prod.count()
                + silver_ord.count()
                + silver_items.count()
                + silver_ret.count()
            )
            report.total_silver_rows = silver_total

            # ------------------------------------------------------------------
            # STAGE 4: GOLD LAYER ANALYTICS MARTS
            # ------------------------------------------------------------------
            logger.info("=== STAGE 4: Gold Analytics Layer & Star Schema ===")
            gold_engine = GoldTransformer(self.spark)

            # 4a. Dim Dates
            dim_dates = gold_engine.build_dim_dates()
            self.storage.write_parquet(dim_dates, f"{self.config.local.gold_dir}/dim_dates", mode="overwrite")

            # 4b. Fact Order Items (Grain: 1 row per order item)
            fact_order_items = gold_engine.build_fact_order_items(
                order_items_df=silver_items,
                orders_df=silver_ord,
                products_df=silver_prod,
                returns_df=silver_ret,
            )
            self.storage.write_parquet(
                fact_order_items,
                f"{self.config.local.gold_dir}/fact_order_items",
                partition_by=self.config.storage.partitions.get("fact_order_items", ["year", "month"]),
                mode="overwrite",
            )

            # 4c. Dim Products
            dim_products = gold_engine.build_dim_products(silver_prod, silver_items)
            self.storage.write_parquet(dim_products, f"{self.config.local.gold_dir}/dim_products", mode="overwrite")

            # 4d. Dim Customers
            dim_customers = gold_engine.build_dim_customers(silver_cust, fact_order_items)
            self.storage.write_parquet(dim_customers, f"{self.config.local.gold_dir}/dim_customers", mode="overwrite")

            # 4e. Agg Daily Sales
            agg_daily_sales = gold_engine.build_agg_daily_sales(fact_order_items)
            self.storage.write_parquet(
                agg_daily_sales,
                f"{self.config.local.gold_dir}/agg_daily_sales",
                partition_by=self.config.storage.partitions.get("agg_daily_sales", ["year"]),
                mode="overwrite",
            )

            # 4f. Agg Customer Performance
            agg_cust_perf = gold_engine.build_agg_customer_performance(dim_customers)
            self.storage.write_parquet(
                agg_cust_perf, f"{self.config.local.gold_dir}/agg_customer_performance", mode="overwrite"
            )

            report.gold_record_counts = {
                "fact_order_items": fact_order_items.count(),
                "dim_customers": dim_customers.count(),
                "dim_products": dim_products.count(),
                "dim_dates": dim_dates.count(),
                "agg_daily_sales": agg_daily_sales.count(),
                "agg_customer_performance": agg_cust_perf.count(),
            }

            # ------------------------------------------------------------------
            # STAGE 5: INCREMENTAL COMMIT & AUDIT REPORTING
            # ------------------------------------------------------------------
            self.state_manager.commit_batch(
                batch_id=batch_id,
                records_read=report.total_rows_read,
                records_silver=report.total_silver_rows,
                records_quarantine=report.total_quarantine_rows,
                batch_watermark=datetime.now(timezone.utc).isoformat(),
            )

            # Data Quality Score calculation
            if report.total_rows_read > 0:
                pass_ratio = (report.total_rows_read - report.total_quarantine_rows) / report.total_rows_read
                report.data_quality_score = round(max(0.0, pass_ratio * 100.0), 2)
            else:
                report.data_quality_score = 100.0

            # ------------------------------------------------------------------
            # STAGE 6: AWS INTEGRATION (IF CONFIGURED)
            # ------------------------------------------------------------------
            if self.config.environment == "aws":
                logger.info("=== STAGE 6: AWS S3 and Glue Catalog Synchronization ===")
                s3_client = S3SyncClient(self.config.aws)
                s3_client.validate_bucket_access()

                # Upload Gold Parquet datasets to S3
                s3_client.upload_directory(
                    local_dir=self.config.local.gold_dir,
                    s3_prefix=self.config.aws.s3_prefixes.get("gold", "ecommerce/gold"),
                )

                # Sync Glue Data Catalog
                glue_mgr = GlueCatalogManager(self.config.aws)
                glue_mgr.sync_all_tables()
                logger.info("AWS S3 upload and Glue Data Catalog tables synced successfully.")

            report.status = "SUCCESS_WITH_WARNINGS" if quarantine_count > 0 else "SUCCESS"

        except Exception as e:
            report.status = "FAILED"
            report.error_details.append(str(e))
            self.state_manager.fail_batch(batch_id, str(e))
            logger.error(f"Pipeline failed for batch {batch_id}: {e}", exc_info=True)
            raise
        finally:
            end_perf = time.perf_counter()
            report.end_time = datetime.now(timezone.utc).isoformat()
            report.duration_seconds = round(end_perf - start_perf, 2)
            self._save_audit_report(report)

        return report

    def _save_audit_report(self, report: AuditReport) -> None:
        """Writes structured JSON and Markdown audit reports."""
        audit_path = Path(self.config.local.audit_dir)
        audit_path.mkdir(parents=True, exist_ok=True)

        json_file = audit_path / f"audit_{report.run_id}.json"
        with open(json_file, "w", encoding="utf-8") as f:
            json.dump(asdict(report), f, indent=2)

        md_file = audit_path / f"audit_{report.run_id}.md"
        with open(md_file, "w", encoding="utf-8") as f:
            f.write(f"""# Pipeline Run Audit Report: {report.run_id}

- **Batch ID:** `{report.batch_id}`
- **Environment:** `{report.environment}`
- **Execution Status:** **{report.status}**
- **Start Time:** `{report.start_time}`
- **End Time:** `{report.end_time}`
- **Duration:** `{report.duration_seconds}s`
- **Data Quality Score:** `{report.data_quality_score}%`

## Processing Metrics
| Metric | Count |
| :--- | :--- |
| **Total Rows Read** | {report.total_rows_read} |
| **Silver Curated Rows** | {report.total_silver_rows} |
| **Quarantine Rejected Rows** | {report.total_quarantine_rows} |
| **Duplicates Removed** | {report.total_duplicates_removed} |

## Gold Analytics Marts
| Dataset | Row Count |
| :--- | :--- |
""" + "\n".join([f"| `{k}` | {v} |" for k, v in report.gold_record_counts.items()]) + "\n")

        logger.info(f"Audit reports written to {json_file} and {md_file}")
