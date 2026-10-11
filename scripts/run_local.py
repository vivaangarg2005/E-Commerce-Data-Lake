"""CLI command to run the end-to-end Local Data Lake pipeline and execute SQL analytics."""

import argparse
import sys
from pathlib import Path

# Add src to pythonpath
repo_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(repo_root / "src"))

from ecommerce_data_lake.analytics import AnalyticsEngine
from ecommerce_data_lake.config import PipelineConfig
from ecommerce_data_lake.data_generation import SyntheticDataGenerator
from ecommerce_data_lake.logging_config import setup_logger
from ecommerce_data_lake.pipeline import EcommercePipeline

logger = setup_logger("scripts.run_local")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run local E-Commerce Data Lake pipeline.")
    parser.add_argument("--batch", type=str, default="batch_001", help="Batch identifier (default: batch_001)")
    parser.add_argument("--profile", type=str, default="dev", choices=["dev", "standard", "large"], help="Profile size if generating data")
    parser.add_argument("--auto-generate", action="store_true", default=True, help="Auto-generate data if batch is missing")
    parser.add_argument("--run-analytics", action="store_true", default=True, help="Execute SQL analytical queries after pipeline run")
    args = parser.parse_args()

    cfg = PipelineConfig.load()
    raw_batch_dir = Path(cfg.local.raw_dir) / args.batch

    # Auto-generate raw batch if missing
    if not raw_batch_dir.exists() or not any(raw_batch_dir.iterdir()):
        logger.info(f"Raw data for '{args.batch}' not found. Generating using '{args.profile}' profile...")
        generator = SyntheticDataGenerator(seed=cfg.generator.seed)
        profile = cfg.generator.profiles.get(args.profile, cfg.generator.profiles["dev"])
        data = generator.generate_batch(profile, batch_id=args.batch)
        generator.write_batch_to_csv(data, Path(cfg.local.raw_dir), args.batch)

    # Execute Pipeline
    pipeline = EcommercePipeline(config=cfg)
    report = pipeline.run_pipeline(batch_id=args.batch)

    print("\n" + "=" * 80)
    print(f"PIPELINE EXECUTION SUMMARY: {report.run_id}")
    print("=" * 80)
    print(f"Status:               {report.status}")
    print(f"Batch ID:             {report.batch_id}")
    print(f"Duration:             {report.duration_seconds}s")
    print(f"Data Quality Score:   {report.data_quality_score}%")
    print(f"Total Rows Ingested:  {report.total_rows_read}")
    print(f"Silver Curated Rows:  {report.total_silver_rows}")
    print(f"Quarantined Rows:     {report.total_quarantine_rows}")
    print(f"Duplicates Removed:   {report.total_duplicates_removed}")
    print("\nGold Analytical Datasets Created:")
    for ds_name, cnt in report.gold_record_counts.items():
        print(f"  * {ds_name:25s}: {cnt:>6d} rows")
    print("=" * 80)

    # Run SQL Analytics
    if args.run_analytics:
        print("\n" + "=" * 80)
        print("EXECUTING ATHENA-COMPATIBLE SQL ANALYTICAL QUERIES (LOCAL DUCKDB ENGINE)")
        print("=" * 80)

        analytics = AnalyticsEngine(
            gold_dir=cfg.local.gold_dir,
            quarantine_dir=cfg.local.quarantine_dir,
        )

        sql_dir = repo_root / "sql" / "analytics"
        sql_files = sorted(sql_dir.glob("*.sql"))

        for sql_file in sql_files:
            try:
                res = analytics.execute_sql_file(str(sql_file))
                print(f"\n[QUERY] {sql_file.name} (executed in {res.execution_time_ms}ms, returned {res.row_count} rows)")
                if res.sample_rows:
                    # Print first 3 rows
                    for r in res.sample_rows[:3]:
                        print("   ", r)
                else:
                    print("    (0 rows returned)")
            except Exception as e:
                print(f"[ERROR] Failed query {sql_file.name}: {e}")

        print("\n" + "=" * 80)
        print("ALL ANALYTICAL QUERIES COMPLETED SUCCESSFULLY.")
        print("=" * 80 + "\n")


if __name__ == "__main__":
    main()
