"""CLI command to run the AWS-integrated Data Lake pipeline."""

import argparse
import os
import sys
from pathlib import Path

# Add src to pythonpath
repo_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(repo_root / "src"))

from ecommerce_data_lake.aws_s3 import S3SyncClient
from ecommerce_data_lake.config import PipelineConfig
from ecommerce_data_lake.logging_config import setup_logger
from ecommerce_data_lake.pipeline import EcommercePipeline

logger = setup_logger("scripts.run_aws")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run AWS-integrated E-Commerce Data Lake pipeline.")
    parser.add_argument("--batch", type=str, default="batch_001", help="Batch identifier (default: batch_001)")
    parser.add_argument("--bucket", type=str, default=None, help="Override S3 bucket name")
    parser.add_argument("--region", type=str, default=None, help="Override AWS region")
    args = parser.parse_args()

    # Force environment to 'aws'
    os.environ["PIPELINE_ENV"] = "aws"
    if args.bucket:
        os.environ["AWS_S3_BUCKET"] = args.bucket
    if args.region:
        os.environ["AWS_REGION"] = args.region

    try:
        cfg = PipelineConfig.load()
    except Exception as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)

    logger.info(f"Target AWS S3 Bucket: s3://{cfg.aws.s3_bucket}")
    logger.info(f"Target AWS Region:    {cfg.aws.region}")
    logger.info(f"Target Glue Database: {cfg.aws.glue_database}")

    # Validate AWS Connectivity
    s3_client = S3SyncClient(cfg.aws)
    try:
        logger.info("Validating S3 bucket connectivity...")
        s3_client.validate_bucket_access()
    except Exception as e:
        logger.error(f"AWS pre-flight check failed: {e}")
        logger.error(
            "Ensure AWS credentials (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION) "
            "are exported or configured in ~/.aws/credentials, and that the S3 bucket exists."
        )
        sys.exit(2)

    # Execute Pipeline
    pipeline = EcommercePipeline(config=cfg)
    report = pipeline.run_pipeline(batch_id=args.batch)

    print("\n" + "=" * 80)
    print(f"AWS PIPELINE RUN COMPLETED: {report.run_id}")
    print(f"Status:             {report.status}")
    print(f"S3 Prefix:          s3://{cfg.aws.s3_bucket}/{cfg.aws.s3_base_prefix}/")
    print(f"Glue Database:      {cfg.aws.glue_database}")
    print(f"Curated Gold Rows:  {report.total_silver_rows}")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    main()
