"""AWS S3 integration client: resilient uploads, downloads, validations, and retries."""

import time
from pathlib import Path
from typing import Any, List

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

from ecommerce_data_lake.config import AWSStorageConfig
from ecommerce_data_lake.logging_config import setup_logger

logger = setup_logger("ecommerce_data_lake.aws_s3")


class S3SyncClient:
    """Manages S3 object synchronization and bucket health checks with exponential backoff."""

    def __init__(self, config: AWSStorageConfig) -> None:
        self.config = config
        self.region = config.region
        self.bucket_name = config.s3_bucket
        self._s3_client = None

    @property
    def client(self) -> Any:
        if self._s3_client is None:
            self._s3_client = boto3.client("s3", region_name=self.region)
        return self._s3_client

    def validate_bucket_access(self) -> bool:
        """Verifies bucket existence and IAM read/write permissions."""
        if not self.bucket_name:
            raise ValueError("S3 bucket name is not configured.")

        try:
            self.client.head_bucket(Bucket=self.bucket_name)
            logger.info(f"Successfully validated access to S3 bucket: {self.bucket_name}")
            return True
        except NoCredentialsError:
            logger.error("AWS credentials not found in environment or default credentials chain.")
            raise
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "Unknown")
            if code == "404":
                raise FileNotFoundError(f"S3 bucket '{self.bucket_name}' does not exist.")
            elif code == "403":
                raise PermissionError(f"Access denied for S3 bucket '{self.bucket_name}'. Check IAM permissions.")
            else:
                logger.error(f"Error checking bucket {self.bucket_name}: {e}")
                raise

    def upload_file(
        self,
        local_path: str,
        s3_key: str,
        max_retries: int = 3,
        initial_backoff_sec: float = 1.0,
    ) -> bool:
        """Uploads a local file to S3 with bounded retry and exponential backoff."""
        path = Path(local_path)
        if not path.is_file():
            raise FileNotFoundError(f"Local file does not exist: {local_path}")

        backoff = initial_backoff_sec
        for attempt in range(1, max_retries + 1):
            try:
                self.client.upload_file(str(path), self.bucket_name, s3_key)
                logger.debug(f"Uploaded {path.name} to s3://{self.bucket_name}/{s3_key}")
                return True
            except ClientError as e:
                logger.warning(
                    f"Upload attempt {attempt}/{max_retries} failed for {s3_key}: {e}. Retrying in {backoff}s..."
                )
                if attempt == max_retries:
                    raise
                time.sleep(backoff)
                backoff *= 2
        return False

    def upload_directory(
        self,
        local_dir: str,
        s3_prefix: str,
    ) -> int:
        """Recursively uploads all files in a local directory to an S3 prefix."""
        base_dir = Path(local_dir)
        if not base_dir.is_dir():
            raise FileNotFoundError(f"Local directory does not exist: {local_dir}")

        uploaded_count = 0
        s3_prefix_clean = s3_prefix.strip("/")

        for file_path in base_dir.rglob("*"):
            if file_path.is_file():
                rel_path = file_path.relative_to(base_dir).as_posix()
                s3_key = f"{s3_prefix_clean}/{rel_path}"
                self.upload_file(str(file_path), s3_key)
                uploaded_count += 1

        logger.info(f"Uploaded {uploaded_count} files from {local_dir} to s3://{self.bucket_name}/{s3_prefix_clean}")
        return uploaded_count

    def list_objects(self, s3_prefix: str) -> List[str]:
        """Lists object keys under an S3 prefix."""
        prefix_clean = s3_prefix.strip("/")
        keys: List[str] = []
        paginator = self.client.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix_clean):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])

        return keys
