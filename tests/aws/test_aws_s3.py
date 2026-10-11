"""Unit and mock tests for AWS S3 client integration."""

import pytest

from ecommerce_data_lake.aws_s3 import S3SyncClient
from ecommerce_data_lake.config import AWSStorageConfig


@pytest.mark.aws
def test_s3_client_requires_bucket_name():
    config = AWSStorageConfig(region="us-east-1", s3_bucket="")
    client = S3SyncClient(config)

    with pytest.raises(ValueError, match="S3 bucket name is not configured"):
        client.validate_bucket_access()


@pytest.mark.aws
def test_s3_upload_fails_on_missing_file():
    config = AWSStorageConfig(region="us-east-1", s3_bucket="dummy-bucket")
    client = S3SyncClient(config)

    with pytest.raises(FileNotFoundError):
        client.upload_file("nonexistent_file.parquet", "ecommerce/gold/test.parquet")
