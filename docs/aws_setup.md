# AWS Deployment & Setup Guide

This guide describes how to configure and execute the pipeline in AWS Integration Mode.

## 1. Prerequisites
- An active AWS Account.
- AWS CLI configured or IAM Role assigned.
- Python 3.10+ with project dependencies installed (`pip install -r requirements.txt`).

## 2. Least Privilege IAM Permissions
To run the AWS synchronization and Athena queries, the executing identity needs permissions on S3, Glue, and Athena.

### Recommended IAM Policy:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "S3DataLakeAccess",
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:GetBucketLocation",
        "s3:GetObject",
        "s3:PutObject"
      ],
      "Resource": [
        "arn:aws:s3:::YOUR_BUCKET",
        "arn:aws:s3:::YOUR_BUCKET/*"
      ]
    },
    {
      "Sid": "GlueCatalogManagement",
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase",
        "glue:CreateDatabase",
        "glue:GetTable",
        "glue:CreateTable",
        "glue:UpdateTable",
        "glue:BatchCreatePartition",
        "glue:GetPartitions"
      ],
      "Resource": [
        "arn:aws:glue:us-east-1:*:catalog",
        "arn:aws:glue:us-east-1:*:database/ecommerce_data_lake",
        "arn:aws:glue:us-east-1:*:table/ecommerce_data_lake/*"
      ]
    },
    {
      "Sid": "AthenaQueryExecution",
      "Effect": "Allow",
      "Action": [
        "athena:StartQueryExecution",
        "athena:GetQueryExecution",
        "athena:GetQueryResults",
        "athena:GetWorkGroup"
      ],
      "Resource": "*"
    }
  ]
}
```

## 3. Configuration Setup
Create a `.env` file or export environment variables:
```bash
export PIPELINE_ENV=aws
export AWS_REGION=us-east-1
export AWS_S3_BUCKET=my-ecommerce-data-lake-bucket
export AWS_GLUE_DATABASE=ecommerce_data_lake
export AWS_ATHENA_OUTPUT_LOCATION=s3://my-ecommerce-data-lake-bucket/athena-query-results/
```

Or configure `config/config.yaml`:
```yaml
environment: aws

aws:
  region: us-east-1
  s3_bucket: my-ecommerce-data-lake-bucket
  s3_base_prefix: ecommerce
  glue_database: ecommerce_data_lake
  glue_table_prefix: ecommerce_
```

## 4. Execution Commands
Run the AWS integrated pipeline:
```bash
python scripts/run_aws.py --batch batch_001 --bucket my-ecommerce-data-lake-bucket --region us-east-1
```

## 5. Athena Query Setup
1. In the AWS Athena Console, ensure the Query Result Location is configured (e.g. `s3://my-ecommerce-data-lake-bucket/athena-query-results/`).
2. Run `sql/create_tables.sql` to register or verify the external table definitions.
3. Discover new partitions:
   ```sql
   MSCK REPAIR TABLE ecommerce_data_lake.fact_order_items;
   MSCK REPAIR TABLE ecommerce_data_lake.agg_daily_sales;
   ```
4. Execute analytical queries from `sql/analytics/`.
