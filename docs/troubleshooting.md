# Troubleshooting & Operations Guide

## Common Issues & Resolutions

### 1. Windows: PySpark Missing Hadoop Binaries (`HADOOP_HOME unset`)
- **Symptom:** `java.io.FileNotFoundException: HADOOP_HOME and hadoop.home.dir are unset.`
- **Cause:** Apache Spark's Hadoop client requires `winutils.exe` and `hadoop.dll` when running on Windows to interact with the local filesystem.
- **Solution:** Run the automated setup script included in the repo:
  ```bash
  python scripts/setup_winutils.py
  ```
  This automatically downloads verified binaries into `.hadoop/bin` and configures `HADOOP_HOME`.

### 2. PySpark Python Worker Connection Timeout / Socket Crash
- **Symptom:** `java.net.SocketTimeoutException: Timed out while waiting for the Python worker to connect back` or `Python worker exited unexpectedly (crashed)`.
- **Cause:** On Windows, PySpark may attempt to spawn an alias or localhost binding fails across network interfaces.
- **Solution:** The pipeline explicitly enforces:
  - `PYSPARK_PYTHON = sys.executable`
  - `spark.driver.host = 127.0.0.1`
  - `spark.driver.bindAddress = 127.0.0.1`
  - Uses native DataFrame operations instead of Python RDD transformations.

### 3. Missing S3 Bucket in AWS Mode
- **Symptom:** `FileNotFoundError: S3 bucket 'your-bucket' does not exist.`
- **Solution:** Verify the bucket name in `config/config.yaml` or set the `AWS_S3_BUCKET` environment variable. The AWS mode strictly validates the bucket upfront and never pretends cloud execution succeeded without real credentials.

### 4. Athena Query Returns 0 Rows
- **Symptom:** External table exists in Glue/Athena, but queries return 0 rows.
- **Cause:** Partitioned tables (`fact_order_items`, `agg_daily_sales`) require partition discovery.
- **Solution:** Execute partition recovery in Athena:
  ```sql
  MSCK REPAIR TABLE ecommerce_data_lake.fact_order_items;
  MSCK REPAIR TABLE ecommerce_data_lake.agg_daily_sales;
  ```

### 5. Referential Integrity Quarantine
- **Symptom:** Certain rows appear in `data/quarantine` instead of Silver.
- **Explanation:** This is expected pipeline behavior. If an order references a non-existent customer, or an item has a negative quantity, the validation engine routes it to quarantine with a specific `rule_code` and `rejection_reason` for investigation. Check `data/audit/audit_*.md` for details.
