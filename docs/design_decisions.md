# Key Architectural Decisions & Engineering Rationale

## 1. Why Parquet as the Primary Storage Format?
- **Columnar Pruning & Compression:** E-commerce analytical queries frequently aggregate monetary measures across millions of rows while filtering on a few dimensions (e.g. `order_date`, `category`). Parquet reads only the targeted column byte streams from disk, reducing I/O by up to 80% compared to row-based formats (CSV/JSON).
- **Embedded Schema & Metadata:** Parquet stores statistics (min, max, null counts) per column chunk, enabling predicates pushdown both in Spark and Athena.
- **Snappy Compression:** Offers the optimal balance between high compression ratios and low CPU decompression overhead.

## 2. Why Star Schema with Fact at Order-Item Grain?
- **Grain Integrity:** An order may contain multiple distinct items across categories. Modeling the central fact table at the **order-item grain** allows direct aggregation by product, category, or customer without join fan-out or double-counting revenue.
- **No Revenue Inflation:** In typical flawed implementations, joining orders with items duplicates order-level discounts or totals. In our model, every line item maintains its own `gross_amount = qty * unit_price`, `discount_amount`, and `net_amount = gross_amount - discount_amount`.

## 3. Partitioning Strategy: Why Year/Month and Not Exact Day?
- **Preventing Small File Problem:** Partitioning by exact date (`YYYY-MM-DD`) on small to medium e-commerce volumes creates thousands of tiny files (<1MB), which degrades query performance in distributed engines like Athena and Spark due to excessive metadata overhead.
- **Query Alignment:** The overwhelming majority of analytical queries filter by year, quarter, or month (`year = 2024 AND month = 5`). Partitioning `fact_order_items` by `(year, month)` satisfies partition pruning while ensuring file sizes remain healthy.

## 4. Why Deterministic Deduplication with Tie-Breakers?
- **Reproducibility:** A deduplication strategy that relies solely on `updated_at` can produce non-deterministic results if multiple events have identical millisecond timestamps.
- **Stable Tie-Breaker:** By ordering by `(updated_at DESC, _source_record_id DESC)`, repeated pipeline executions produce 100% byte-for-byte identical outputs, guaranteeing true idempotency.

## 5. Local DuckDB vs Spark for SQL Queries
- While Apache PySpark is used for heavy distributed ETL and writing Parquet, **DuckDB** is embedded as the local SQL analytics engine for querying the Gold layer. DuckDB reads partitioned Parquet natively in milliseconds, accurately simulates Athena engine v3 syntax, and enables instant validation without starting another heavy JVM cluster.
