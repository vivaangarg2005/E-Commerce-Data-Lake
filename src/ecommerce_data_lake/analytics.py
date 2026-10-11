"""Analytics runner: executes Athena-compatible SQL queries against Gold Parquet datasets."""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import duckdb

from ecommerce_data_lake.logging_config import setup_logger

logger = setup_logger("ecommerce_data_lake.analytics")


@dataclass
class QueryResult:
    query_name: str
    row_count: int
    columns: List[str]
    sample_rows: List[Dict[str, Any]]
    execution_time_ms: float


class AnalyticsEngine:
    """Executes analytical SQL queries against Gold and Quarantine datasets using DuckDB."""

    def __init__(self, gold_dir: str, quarantine_dir: Optional[str] = None) -> None:
        self.gold_dir = Path(gold_dir)
        self.quarantine_dir = Path(quarantine_dir) if quarantine_dir else None
        self.conn = duckdb.connect(database=":memory:")
        self._register_views()

    def _register_views(self) -> None:
        """Registers Parquet files from Gold and Quarantine layers as queryable SQL views."""
        # 1. Fact Order Items (partitioned by year/month)
        fact_path = self.gold_dir / "fact_order_items"
        if fact_path.exists():
            pattern = str(fact_path / "**" / "*.parquet").replace("\\", "/")
            self.conn.execute(f"CREATE OR REPLACE VIEW fact_order_items AS SELECT * FROM read_parquet('{pattern}', hive_partitioning=1);")
            logger.debug("Registered view: fact_order_items")

        # 2. Dim Customers
        dim_cust_path = self.gold_dir / "dim_customers"
        if dim_cust_path.exists():
            pattern = str(dim_cust_path / "*.parquet").replace("\\", "/")
            self.conn.execute(f"CREATE OR REPLACE VIEW dim_customers AS SELECT * FROM read_parquet('{pattern}');")
            logger.debug("Registered view: dim_customers")

        # 3. Dim Products
        dim_prod_path = self.gold_dir / "dim_products"
        if dim_prod_path.exists():
            pattern = str(dim_prod_path / "*.parquet").replace("\\", "/")
            self.conn.execute(f"CREATE OR REPLACE VIEW dim_products AS SELECT * FROM read_parquet('{pattern}');")
            logger.debug("Registered view: dim_products")

        # 4. Dim Dates
        dim_dates_path = self.gold_dir / "dim_dates"
        if dim_dates_path.exists():
            pattern = str(dim_dates_path / "*.parquet").replace("\\", "/")
            self.conn.execute(f"CREATE OR REPLACE VIEW dim_dates AS SELECT * FROM read_parquet('{pattern}');")
            logger.debug("Registered view: dim_dates")

        # 5. Agg Daily Sales
        agg_daily_path = self.gold_dir / "agg_daily_sales"
        if agg_daily_path.exists():
            pattern = str(agg_daily_path / "**" / "*.parquet").replace("\\", "/")
            self.conn.execute(f"CREATE OR REPLACE VIEW agg_daily_sales AS SELECT * FROM read_parquet('{pattern}', hive_partitioning=1);")
            logger.debug("Registered view: agg_daily_sales")

        # 6. Agg Customer Performance
        agg_cust_path = self.gold_dir / "agg_customer_performance"
        if agg_cust_path.exists():
            pattern = str(agg_cust_path / "*.parquet").replace("\\", "/")
            self.conn.execute(f"CREATE OR REPLACE VIEW agg_customer_performance AS SELECT * FROM read_parquet('{pattern}');")
            logger.debug("Registered view: agg_customer_performance")

        # 7. Quarantine Exceptions
        if self.quarantine_dir and self.quarantine_dir.exists():
            q_pattern = str(self.quarantine_dir / "**" / "*.parquet").replace("\\", "/")
            try:
                self.conn.execute(f"CREATE OR REPLACE VIEW quarantine_exceptions AS SELECT * FROM read_parquet('{q_pattern}');")
                logger.debug("Registered view: quarantine_exceptions")
            except Exception:
                # If quarantine directory is empty or has no parquet files yet
                self.conn.execute("""
                    CREATE OR REPLACE VIEW quarantine_exceptions AS 
                    SELECT '' as quarantine_id, '' as entity_name, '' as source_record_id, '' as batch_id,
                           '' as source_filename, '' as rejection_stage, '' as rule_code, '' as rejection_reason,
                           '' as raw_record_json, TIMESTAMP '2025-01-01 00:00:00' as quarantine_timestamp
                    WHERE 1 = 0;
                """)

    def execute_query(self, sql_query: str, query_name: str = "custom_query") -> QueryResult:
        """Executes SQL query and returns strongly typed QueryResult."""
        import time
        start_time = time.perf_counter()
        df = self.conn.execute(sql_query).df()
        duration_ms = (time.perf_counter() - start_time) * 1000

        cols = list(df.columns)
        records = df.head(10).to_dict(orient="records")

        return QueryResult(
            query_name=query_name,
            row_count=len(df),
            columns=cols,
            sample_rows=records,
            execution_time_ms=round(duration_ms, 2),
        )

    def execute_sql_file(self, sql_file_path: str) -> QueryResult:
        """Reads SQL statement from file and executes it."""
        path = Path(sql_file_path)
        if not path.is_file():
            raise FileNotFoundError(f"SQL file not found: {sql_file_path}")

        with open(path, "r", encoding="utf-8") as f:
            query = f.read()

        return self.execute_query(query, query_name=path.stem)
