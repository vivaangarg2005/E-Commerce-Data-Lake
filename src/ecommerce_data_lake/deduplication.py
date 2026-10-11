"""Deterministic deduplication engine with stable ordering and duplicate audit reporting."""

from dataclasses import dataclass
from typing import Optional, Tuple

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from ecommerce_data_lake.logging_config import setup_logger
from ecommerce_data_lake.schemas import PRIMARY_KEYS

logger = setup_logger("ecommerce_data_lake.deduplication")


@dataclass
class DeduplicationMetrics:
    entity_name: str
    input_rows: int = 0
    unique_rows: int = 0
    duplicate_rows_removed: int = 0


class Deduplicator:
    """Removes duplicate entity records deterministically based on business keys and update timestamps."""

    @staticmethod
    def deduplicate(
        df: DataFrame,
        entity_name: str,
        business_key: Optional[str] = None,
        order_col: str = "updated_at",
        tie_breaker_col: str = "_source_record_id",
    ) -> Tuple[DataFrame, DeduplicationMetrics]:
        """Deduplicates rows keeping the most recent record by order_col with deterministic tie-breaker."""
        key = business_key or PRIMARY_KEYS.get(entity_name)
        if not key:
            raise ValueError(f"No business key defined for entity '{entity_name}'.")

        input_count = df.count()

        # Parse timestamp safely if stored as string
        ranked_df = df
        ts_order_expr = F.to_timestamp(F.col(order_col))

        window_spec = (
            Window.partitionBy(key)
            .orderBy(
                ts_order_expr.desc_nulls_last(),
                F.col(tie_breaker_col).desc_nulls_last() if tie_breaker_col in df.columns else F.monotonically_increasing_id().desc(),
            )
        )

        deduped_df = (
            ranked_df
            .withColumn("__row_num", F.row_number().over(window_spec))
            .filter(F.col("__row_num") == 1)
            .drop("__row_num")
        )

        unique_count = deduped_df.count()
        dups_removed = input_count - unique_count

        metrics = DeduplicationMetrics(
            entity_name=entity_name,
            input_rows=input_count,
            unique_rows=unique_count,
            duplicate_rows_removed=dups_removed,
        )

        logger.info(
            f"Deduplicated '{entity_name}': {input_count} input rows, "
            f"{unique_count} distinct rows, {dups_removed} duplicates removed (key: {key})"
        )

        return deduped_df, metrics
