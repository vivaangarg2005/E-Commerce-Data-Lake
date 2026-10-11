"""Silver standardization and Gold dimensional/analytical business transformations."""

from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
)

from ecommerce_data_lake.logging_config import setup_logger

logger = setup_logger("ecommerce_data_lake.transformations")


class SilverTransformer:
    """Casts validated entities into standardized, strongly-typed Silver representations."""

    @staticmethod
    def transform_customers(df: DataFrame) -> DataFrame:
        return (
            df
            .withColumn("signup_date", F.to_date(F.col("signup_date"), "yyyy-MM-dd"))
            .withColumn("updated_at", F.to_timestamp(F.col("updated_at")))
            .withColumn("_ingestion_timestamp", F.to_timestamp(F.col("_ingestion_timestamp")))
            .select(
                "customer_id",
                "first_name",
                "last_name",
                "email",
                "city",
                "state",
                "signup_date",
                "customer_status",
                "updated_at",
                "_ingestion_timestamp",
                "_source_filename",
                "_batch_id",
                "_source_record_id",
            )
        )

    @staticmethod
    def transform_products(df: DataFrame) -> DataFrame:
        return (
            df
            .withColumn("unit_price", F.round(F.col("unit_price").cast(DecimalType(10, 2)), 2))
            .withColumn("updated_at", F.to_timestamp(F.col("updated_at")))
            .withColumn("_ingestion_timestamp", F.to_timestamp(F.col("_ingestion_timestamp")))
            .select(
                "product_id",
                "product_name",
                "category",
                "unit_price",
                "product_status",
                "updated_at",
                "_ingestion_timestamp",
                "_source_filename",
                "_batch_id",
                "_source_record_id",
            )
        )

    @staticmethod
    def transform_orders(df: DataFrame) -> DataFrame:
        return (
            df
            .withColumn("order_date", F.to_date(F.col("order_date"), "yyyy-MM-dd"))
            .withColumn("updated_at", F.to_timestamp(F.col("updated_at")))
            .withColumn("_ingestion_timestamp", F.to_timestamp(F.col("_ingestion_timestamp")))
            .select(
                "order_id",
                "customer_id",
                "order_date",
                "order_status",
                "payment_method",
                "updated_at",
                "_ingestion_timestamp",
                "_source_filename",
                "_batch_id",
                "_source_record_id",
            )
        )

    @staticmethod
    def transform_order_items(df: DataFrame) -> DataFrame:
        return (
            df
            .withColumn("quantity", F.col("quantity").cast(IntegerType()))
            .withColumn("unit_price", F.round(F.col("unit_price").cast(DecimalType(10, 2)), 2))
            .withColumn(
                "discount_amount",
                F.round(F.coalesce(F.col("discount_amount").cast(DecimalType(10, 2)), F.lit(0.0)), 2),
            )
            .withColumn("updated_at", F.to_timestamp(F.col("updated_at")))
            .withColumn("_ingestion_timestamp", F.to_timestamp(F.col("_ingestion_timestamp")))
            .select(
                "order_item_id",
                "order_id",
                "product_id",
                "quantity",
                "unit_price",
                "discount_amount",
                "updated_at",
                "_ingestion_timestamp",
                "_source_filename",
                "_batch_id",
                "_source_record_id",
            )
        )

    @staticmethod
    def transform_returns(df: DataFrame) -> DataFrame:
        return (
            df
            .withColumn("return_date", F.to_date(F.col("return_date"), "yyyy-MM-dd"))
            .withColumn("refund_amount", F.round(F.col("refund_amount").cast(DecimalType(10, 2)), 2))
            .withColumn("updated_at", F.to_timestamp(F.col("updated_at")))
            .withColumn("_ingestion_timestamp", F.to_timestamp(F.col("_ingestion_timestamp")))
            .select(
                "return_id",
                "order_item_id",
                "return_date",
                "return_reason",
                "refund_amount",
                "return_status",
                "updated_at",
                "_ingestion_timestamp",
                "_source_filename",
                "_batch_id",
                "_source_record_id",
            )
        )


class GoldTransformer:
    """Builds analytics-ready Star Schema and aggregated data marts from Silver datasets."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def build_dim_dates(self, start_date: str = "2024-01-01", end_date: str = "2025-12-31") -> DataFrame:
        """Generates a complete calendar date dimension table."""
        date_df = self.spark.sql(f"""
            SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) as full_date
        """)

        return (
            date_df
            .withColumn("date_key", F.date_format(F.col("full_date"), "yyyyMMdd").cast(IntegerType()))
            .withColumn("year", F.year(F.col("full_date")).cast(IntegerType()))
            .withColumn("quarter", F.quarter(F.col("full_date")).cast(IntegerType()))
            .withColumn("month", F.month(F.col("full_date")).cast(IntegerType()))
            .withColumn("month_name", F.date_format(F.col("full_date"), "MMMM"))
            .withColumn("day", F.dayofmonth(F.col("full_date")).cast(IntegerType()))
            .withColumn("day_of_week", F.dayofweek(F.col("full_date")).cast(IntegerType()))
            .withColumn("day_name", F.date_format(F.col("full_date"), "EEEE"))
            .withColumn(
                "is_weekend",
                F.when(F.dayofweek(F.col("full_date")).isin([1, 7]), F.lit(True)).otherwise(F.lit(False)),
            )
        )

    def build_dim_products(
        self,
        products_df: DataFrame,
        order_items_df: Optional[DataFrame] = None,
    ) -> DataFrame:
        """Builds Product dimension enriched with sales performance aggregations."""
        if order_items_df is not None:
            prod_sales = (
                order_items_df
                .groupBy("product_id")
                .agg(
                    F.sum("quantity").cast(IntegerType()).alias("total_units_sold"),
                    F.round(F.sum(F.col("quantity") * F.col("unit_price") - F.col("discount_amount")), 2).cast(DecimalType(12, 2)).alias("total_revenue"),
                )
            )
            return (
                products_df
                .join(prod_sales, on="product_id", how="left")
                .withColumn("total_units_sold", F.coalesce(F.col("total_units_sold"), F.lit(0)))
                .withColumn("total_revenue", F.coalesce(F.col("total_revenue"), F.lit(0.0).cast(DecimalType(12, 2))))
                .select(
                    "product_id",
                    "product_name",
                    "category",
                    "unit_price",
                    "product_status",
                    "total_units_sold",
                    "total_revenue",
                    "updated_at",
                )
            )
        return products_df.select(
            "product_id", "product_name", "category", "unit_price", "product_status", "updated_at"
        )

    def build_fact_order_items(
        self,
        order_items_df: DataFrame,
        orders_df: DataFrame,
        products_df: DataFrame,
        returns_df: Optional[DataFrame] = None,
    ) -> DataFrame:
        """Constructs the central Sales Fact dataset at order-item grain.
        
        Guarantees:
        - Grain is strictly 1 row per order_item.
        - No join fan-out inflating financial numbers.
        - Explicit gross, discount, net amounts, and return flags.
        """
        # Exclude internal metadata columns for clean join
        items = order_items_df.select(
            "order_item_id", "order_id", "product_id", "quantity", "unit_price", "discount_amount"
        )
        orders = orders_df.select(
            "order_id", "customer_id", "order_date", "order_status", "payment_method"
        )
        prods = products_df.select("product_id", "product_name", "category")

        # 1-to-1 Join items with orders and products
        joined = (
            items
            .join(orders, on="order_id", how="inner")
            .join(prods, on="product_id", how="inner")
        )

        # 1-to-1 Join with returns if present
        if returns_df is not None and not returns_df.isEmpty():
            valid_returns = (
                returns_df
                .filter(F.col("return_status").isin(["COMPLETED", "APPROVED"]))
                .groupBy("order_item_id")
                .agg(
                    F.max("return_id").alias("return_id"),
                    F.max("return_date").alias("return_date"),
                    F.max("return_reason").alias("return_reason"),
                    F.sum("refund_amount").alias("refund_amount"),
                )
            )
            joined = joined.join(valid_returns, on="order_item_id", how="left")
        else:
            joined = (
                joined
                .withColumn("return_id", F.lit(None).cast(StringType()))
                .withColumn("return_date", F.lit(None).cast(DateType()))
                .withColumn("return_reason", F.lit(None).cast(StringType()))
                .withColumn("refund_amount", F.lit(0.0).cast(DecimalType(10, 2)))
            )

        # Financial Calculations (exact 2 decimal places)
        gross_expr = F.round(F.col("quantity") * F.col("unit_price"), 2).cast(DecimalType(10, 2))
        discount_expr = F.round(F.col("discount_amount"), 2).cast(DecimalType(10, 2))
        net_expr = F.round(gross_expr - discount_expr, 2).cast(DecimalType(10, 2))
        refund_expr = F.round(F.coalesce(F.col("refund_amount"), F.lit(0.0)), 2).cast(DecimalType(10, 2))

        fact_df = (
            joined
            .withColumn("gross_amount", gross_expr)
            .withColumn("net_amount", net_expr)
            .withColumn("refund_amount", refund_expr)
            .withColumn("is_returned", F.when(F.col("return_id").isNotNull(), F.lit(True)).otherwise(F.lit(False)))
            .withColumn("is_cancelled", F.when(F.col("order_status") == "CANCELLED", F.lit(True)).otherwise(F.lit(False)))
            .withColumn("year", F.year(F.col("order_date")).cast(IntegerType()))
            .withColumn("month", F.month(F.col("order_date")).cast(IntegerType()))
            .select(
                "order_item_id",
                "order_id",
                "customer_id",
                "product_id",
                "product_name",
                "category",
                "order_date",
                "order_status",
                "payment_method",
                "quantity",
                "unit_price",
                "gross_amount",
                "discount_amount",
                "net_amount",
                "is_returned",
                "return_id",
                "return_date",
                "return_reason",
                "refund_amount",
                "is_cancelled",
                "year",
                "month",
            )
        )
        return fact_df

    def build_dim_customers(
        self,
        customers_df: DataFrame,
        fact_order_items_df: DataFrame,
    ) -> DataFrame:
        """Constructs Customer Dimension enriched with lifetime order and spend metrics."""
        # Calculate metrics only on valid, non-cancelled orders
        valid_sales = fact_order_items_df.filter(F.col("is_cancelled") == False)

        cust_metrics = (
            valid_sales
            .groupBy("customer_id")
            .agg(
                F.min("order_date").alias("first_order_date"),
                F.max("order_date").alias("latest_order_date"),
                F.countDistinct("order_id").cast(IntegerType()).alias("total_orders"),
                F.sum("quantity").cast(IntegerType()).alias("lifetime_units_purchased"),
                F.round(F.sum("gross_amount"), 2).cast(DecimalType(12, 2)).alias("lifetime_gross_spend"),
                F.round(F.sum("net_amount"), 2).cast(DecimalType(12, 2)).alias("lifetime_net_spend"),
            )
        )

        return (
            customers_df
            .join(cust_metrics, on="customer_id", how="left")
            .withColumn("total_orders", F.coalesce(F.col("total_orders"), F.lit(0)))
            .withColumn("lifetime_units_purchased", F.coalesce(F.col("lifetime_units_purchased"), F.lit(0)))
            .withColumn("lifetime_gross_spend", F.coalesce(F.col("lifetime_gross_spend"), F.lit(0.0).cast(DecimalType(12, 2))))
            .withColumn("lifetime_net_spend", F.coalesce(F.col("lifetime_net_spend"), F.lit(0.0).cast(DecimalType(12, 2))))
            .select(
                "customer_id",
                "first_name",
                "last_name",
                "email",
                "city",
                "state",
                "signup_date",
                "customer_status",
                "first_order_date",
                "latest_order_date",
                "total_orders",
                "lifetime_units_purchased",
                "lifetime_gross_spend",
                "lifetime_net_spend",
                "updated_at",
            )
        )

    def build_agg_daily_sales(self, fact_order_items_df: DataFrame) -> DataFrame:
        """Aggregates daily sales performance by date and product category."""
        # Non-cancelled sales contribute to revenue
        valid_sales = fact_order_items_df.filter(F.col("is_cancelled") == False)

        return (
            valid_sales
            .groupBy("order_date", "category")
            .agg(
                F.countDistinct("order_id").cast(IntegerType()).alias("order_count"),
                F.sum("quantity").cast(IntegerType()).alias("units_sold"),
                F.round(F.sum("gross_amount"), 2).cast(DecimalType(12, 2)).alias("gross_sales"),
                F.round(F.sum("discount_amount"), 2).cast(DecimalType(12, 2)).alias("total_discounts"),
                F.round(F.sum("net_amount"), 2).cast(DecimalType(12, 2)).alias("net_sales"),
                F.round(F.sum("refund_amount"), 2).cast(DecimalType(12, 2)).alias("total_refunds"),
            )
            .withColumn("year", F.year(F.col("order_date")).cast(IntegerType()))
            .select(
                "order_date",
                "category",
                "order_count",
                "units_sold",
                "gross_sales",
                "total_discounts",
                "net_sales",
                "total_refunds",
                "year",
            )
        )

    def build_agg_customer_performance(self, dim_customers_df: DataFrame) -> DataFrame:
        """Builds Customer segmentation and RFM purchase frequency data mart."""
        return (
            dim_customers_df
            .filter(F.col("total_orders") > 0)
            .withColumn(
                "customer_tier",
                F.when(F.col("lifetime_net_spend") >= 1000, F.lit("VIP"))
                .when(F.col("lifetime_net_spend") >= 400, F.lit("REGULAR"))
                .otherwise(F.lit("OCCASIONAL")),
            )
            .select(
                "customer_id",
                "first_name",
                "last_name",
                "email",
                "state",
                "first_order_date",
                "latest_order_date",
                "total_orders",
                "lifetime_net_spend",
                "customer_tier",
            )
        )
