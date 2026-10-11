"""Data validation engine: business rules, schema integrity, and quarantine routing."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
)

from ecommerce_data_lake.logging_config import setup_logger
from ecommerce_data_lake.schemas import (
    ALLOWED_CUSTOMER_STATUSES,
    ALLOWED_ORDER_STATUSES,
    ALLOWED_PAYMENT_METHODS,
    ALLOWED_PRODUCT_STATUSES,
    ALLOWED_RETURN_REASONS,
    ALLOWED_RETURN_STATUSES,
    PRIMARY_KEYS,
    QUARANTINE_SCHEMA,
)

logger = setup_logger("ecommerce_data_lake.validation")

# Email validation regex (RFC 5322 standard pattern simplified)
EMAIL_REGEX = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"


@dataclass
class ValidationSummary:
    entity_name: str
    total_evaluated: int = 0
    passed_count: int = 0
    quarantine_count: int = 0
    rule_violations: Dict[str, int] = field(default_factory=dict)


class DataValidator:
    """Applies domain business rules, referential integrity checks, and separates clean vs quarantine data."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def _build_quarantine_records(
        self,
        df: DataFrame,
        entity_name: str,
        rejection_stage: str,
        violation_col: str,
        rule_code: str,
        reason_expr: str,
    ) -> DataFrame:
        """Filters rows failing a validation rule and projects them into the standard quarantine schema."""
        failed_df = df.filter(F.col(violation_col) == True)
        if failed_df.isEmpty():
            return self.spark.createDataFrame([], QUARANTINE_SCHEMA)

        now_ts = datetime.now(timezone.utc)
        pk_col = PRIMARY_KEYS.get(entity_name, "_source_record_id")

        # Exclude internal metadata from raw JSON dump for clean audit
        data_cols = [c for c in df.columns if not c.startswith("_") and not c.startswith("__")]

        quarantine_df = (
            failed_df
            .withColumn("quarantine_id", F.concat_ws("_", F.lit("Q"), F.monotonically_increasing_id().cast(StringType())))
            .withColumn("entity_name", F.lit(entity_name))
            .withColumn("source_record_id", F.col(pk_col).cast(StringType()))
            .withColumn("batch_id", F.col("_batch_id"))
            .withColumn("source_filename", F.col("_source_filename"))
            .withColumn("rejection_stage", F.lit(rejection_stage))
            .withColumn("rule_code", F.lit(rule_code))
            .withColumn("rejection_reason", F.expr(reason_expr))
            .withColumn("raw_record_json", F.to_json(F.struct(*[F.col(c) for c in data_cols])))
            .withColumn("quarantine_timestamp", F.to_timestamp(F.lit(now_ts.strftime("%Y-%m-%d %H:%M:%S"))))
            .select(
                "quarantine_id",
                "entity_name",
                "source_record_id",
                "batch_id",
                "source_filename",
                "rejection_stage",
                "rule_code",
                "rejection_reason",
                "raw_record_json",
                "quarantine_timestamp",
            )
        )
        return quarantine_df

    def _combine_quarantines(self, quarantine_list: List[DataFrame]) -> DataFrame:
        """Combines multiple quarantine DataFrames into one cleanly."""
        valid_q = [q for q in quarantine_list if not q.isEmpty()]
        if not valid_q:
            return self.spark.createDataFrame([], QUARANTINE_SCHEMA)
        combined = valid_q[0]
        for q in valid_q[1:]:
            combined = combined.unionByName(q)
        return combined

    def validate_customers(self, df: DataFrame) -> Tuple[DataFrame, DataFrame, ValidationSummary]:
        """Validates customer entities: non-null id/email/name, valid email regex, valid status."""
        total = df.count()
        summary = ValidationSummary(entity_name="customers", total_evaluated=total)
        quarantines: List[DataFrame] = []

        # Normalization
        clean_df = (
            df
            .withColumn("first_name", F.trim(F.col("first_name")))
            .withColumn("last_name", F.trim(F.col("last_name")))
            .withColumn("email", F.lower(F.trim(F.col("email"))))
            .withColumn("city", F.trim(F.col("city")))
            .withColumn("state", F.upper(F.trim(F.col("state"))))
            .withColumn("customer_status", F.upper(F.trim(F.col("customer_status"))))
        )

        # Rule 1: Non-null primary key and mandatory attributes
        null_cond = (
            F.col("customer_id").isNull()
            | (F.col("customer_id") == "")
            | F.col("email").isNull()
            | (F.col("email") == "")
            | F.col("first_name").isNull()
            | F.col("last_name").isNull()
        )
        q_null = self._build_quarantine_records(
            clean_df.withColumn("__null_violation", null_cond),
            "customers", "VALIDATION", "__null_violation", "CUST_NULL_ATTR",
            "'Missing mandatory attribute: customer_id, email, or name'"
        )
        if not q_null.isEmpty():
            quarantines.append(q_null)
            clean_df = clean_df.filter(~null_cond)

        # Rule 2: Email format validation
        email_invalid = ~F.col("email").rlike(EMAIL_REGEX)
        q_email = self._build_quarantine_records(
            clean_df.withColumn("__email_violation", email_invalid),
            "customers", "VALIDATION", "__email_violation", "CUST_INVALID_EMAIL",
            "concat('Invalid email format: ', email)"
        )
        if not q_email.isEmpty():
            quarantines.append(q_email)
            clean_df = clean_df.filter(~email_invalid)

        # Rule 3: Allowed customer status
        status_invalid = ~F.col("customer_status").isin(list(ALLOWED_CUSTOMER_STATUSES))
        q_status = self._build_quarantine_records(
            clean_df.withColumn("__status_violation", status_invalid),
            "customers", "VALIDATION", "__status_violation", "CUST_INVALID_STATUS",
            "concat('Disallowed customer status: ', customer_status)"
        )
        if not q_status.isEmpty():
            quarantines.append(q_status)
            clean_df = clean_df.filter(~status_invalid)

        quarantine_all = self._combine_quarantines(quarantines)
        passed = clean_df.count()
        q_count = total - passed
        summary.passed_count = passed
        summary.quarantine_count = q_count
        logger.info(f"Customers validated: {total} total, {passed} passed, {q_count} quarantined")
        return clean_df, quarantine_all, summary

    def validate_products(self, df: DataFrame) -> Tuple[DataFrame, DataFrame, ValidationSummary]:
        """Validates product entities: non-null id/name, unit_price >= 0, valid status."""
        total = df.count()
        summary = ValidationSummary(entity_name="products", total_evaluated=total)
        quarantines: List[DataFrame] = []

        clean_df = (
            df
            .withColumn("product_name", F.trim(F.col("product_name")))
            .withColumn("category", F.trim(F.col("category")))
            .withColumn("product_status", F.upper(F.trim(F.col("product_status"))))
            .withColumn("unit_price_num", F.col("unit_price").cast(DoubleType()))
        )

        # Rule 1: Non-null primary key and product name
        null_cond = (
            F.col("product_id").isNull()
            | (F.col("product_id") == "")
            | F.col("product_name").isNull()
            | (F.col("product_name") == "")
        )
        q_null = self._build_quarantine_records(
            clean_df.withColumn("__null_violation", null_cond),
            "products", "VALIDATION", "__null_violation", "PROD_NULL_ATTR",
            "'Missing mandatory attribute: product_id or product_name'"
        )
        if not q_null.isEmpty():
            quarantines.append(q_null)
            clean_df = clean_df.filter(~null_cond)

        # Rule 2: Price must be non-negative
        price_invalid = F.col("unit_price_num").isNull() | (F.col("unit_price_num") < 0)
        q_price = self._build_quarantine_records(
            clean_df.withColumn("__price_violation", price_invalid),
            "products", "VALIDATION", "__price_violation", "PROD_INVALID_PRICE",
            "concat('Unit price must be non-negative, got: ', coalesce(unit_price, 'null'))"
        )
        if not q_price.isEmpty():
            quarantines.append(q_price)
            clean_df = clean_df.filter(~price_invalid)

        # Rule 3: Valid status
        status_invalid = ~F.col("product_status").isin(list(ALLOWED_PRODUCT_STATUSES))
        q_status = self._build_quarantine_records(
            clean_df.withColumn("__status_violation", status_invalid),
            "products", "VALIDATION", "__status_violation", "PROD_INVALID_STATUS",
            "concat('Disallowed product status: ', product_status)"
        )
        if not q_status.isEmpty():
            quarantines.append(q_status)
            clean_df = clean_df.filter(~status_invalid)

        quarantine_all = self._combine_quarantines(quarantines)
        clean_df = clean_df.drop("unit_price_num")
        passed = clean_df.count()
        q_count = total - passed
        summary.passed_count = passed
        summary.quarantine_count = q_count
        logger.info(f"Products validated: {total} total, {passed} passed, {q_count} quarantined")
        return clean_df, quarantine_all, summary

    def validate_orders(
        self,
        orders_df: DataFrame,
        valid_customers_df: DataFrame,
    ) -> Tuple[DataFrame, DataFrame, ValidationSummary]:
        """Validates orders: non-null id/customer, valid status/payment, and referential integrity to customers."""
        total = orders_df.count()
        summary = ValidationSummary(entity_name="orders", total_evaluated=total)
        quarantines: List[DataFrame] = []

        clean_df = (
            orders_df
            .withColumn("order_status", F.upper(F.trim(F.col("order_status"))))
            .withColumn("payment_method", F.upper(F.trim(F.col("payment_method"))))
        )

        # Rule 1: Non-null primary key and customer_id
        null_cond = (
            F.col("order_id").isNull()
            | (F.col("order_id") == "")
            | F.col("customer_id").isNull()
            | (F.col("customer_id") == "")
        )
        q_null = self._build_quarantine_records(
            clean_df.withColumn("__null_violation", null_cond),
            "orders", "VALIDATION", "__null_violation", "ORD_NULL_ATTR",
            "'Missing mandatory attribute: order_id or customer_id'"
        )
        if not q_null.isEmpty():
            quarantines.append(q_null)
            clean_df = clean_df.filter(~null_cond)

        # Rule 2: Allowed status and payment method
        status_invalid = ~F.col("order_status").isin(list(ALLOWED_ORDER_STATUSES))
        pay_invalid = ~F.col("payment_method").isin(list(ALLOWED_PAYMENT_METHODS))
        domain_invalid = status_invalid | pay_invalid
        q_domain = self._build_quarantine_records(
            clean_df.withColumn("__domain_violation", domain_invalid),
            "orders", "VALIDATION", "__domain_violation", "ORD_INVALID_DOMAIN",
            "concat('Invalid order status (', order_status, ') or payment method (', payment_method, ')')"
        )
        if not q_domain.isEmpty():
            quarantines.append(q_domain)
            clean_df = clean_df.filter(~domain_invalid)

        # Rule 3: Referential Integrity - Customer Foreign Key
        valid_cust_ids = valid_customers_df.select("customer_id").distinct()
        joined_cust = clean_df.join(
            valid_cust_ids.withColumn("__cust_exists", F.lit(True)),
            on="customer_id",
            how="left",
        )
        orphan_orders = joined_cust.filter(F.col("__cust_exists").isNull())
        q_orphan = self._build_quarantine_records(
            orphan_orders.withColumn("__orphan", F.lit(True)),
            "orders", "INTEGRITY", "__orphan", "ORD_ORPHAN_CUSTOMER",
            "concat('Order references nonexistent customer_id: ', customer_id)"
        )
        if not q_orphan.isEmpty():
            quarantines.append(q_orphan)
            clean_df = joined_cust.filter(F.col("__cust_exists") == True).drop("__cust_exists")

        quarantine_all = self._combine_quarantines(quarantines)
        passed = clean_df.count()
        q_count = total - passed
        summary.passed_count = passed
        summary.quarantine_count = q_count
        logger.info(f"Orders validated: {total} total, {passed} passed, {q_count} quarantined")
        return clean_df, quarantine_all, summary

    def validate_order_items(
        self,
        items_df: DataFrame,
        valid_orders_df: DataFrame,
        valid_products_df: DataFrame,
    ) -> Tuple[DataFrame, DataFrame, ValidationSummary]:
        """Validates order items: quantity > 0, price >= 0, discount <= gross, and foreign keys."""
        total = items_df.count()
        summary = ValidationSummary(entity_name="order_items", total_evaluated=total)
        quarantines: List[DataFrame] = []

        clean_df = (
            items_df
            .withColumn("quantity_num", F.col("quantity").cast(IntegerType()))
            .withColumn("unit_price_num", F.col("unit_price").cast(DoubleType()))
            .withColumn("discount_amount_num", F.coalesce(F.col("discount_amount").cast(DoubleType()), F.lit(0.0)))
        )

        # Rule 1: Quantity must be strictly positive
        qty_invalid = F.col("quantity_num").isNull() | (F.col("quantity_num") <= 0)
        q_qty = self._build_quarantine_records(
            clean_df.withColumn("__qty_violation", qty_invalid),
            "order_items", "VALIDATION", "__qty_violation", "ITEM_INVALID_QTY",
            "concat('Quantity must be positive integer, got: ', coalesce(quantity, 'null'))"
        )
        if not q_qty.isEmpty():
            quarantines.append(q_qty)
            clean_df = clean_df.filter(~qty_invalid)

        # Rule 2: Unit price non-negative
        price_invalid = F.col("unit_price_num").isNull() | (F.col("unit_price_num") < 0)
        q_price = self._build_quarantine_records(
            clean_df.withColumn("__price_violation", price_invalid),
            "order_items", "VALIDATION", "__price_violation", "ITEM_INVALID_PRICE",
            "concat('Unit price must be non-negative, got: ', coalesce(unit_price, 'null'))"
        )
        if not q_price.isEmpty():
            quarantines.append(q_price)
            clean_df = clean_df.filter(~price_invalid)

        # Rule 3: Discount amount constraint (0 <= discount <= unit_price * quantity)
        gross_expr = F.col("unit_price_num") * F.col("quantity_num")
        discount_invalid = (
            (F.col("discount_amount_num") < 0)
            | (F.col("discount_amount_num") > gross_expr)
        )
        q_discount = self._build_quarantine_records(
            clean_df.withColumn("__discount_violation", discount_invalid),
            "order_items", "VALIDATION", "__discount_violation", "ITEM_EXCESSIVE_DISCOUNT",
            "concat('Discount exceeds gross total. Discount: ', discount_amount, ', gross: ', cast(unit_price_num * quantity_num as string))"
        )
        if not q_discount.isEmpty():
            quarantines.append(q_discount)
            clean_df = clean_df.filter(~discount_invalid)

        # Rule 4: Referential Integrity - Order Foreign Key
        valid_order_ids = valid_orders_df.select("order_id").distinct()
        joined_ord = clean_df.join(
            valid_order_ids.withColumn("__order_exists", F.lit(True)),
            on="order_id",
            how="left",
        )
        orphan_items_ord = joined_ord.filter(F.col("__order_exists").isNull())
        q_orphan_ord = self._build_quarantine_records(
            orphan_items_ord.withColumn("__orphan", F.lit(True)),
            "order_items", "INTEGRITY", "__orphan", "ITEM_ORPHAN_ORDER",
            "concat('Order item references nonexistent order_id: ', order_id)"
        )
        if not q_orphan_ord.isEmpty():
            quarantines.append(q_orphan_ord)
            clean_df = joined_ord.filter(F.col("__order_exists") == True).drop("__order_exists")

        # Rule 5: Referential Integrity - Product Foreign Key
        valid_prod_ids = valid_products_df.select("product_id").distinct()
        joined_prod = clean_df.join(
            valid_prod_ids.withColumn("__prod_exists", F.lit(True)),
            on="product_id",
            how="left",
        )
        orphan_items_prod = joined_prod.filter(F.col("__prod_exists").isNull())
        q_orphan_prod = self._build_quarantine_records(
            orphan_items_prod.withColumn("__orphan", F.lit(True)),
            "order_items", "INTEGRITY", "__orphan", "ITEM_ORPHAN_PRODUCT",
            "concat('Order item references nonexistent product_id: ', product_id)"
        )
        if not q_orphan_prod.isEmpty():
            quarantines.append(q_orphan_prod)
            clean_df = joined_prod.filter(F.col("__prod_exists") == True).drop("__prod_exists")

        quarantine_all = self._combine_quarantines(quarantines)
        clean_df = clean_df.drop("quantity_num", "unit_price_num", "discount_amount_num")
        passed = clean_df.count()
        q_count = total - passed
        summary.passed_count = passed
        summary.quarantine_count = q_count
        logger.info(f"Order Items validated: {total} total, {passed} passed, {q_count} quarantined")
        return clean_df, quarantine_all, summary

    def validate_returns(
        self,
        returns_df: DataFrame,
        valid_items_df: DataFrame,
    ) -> Tuple[DataFrame, DataFrame, ValidationSummary]:
        """Validates returns: refund >= 0, valid status/reason, and foreign key to order_items."""
        total = returns_df.count()
        summary = ValidationSummary(entity_name="returns", total_evaluated=total)
        quarantines: List[DataFrame] = []

        clean_df = (
            returns_df
            .withColumn("return_status", F.upper(F.trim(F.col("return_status"))))
            .withColumn("return_reason", F.upper(F.trim(F.col("return_reason"))))
            .withColumn("refund_amount_num", F.col("refund_amount").cast(DoubleType()))
        )

        # Rule 1: Non-null keys
        null_cond = (
            F.col("return_id").isNull()
            | (F.col("return_id") == "")
            | F.col("order_item_id").isNull()
            | (F.col("order_item_id") == "")
        )
        q_null = self._build_quarantine_records(
            clean_df.withColumn("__null_violation", null_cond),
            "returns", "VALIDATION", "__null_violation", "RET_NULL_ATTR",
            "'Missing mandatory attribute: return_id or order_item_id'"
        )
        if not q_null.isEmpty():
            quarantines.append(q_null)
            clean_df = clean_df.filter(~null_cond)

        # Rule 2: Refund must be non-negative
        refund_invalid = F.col("refund_amount_num").isNull() | (F.col("refund_amount_num") < 0)
        q_refund = self._build_quarantine_records(
            clean_df.withColumn("__refund_violation", refund_invalid),
            "returns", "VALIDATION", "__refund_violation", "RET_INVALID_REFUND",
            "concat('Refund amount must be non-negative, got: ', coalesce(refund_amount, 'null'))"
        )
        if not q_refund.isEmpty():
            quarantines.append(q_refund)
            clean_df = clean_df.filter(~refund_invalid)

        # Rule 3: Valid status and reason
        domain_invalid = (
            ~F.col("return_status").isin(list(ALLOWED_RETURN_STATUSES))
            | ~F.col("return_reason").isin(list(ALLOWED_RETURN_REASONS))
        )
        q_domain = self._build_quarantine_records(
            clean_df.withColumn("__domain_violation", domain_invalid),
            "returns", "VALIDATION", "__domain_violation", "RET_INVALID_DOMAIN",
            "concat('Invalid return status (', return_status, ') or reason (', return_reason, ')')"
        )
        if not q_domain.isEmpty():
            quarantines.append(q_domain)
            clean_df = clean_df.filter(~domain_invalid)

        # Rule 4: Referential integrity to order_item
        valid_item_ids = valid_items_df.select("order_item_id").distinct()
        joined_items = clean_df.join(
            valid_item_ids.withColumn("__item_exists", F.lit(True)),
            on="order_item_id",
            how="left",
        )
        orphan_returns = joined_items.filter(F.col("__item_exists").isNull())
        q_orphan = self._build_quarantine_records(
            orphan_returns.withColumn("__orphan", F.lit(True)),
            "returns", "INTEGRITY", "__orphan", "RET_ORPHAN_ITEM",
            "concat('Return references nonexistent order_item_id: ', order_item_id)"
        )
        if not q_orphan.isEmpty():
            quarantines.append(q_orphan)
            clean_df = joined_items.filter(F.col("__item_exists") == True).drop("__item_exists")

        quarantine_all = self._combine_quarantines(quarantines)
        clean_df = clean_df.drop("refund_amount_num")
        passed = clean_df.count()
        q_count = total - passed
        summary.passed_count = passed
        summary.quarantine_count = q_count
        logger.info(f"Returns validated: {total} total, {passed} passed, {q_count} quarantined")
        return clean_df, quarantine_all, summary
