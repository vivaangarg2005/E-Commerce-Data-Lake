"""Deterministic synthetic e-commerce dataset generator for data lake pipelines."""

import csv
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from ecommerce_data_lake.config import DatasetProfile
from ecommerce_data_lake.schemas import (
    ALLOWED_PAYMENT_METHODS,
    ALLOWED_RETURN_REASONS,
    VALID_CATEGORIES,
)

# Reference sample data pools
FIRST_NAMES = [
    "James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael", "Linda",
    "David", "Elizabeth", "William", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
    "Thomas", "Sarah", "Charles", "Karen", "Christopher", "Nancy", "Daniel", "Lisa",
    "Matthew", "Betty", "Anthony", "Margaret", "Mark", "Sandra", "Donald", "Ashley",
    "Steven", "Kimberly", "Paul", "Emily", "Andrew", "Donna", "Joshua", "Michelle",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
    "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker",
]

CITIES_STATES = [
    ("New York", "NY"), ("Los Angeles", "CA"), ("Chicago", "IL"), ("Houston", "TX"),
    ("Phoenix", "AZ"), ("Philadelphia", "PA"), ("San Antonio", "TX"), ("San Diego", "CA"),
    ("Dallas", "TX"), ("San Jose", "CA"), ("Austin", "TX"), ("Jacksonville", "FL"),
    ("Fort Worth", "TX"), ("Columbus", "OH"), ("Charlotte", "NC"), ("San Francisco", "CA"),
    ("Indianapolis", "IN"), ("Seattle", "WA"), ("Denver", "CO"), ("Boston", "MA"),
]

PRODUCT_TEMPLATES = {
    "Electronics": ["Wireless Headphones", "Smart Watch", "4K Monitor", "USB-C Hub", "Mechanical Keyboard", "Bluetooth Speaker"],
    "Clothing": ["Organic Cotton T-Shirt", "Slim Fit Denim Jeans", "Windbreaker Jacket", "Merino Wool Sweater", "Athletic Shorts"],
    "Home & Kitchen": ["Stainless Steel Cookware", "Espresso Machine", "Ceramic Dinner Plates", "Cast Iron Skillet", "Air Fryer"],
    "Books": ["Data Engineering Handbook", "Designing Data-Intensive Apps", "Python Mastery Guide", "Cloud Architecture Blueprint"],
    "Beauty & Health": ["Hydrating Face Serum", "Mineral Sunscreen SPF 50", "Bamboo Toothbrush Set", "Organic Lip Balm"],
    "Sports & Outdoors": ["Yoga Mat Non-Slip", "Insulated Water Bottle", "Camping Tent 2-Person", "Resistance Bands Set"],
    "Toys & Games": ["Strategy Board Game", "Building Block Robotics", "Wooden Puzzle Set", "Card Game Expansion"],
}


class SyntheticDataGenerator:
    """Generates deterministic, referentially-consistent e-commerce transactional data."""

    def __init__(self, seed: int = 42) -> None:
        self.seed = seed
        self.rng = random.Random(seed)
        self.base_date = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

    def generate_batch(
        self,
        profile: DatasetProfile,
        batch_id: str = "batch_001",
        is_incremental: bool = False,
        existing_customers: Optional[List[Dict[str, Any]]] = None,
        existing_products: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Generates a complete relational batch of e-commerce entities."""
        customers: List[Dict[str, Any]] = []
        products: List[Dict[str, Any]] = []
        orders: List[Dict[str, Any]] = []
        order_items: List[Dict[str, Any]] = []
        returns: List[Dict[str, Any]] = []

        # 1. Customers
        if not is_incremental or not existing_customers:
            cust_count = profile.customers
            start_idx = 1
        else:
            cust_count = max(5, int(profile.customers * 0.15))
            start_idx = len(existing_customers) + 1

        for i in range(cust_count):
            c_id = f"CUST-{start_idx + i:06d}"
            fn = self.rng.choice(FIRST_NAMES)
            ln = self.rng.choice(LAST_NAMES)
            city, state = self.rng.choice(CITIES_STATES)
            days_ago = self.rng.randint(60, 365) if not is_incremental else self.rng.randint(1, 45)
            signup = (self.base_date - timedelta(days=days_ago)).date().isoformat()
            status = self.rng.choices(["ACTIVE", "INACTIVE", "SUSPENDED"], weights=[0.85, 0.12, 0.03])[0]
            updated_at = (self.base_date - timedelta(days=days_ago, hours=self.rng.randint(0, 12))).strftime("%Y-%m-%d %H:%M:%S")

            email = f"{fn.lower()}.{ln.lower()}{self.rng.randint(10, 999)}@example.com"
            customers.append({
                "customer_id": c_id,
                "first_name": fn,
                "last_name": ln,
                "email": email,
                "city": city,
                "state": state,
                "signup_date": signup,
                "customer_status": status,
                "updated_at": updated_at,
            })

        all_customers = (existing_customers or []) + customers

        # 2. Products
        if not is_incremental or not existing_products:
            prod_count = profile.products
            start_p_idx = 1
            for i in range(prod_count):
                p_id = f"PROD-{start_p_idx + i:05d}"
                cat = self.rng.choice(VALID_CATEGORIES)
                template = self.rng.choice(PRODUCT_TEMPLATES.get(cat, ["Specialty Product"]))
                prod_name = f"{template} v{self.rng.randint(1, 5)}"
                price = round(self.rng.uniform(9.99, 499.99), 2)
                p_status = self.rng.choices(["ACTIVE", "DISCONTINUED", "OUT_OF_STOCK"], weights=[0.90, 0.05, 0.05])[0]
                updated_at = self.base_date.strftime("%Y-%m-%d %H:%M:%S")

                products.append({
                    "product_id": p_id,
                    "product_name": prod_name,
                    "category": cat,
                    "unit_price": f"{price:.2f}",
                    "product_status": p_status,
                    "updated_at": updated_at,
                })
        else:
            products = []

        all_products = (existing_products or []) + products

        # 3. Orders and Order Items
        order_count = profile.orders if not is_incremental else max(10, int(profile.orders * 0.25))
        order_start_idx = 1 if not is_incremental else (100000 + self.rng.randint(1, 999))
        item_start_idx = 1 if not is_incremental else (500000 + self.rng.randint(1, 999))

        for o_i in range(order_count):
            o_id = f"ORD-{order_start_idx + o_i:07d}"
            cust = self.rng.choice(all_customers)
            c_id = cust["customer_id"]

            # Order date between customer signup and current simulation date
            cust_signup = datetime.strptime(cust["signup_date"], "%Y-%m-%d").replace(tzinfo=timezone.utc)
            max_days = max(1, (self.base_date - cust_signup).days)
            order_days = self.rng.randint(0, max_days)
            o_dt = cust_signup + timedelta(days=order_days, hours=self.rng.randint(1, 23), minutes=self.rng.randint(0, 59))
            o_date_str = o_dt.date().isoformat()
            o_updated = (o_dt + timedelta(hours=self.rng.randint(2, 48))).strftime("%Y-%m-%d %H:%M:%S")

            o_status = self.rng.choices(
                ["DELIVERED", "SHIPPED", "PROCESSING", "PENDING", "CANCELLED", "RETURNED"],
                weights=[0.60, 0.15, 0.08, 0.05, 0.07, 0.05],
            )[0]
            pay_method = self.rng.choice(list(ALLOWED_PAYMENT_METHODS))

            orders.append({
                "order_id": o_id,
                "customer_id": c_id,
                "order_date": o_date_str,
                "order_status": o_status,
                "payment_method": pay_method,
                "updated_at": o_updated,
            })

            # Generate 1 to 4 items per order
            num_items = self.rng.randint(1, 4)
            chosen_prods = self.rng.sample(all_products, min(num_items, len(all_products)))

            for p in chosen_prods:
                item_id = f"ITEM-{item_start_idx:08d}"
                item_start_idx += 1
                qty = self.rng.randint(1, 5)
                u_price = float(p["unit_price"])
                # Discount up to 20%
                has_discount = self.rng.random() < 0.25
                discount = round(self.rng.uniform(1.0, u_price * qty * 0.2), 2) if has_discount else 0.00

                order_items.append({
                    "order_item_id": item_id,
                    "order_id": o_id,
                    "product_id": p["product_id"],
                    "quantity": qty,
                    "unit_price": f"{u_price:.2f}",
                    "discount_amount": f"{discount:.2f}",
                    "updated_at": o_updated,
                })

                # 4. Returns (Only on delivered/returned orders)
                if o_status in ("DELIVERED", "RETURNED") and self.rng.random() < profile.returns_rate:
                    ret_id = f"RET-{len(returns) + 1:07d}"
                    ret_days = self.rng.randint(1, 14)
                    ret_dt = o_dt + timedelta(days=ret_days)
                    refund = round(u_price * qty - discount, 2)
                    returns.append({
                        "return_id": ret_id,
                        "order_item_id": item_id,
                        "return_date": ret_dt.date().isoformat(),
                        "return_reason": self.rng.choice(list(ALLOWED_RETURN_REASONS)),
                        "refund_amount": f"{refund:.2f}",
                        "return_status": self.rng.choices(["COMPLETED", "APPROVED", "REJECTED"], weights=[0.8, 0.1, 0.1])[0],
                        "updated_at": (ret_dt + timedelta(hours=6)).strftime("%Y-%m-%d %H:%M:%S"),
                    })

        # 5. Inject Controlled Duplicates (Deterministic deduplication testing)
        if profile.duplicate_rate > 0 and len(orders) > 10:
            dup_count = max(1, int(len(orders) * profile.duplicate_rate))
            for _ in range(dup_count):
                src_order = self.rng.choice(orders)
                # Create duplicate with an updated timestamp (e.g. status transition)
                dup_order = dict(src_order)
                old_time = datetime.strptime(src_order["updated_at"], "%Y-%m-%d %H:%M:%S")
                new_time = old_time + timedelta(hours=self.rng.randint(1, 24))
                dup_order["updated_at"] = new_time.strftime("%Y-%m-%d %H:%M:%S")
                # Status progression
                if dup_order["order_status"] == "PENDING":
                    dup_order["order_status"] = "PROCESSING"
                elif dup_order["order_status"] == "PROCESSING":
                    dup_order["order_status"] = "SHIPPED"
                orders.append(dup_order)

        # 6. Inject Controlled Anomalies (For Data Quality & Quarantine Verification)
        if profile.anomaly_rate > 0:
            # Anomaly A: Invalid negative quantity in order_items
            if order_items:
                bad_item = dict(order_items[-1])
                bad_item["order_item_id"] = f"ITEM-ERR-{self.rng.randint(1000, 9999)}"
                bad_item["quantity"] = -2
                order_items.append(bad_item)

            # Anomaly B: Malformed email in customer
            if customers:
                bad_cust = dict(customers[-1])
                bad_cust["customer_id"] = f"CUST-ERR-{self.rng.randint(1000, 9999)}"
                bad_cust["email"] = "not-a-valid-email-format"
                customers.append(bad_cust)

            # Anomaly C: Orphaned foreign key (order referencing nonexistent customer)
            if orders:
                orphan_order = dict(orders[-1])
                orphan_order["order_id"] = f"ORD-ERR-{self.rng.randint(1000, 9999)}"
                orphan_order["customer_id"] = "CUST-NONEXISTENT-999999"
                orders.append(orphan_order)

            # Anomaly D: Discount greater than gross total
            if len(order_items) > 1:
                bad_discount_item = dict(order_items[-2])
                bad_discount_item["order_item_id"] = f"ITEM-ERR-{self.rng.randint(1000, 9999)}"
                bad_discount_item["quantity"] = 1
                bad_discount_item["unit_price"] = "20.00"
                bad_discount_item["discount_amount"] = "99.00"  # Exceeds total
                order_items.append(bad_discount_item)

        return {
            "customers": customers,
            "products": products if not is_incremental else all_products,
            "orders": orders,
            "order_items": order_items,
            "returns": returns,
        }

    def write_batch_to_csv(
        self,
        batch_data: Dict[str, List[Dict[str, Any]]],
        output_dir: Path,
        batch_id: str,
    ) -> Dict[str, Path]:
        """Saves generated batch datasets as clean CSV files."""
        batch_dir = output_dir / batch_id
        batch_dir.mkdir(parents=True, exist_ok=True)
        file_paths: Dict[str, Path] = {}

        for entity_name, records in batch_data.items():
            if not records:
                continue
            file_path = batch_dir / f"{entity_name}.csv"
            keys = list(records[0].keys())
            with open(file_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=keys)
                writer.writeheader()
                writer.writerows(records)
            file_paths[entity_name] = file_path

        return file_paths
