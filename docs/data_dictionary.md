# E-Commerce Data Dictionary

## 1. Source Entities

### Customers
| Column | Type | Nullable | Description | Business Rules / Constraints |
| :--- | :--- | :--- | :--- | :--- |
| `customer_id` | String | No | Unique Customer identifier | Primary key; format: `CUST-XXXXXX` |
| `first_name` | String | No | Customer given name | Trimmed, non-empty |
| `last_name` | String | No | Customer family name | Trimmed, non-empty |
| `email` | String | No | Contact email address | Lowercased, valid RFC 5322 regex format |
| `city` | String | No | Customer billing/shipping city | Trimmed |
| `state` | String | No | Two-letter US state code | Uppercase 2-letter state |
| `signup_date` | Date | No | Customer registration date | Format `YYYY-MM-DD` |
| `customer_status` | String | No | Account status | Allowed: `ACTIVE`, `INACTIVE`, `SUSPENDED` |
| `updated_at` | Timestamp | No | Last update timestamp | Order tie-breaker for deduplication |

### Products
| Column | Type | Nullable | Description | Business Rules / Constraints |
| :--- | :--- | :--- | :--- | :--- |
| `product_id` | String | No | Unique Product identifier | Primary key; format: `PROD-XXXXX` |
| `product_name` | String | No | Product title | Trimmed, non-empty |
| `category` | String | No | Merchandise category | Standardized category list |
| `unit_price` | Decimal(10,2) | No | List price in USD | Must be strictly non-negative (`>= 0.00`) |
| `product_status` | String | No | Product availability status | Allowed: `ACTIVE`, `DISCONTINUED`, `OUT_OF_STOCK` |
| `updated_at` | Timestamp | No | Last modified timestamp | Order tie-breaker for deduplication |

### Orders
| Column | Type | Nullable | Description | Business Rules / Constraints |
| :--- | :--- | :--- | :--- | :--- |
| `order_id` | String | No | Unique Order header identifier | Primary key; format: `ORD-XXXXXXX` |
| `customer_id` | String | No | Foreign key to Customers | Must exist in validated `customers` table |
| `order_date` | Date | No | Date order was placed | Must be `>= customer.signup_date` |
| `order_status` | String | No | Fulfillment lifecycle status | Allowed: `PENDING`, `PROCESSING`, `SHIPPED`, `DELIVERED`, `CANCELLED`, `RETURNED` |
| `payment_method` | String | No | Tender type | Allowed: `CREDIT_CARD`, `DEBIT_CARD`, `PAYPAL`, `APPLE_PAY`, `BANK_TRANSFER` |
| `updated_at` | Timestamp | No | Last order status update | Deduplication ordering key |

### Order Items
| Column | Type | Nullable | Description | Business Rules / Constraints |
| :--- | :--- | :--- | :--- | :--- |
| `order_item_id` | String | No | Unique line item identifier | Primary key; format: `ITEM-XXXXXXXX` |
| `order_id` | String | No | Foreign key to Orders | Must exist in validated `orders` table |
| `product_id` | String | No | Foreign key to Products | Must exist in validated `products` table |
| `quantity` | Integer | No | Count of units ordered | Must be strictly positive integer (`> 0`) |
| `unit_price` | Decimal(10,2) | No | Price charged per unit | Must be `>= 0.00` |
| `discount_amount` | Decimal(10,2) | No | Line item promotional discount | `0.00 <= discount <= unit_price * quantity` |
| `updated_at` | Timestamp | No | Record update timestamp | Deduplication tie-breaker |

### Returns
| Column | Type | Nullable | Description | Business Rules / Constraints |
| :--- | :--- | :--- | :--- | :--- |
| `return_id` | String | No | Unique return transaction id | Primary key; format: `RET-XXXXXXX` |
| `order_item_id` | String | No | Foreign key to Order Items | Must reference an existing line item |
| `return_date` | Date | No | Date return was requested | Must be `>= order_date` |
| `return_reason` | String | No | Customer return justification | Allowed: `DEFECTIVE`, `WRONG_ITEM`, `NOT_AS_DESCRIBED`, `CHANGED_MIND`, `LATE_DELIVERY` |
| `refund_amount` | Decimal(10,2) | No | Cash or credit refund value | `0.00 <= refund <= gross_amount - discount` |
| `return_status` | String | No | Return lifecycle status | Allowed: `REQUESTED`, `APPROVED`, `COMPLETED`, `REJECTED` |
| `updated_at` | Timestamp | No | Return update timestamp | Deduplication tie-breaker |

---

## 2. Gold Layer Analytical Schemas

### `fact_order_items`
- **Grain:** 1 row per order item.
- **Partitioning:** `year: int`, `month: int`.
- **Key Columns:** `order_item_id`, `order_id`, `customer_id`, `product_id`, `product_name`, `category`, `order_date`, `order_status`, `payment_method`, `quantity`, `unit_price`, `gross_amount`, `discount_amount`, `net_amount`, `is_returned`, `return_id`, `return_date`, `return_reason`, `refund_amount`, `is_cancelled`, `year`, `month`.

### `dim_customers`
- **Grain:** 1 row per customer.
- **Columns:** `customer_id`, `first_name`, `last_name`, `email`, `city`, `state`, `signup_date`, `customer_status`, `first_order_date`, `latest_order_date`, `total_orders`, `lifetime_units_purchased`, `lifetime_gross_spend`, `lifetime_net_spend`, `updated_at`.

### `dim_products`
- **Grain:** 1 row per product.
- **Columns:** `product_id`, `product_name`, `category`, `unit_price`, `product_status`, `total_units_sold`, `total_revenue`, `updated_at`.

### `dim_dates`
- **Grain:** 1 row per calendar day.
- **Columns:** `date_key`, `full_date`, `year`, `quarter`, `month`, `month_name`, `day`, `day_of_week`, `day_name`, `is_weekend`.

### `agg_daily_sales`
- **Grain:** `order_date`, `category`.
- **Partitioning:** `year: int`.
- **Columns:** `order_date`, `category`, `order_count`, `units_sold`, `gross_sales`, `total_discounts`, `net_sales`, `total_refunds`, `year`.
