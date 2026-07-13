# DataOne Data Dictionary & Schema Contract

This document outlines the structure, purpose, and schemas of the datasets progressing through the DataOne Lakehouse architecture.

## 🗃️ Metadata Layer & Data Contracts

The platform features a first-class, dynamic Metadata Layer under the `metadata/` directory. Metadata is partitioned into:
- **`metadata/datasets/`**: Basic dataset identifiers, descriptions, classification, and medallion layer.
- **`metadata/ownership/`**: Domain, owner, steward, retention policy, SLA, and consumer list.
- **`metadata/contracts/`**: Concrete schema definition (columns, types, nullability), primary/business keys, partition strategy, and sort order.
- **`metadata/quality/`**: Expected required columns, value boundaries, and custom validations.
- **`metadata/lineage/`**: Static upstream and downstream data flow mappings.

### Data Contract Enforcement & Evolution
Every batch and streaming run validates incoming PySpark DataFrames against these contracts before saving or processing data:
1. **Schema Evolution Policies**: Evolution is checked according to contract configuration (`allowed_schema_evolution`). For example, under `backward` evolution, adding new nullable columns is allowed, but deleting columns or modifying types is strictly blocked.
2. **Type Safety & Constraints**: Columns are checked against their declared types, and primary key uniqueness and nullability constraints are dynamically validated.
3. **Data Quality Quarantine**: Non-conforming rows failing null checks or value boundaries are routed to `quarantine.*` tables (along with a `_quarantine_reason` tag) for audit and triage, while valid data continues downstream.


## 🗂️ Data Lineage & Layers

### 1. Raw / Source Data
- **Postgres (OLTP)**: `customers`, `orders`, `order_items`, `products`.
- **MongoDB**: `reviews` (document-based product feedback).
- **Staged Files**: `campaigns.csv` (Simulated marketing tool exports).
- **Kafka**: 
  - `orders-cdc`: Real-time change events from Postgres.
  - `clickstream`: High-volume user behavioral events.

### 2. Bronze Layer (Landing)
The initial insertion point in the Iceberg lakehouse. Data is append-only, preserving its raw format.
- `bronze.orders_cdc`
- `bronze.clickstream`
- `bronze.campaigns`
- `bronze.reviews`
- `bronze.dead_letters` (Unparseable or malformed raw events)

### 3. Silver Layer (Curated & Validated)
Data is cleansed, joined, and standardized. Invalid records are routed to the `quarantine` layer via PySpark Quality Gates. Silver holds **only cleansed 3NF entities** — the Kimball star schema (facts and dimensions) lives entirely in the Gold layer (see `src/dataone/utils/schemas.py`).
- `silver.reviews` (Deduplicated with sentiment scoring)
- `silver.customers` (Curated latest-state customers)
- `silver.orders` (Curated deduplicated orders)
- `silver.clickstream` (Curated structured click events)
### 4. Gold Layer (Star Schema + Business Analytics Marts)
The complete Kimball star schema plus aggregated, highly optimized marts directly served to ClickHouse/Grafana.
- Star schema: `gold.fact_order_items`, `gold.dim_customer` (SCD Type 2), `gold.dim_product`, `gold.dim_campaign`, `gold.dim_date`.
- Marts: `gold.daily_sales`, `gold.top_products`, `gold.customer_segments`, `gold.conversion_rate`, `gold.campaign_effectiveness`, `gold.product_sentiment`, `gold.customer_clv`, `gold.funnel_conversion`, `gold.roas`, `gold.quarantine_summary`, `gold.quality_gate_summary`.

---

## 🏗️ Core Star-Schema Tables (Gold)

### `gold.fact_order_items`
Partitioned By: `days(order_date)`, `bucket(16, customer_id)`
| Column Name | Data Type | Description |
|---|---|---|
| `sk_order_id` | `STRING` | Surrogate Key |
| `order_id` | `BIGINT` | Unique Order Identifier |
| `customer_id` | `BIGINT` | Customer Identifier |
| `order_date` | `TIMESTAMP` | Time of placement |
| `date_key` | `INT` | FK to `gold.dim_date` (YYYYMMDD) |
| `status` | `STRING` | Order Status |
| `product_id` | `BIGINT` | Product Identifier |
| `sk_product_id` | `STRING` | FK to `gold.dim_product` |
| `quantity` | `BIGINT` | Quantity Ordered |
| `unit_price` | `DOUBLE` | Price per Unit |
| `line_total` | `DOUBLE` | Quantity * Unit Price |
| `campaign_id` | `BIGINT` | Associated Marketing Campaign (if any) |
| `sk_campaign_id` | `STRING` | FK to `gold.dim_campaign` |

### `gold.dim_customer`
*Implements SCD Type 2 tracking for segment and address changes.*
Partitioned By: `bucket(16, customer_id)`
| Column Name | Data Type | Description |
|---|---|---|
| `sk_customer_id` | `STRING` | Surrogate Key |
| `customer_id` | `BIGINT` | Immutable Business Key |
| `full_name` | `STRING` | Customer Name |
| `email` | `STRING` | Email |
| `segment` | `STRING` | Marketing Segment (e.g., standard, vip) |
| `address` | `STRING` | Mailing Address |
| `valid_from` | `TIMESTAMP` | Start of validity |
| `valid_to` | `TIMESTAMP` | End of validity (Null if current) |
| `is_current` | `BOOLEAN` | Flag for currently active row |

---

## 🏗️ Core Silver Schemas

### `silver.reviews`
Partitioned By: `months(submitted_at)`
| Column Name | Data Type | Description |
|---|---|---|
| `review_id` | `STRING` | Unique Review Identifier |
| `product_id` | `BIGINT` | Product Identifier |
| `customer_id` | `BIGINT` | Customer Identifier |
| `rating` | `INT` | 1-5 Star Rating |
| `title` | `STRING` | Review Title |
| `body` | `STRING` | Review Content |
| `verified_purchase` | `BOOLEAN` | Purchase Verification Flag |
| `submitted_at` | `TIMESTAMP` | Timestamp of Review Submission |
| `ingested_at` | `TIMESTAMP` | Timestamp of System Ingestion |
| `sentiment_score` | `DOUBLE` | Computed Polarity (-1.0 to 1.0) |

---

## 📊 Core Gold Marts

### `gold.daily_sales`
Partitioned By: `months(sales_date)`
| Column | Type |
|---|---|
| `sales_date` | `DATE` |
| `order_count` | `BIGINT` |
| `total_revenue` | `DOUBLE` |
| `revenue_7d_avg` | `DOUBLE` |
| `revenue_30d_avg` | `DOUBLE` |

### `gold.funnel_conversion`
Partitioned By: `months(activity_date)`
| Column | Type |
|---|---|
| `activity_date` | `DATE` |
| `page_view` | `BIGINT` |
| `add_to_cart` | `BIGINT` |
| `checkout_start` | `BIGINT` |
| `checkout_complete` | `BIGINT` |
| `cart_to_checkout_rate` | `DOUBLE` |
| `checkout_to_purchase_rate` | `DOUBLE` |

### `gold.product_sentiment`
| Column | Type |
|---|---|
| `product_id` | `BIGINT` |
| `product_name` | `STRING` |
| `category` | `STRING` |
| `review_count` | `BIGINT` |
| `avg_rating` | `DOUBLE` |
| `avg_sentiment` | `DOUBLE` |
| `pct_verified` | `DOUBLE` |

---

## 🎲 Generating Mock Data

The environment relies on Python-based synthetic generators utilizing `Faker`.

**Configuration:** Data volume is controlled via variables in `.env`:
- `GEN_ORDERS_TOTAL_ROWS=100000`
- `GEN_PRODUCTS_TOTAL=2000`
- `GEN_REVIEWS_TOTAL=5000`
- `GEN_CLICKSTREAM_EVENTS_PER_SEC=200`

**Commands:**
1. Populate Postgres and MongoDB references: 
   ```bash
   make seed
   ```
2. Initiate continuous real-time CDC polling:
   ```bash
   make stream-cdc
   ```
3. Inject continuous clickstream events into Kafka:
   ```bash
   make stream-clickstream
   ```
