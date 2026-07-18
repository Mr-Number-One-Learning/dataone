# The Quarantine Layer

## Overview
The **Quarantine Layer** (often referred to in the industry as a Dead Letter Queue or Reject Table) is a foundational component of our Data Lakehouse's data quality strategy. 

Instead of silently dropping bad data or crashing the entire pipeline when an upstream system sends malformed records, the pipeline routes failing rows into persistent quarantine tables. This guarantees **zero data loss**, maintains high availability for the 99% of healthy data, and provides an irrefutable audit trail for data engineers to investigate and fix issues.

---

## How It Works

### 1. The Quality Gate (`src/dataone/quality/validators.py`)
As data moves from Bronze (raw) into Silver or Gold, it passes through the `run_quality_gate()` function. This function applies a strict schema contract to the incoming DataFrame:
- **Null Checks:** Are primary keys and foreign keys present?
- **Range Bounds:** Are prices ≥ 0? Are review ratings between 1 and 5?

### 2. Routing to Quarantine
If a row passes, it moves downstream normally. If it fails *any* check, it is **not** dropped. Instead:
1. It is tagged with a human-readable `_quarantine_reason` (e.g., `"null_check_failed"` or `"range_check_failed"`).
2. It is routed and appended to a `quarantine.<table_name>` Iceberg table.

### 3. Aggregation and Monitoring
To ensure quarantined data isn't just "swept under the rug", the batch pipeline automatically builds a **`quarantine_summary`** Gold mart. This table aggregates failures across all datasets by `batch_date`, `table_name`, and `failure_reason`. Alongside it, a **`quality_gate_summary`** mart tracks overall passed vs. quarantined rows, providing a clean interface for true quarantine rate monitoring in data quality dashboards and alerting.

---

## Covered Datasets

Currently, the quality gate covers eight core datasets:

| Bronze / Silver Source | Quality Constraints | Target Quarantine Table |
| :--- | :--- | :--- |
| `campaigns` | `campaign_id`, `name`, `start_date`, `end_date` must not be null. `budget`, `spend`, `clicks`, `conversions` ≥ 0. | `quarantine.campaigns` |
| `customers` | `customer_id` must not be null. | `quarantine.customers` |
| `orders` | `order_id`, `customer_id`, `order_date` must not be null. | `quarantine.orders` |
| `products` | `product_id`, `category`, `name` must not be null. | `quarantine.products` |
| `clickstream` | `session_id`, `event_type`, `ts` must not be null. `event_type` must be valid. | `quarantine.clickstream` |
| `order_items` | `order_item_id`, `order_id` must not be null. `quantity` ≥ 1, `unit_price` ≥ 0. | `quarantine.order_items` |
| `fact_order_items` | `order_id`, `customer_id`, `product_id` must not be null. `unit_price` ≥ 0, `quantity` ≥ 1. | `quarantine.fact_order_items` |
| `reviews` | `review_id`, `product_id`, `rating` must not be null. `rating` must be between 1 and 5. | `quarantine.reviews` |

---

## Why is this a Best Practice?

1. **No Data Loss:** Dropping data silently hides upstream bugs and corrupts financial metrics. Quarantining guarantees every row is accounted for.
2. **Fault Tolerance:** A single bad row won't crash a pipeline processing millions of good rows, ensuring dashboards update on time.
3. **Replayability:** Because the exact raw payload is saved, data engineers can fix the parsing bug and replay the quarantined rows back into the pipeline without asking the upstream source to resend the data.
4. **Automated Observability:** The `quality_gate_summary` mart allows the team to set alerts in Grafana (e.g., *"Page on-call if quarantine rate exceeds 5% of total batch"*), shifting data quality from reactive debugging to proactive monitoring.
