# PROJECT OVERVIEW: FRESHMAN ONBOARDING MANUAL

Welcome to the team! If you're reading this, you are probably our newest Data Engineer. This document is designed to take you from zero to "Know-It-All" regarding the DataOne project. 

Read this manual carefully. By the end of it, you will confidently understand exactly how our data moves, how our storage engines work under the hood, and the "why" behind every major technical decision we've made.

---

## 1. The 30,000-Foot View (The Big Picture)

**What does this project actually do?**  
DataOne is an end-to-end, fully localized E-Commerce Analytics Lakehouse. Our core business goal is to ingest messy, high-velocity data from simulated online storefronts (orders, clicks, reviews) and clean it up so we can calculate exact business metrics—like our daily revenue, our marketing campaign success, and our checkout funnel conversion rates. 

**Who cares about this data?**  
- **Financial Analysts:** They need exact daily sales numbers and Return on Ad Spend (ROAS) to adjust budgets.
- **Data Scientists:** They use our clean historical customer data to train predictive lifetime-value models.
- **Executive Dashboards:** Our leadership team stares at the Grafana dashboards powered by our fastest database layers to make real-time decisions.

---

## 2. The Step-by-Step Data Flows (From Source to Destination)

To understand this project, you need to understand the life of a single data point. We have four main streams of data. Let's walk through their journeys:

### Stream A: Core E-Commerce Transactions (Orders & Customers)
- **The Origin:** Starts in a PostgreSQL database (acting as our live application database). Movement is triggered by a **CDC Simulator** (*Change Data Capture — a system that quietly listens for any "inserts" or "updates" in a database and broadcasts them as events*). It polls every 5 seconds.
- **The Landing:** The events are pushed into an Apache Kafka topic named `orders-cdc`, which is scooped up by a PySpark job and dumped into our raw Iceberg table: `bronze.orders_cdc`.
- **The Journey:** Our nightly batch job (`nightly_batch.py`) wakes up, cracks open the raw JSON data, throws away duplicates, and splits the data cleanly into `silver.orders` and `silver.customers`. Finally, it merges them into our pristine Star Schema as `gold.fact_order_items` and `gold.dim_customer`.

### Stream B: User Clickstream (Behavioral Telemetry)
- **The Origin:** A Python event generator simulating web traffic (page views, adding items to carts). This is a continuous, never-ending stream of events.
- **The Landing:** Events fly directly into the `clickstream` Kafka topic and are appended immediately to `bronze.clickstream`.
- **The Journey:** We scrub out invalid clicks (like missing session IDs) to create `silver.clickstream`. We then join this data against actual completed orders in the Gold layer to calculate our `gold.conversion_rate` (e.g., "Out of 100 people who added to cart, how many actually bought it?").

### Stream C: Product Reviews
- **The Origin:** Written to a MongoDB NoSQL database (since reviews have messy, variable text). We use **Apache NiFi** (*a visual data-routing tool with a drag-and-drop canvas*) to pull daily batches of new reviews.
- **The Landing:** NiFi lands the documents into `bronze.reviews`.
- **The Journey:** A PySpark job cleans the text and runs NLP (*Natural Language Processing*) to assign a `sentiment_score` (-1.0 for angry, +1.0 for happy). This is saved as `silver.reviews`, which is then aggregated to calculate the average `gold.product_sentiment`.

### Stream D: Marketing Campaigns
- **The Origin:** Daily CSV file drops exported from our marketing platforms (like Facebook Ads).
- **The Landing:** NiFi watches a folder, grabs new CSVs, and drops them into `bronze.campaigns`.
- **The Journey:** Campaign data is naturally clean reference data, so it *skips the Silver layer entirely* and is loaded directly into `gold.dim_campaign` to calculate our `gold.roas` (*Return on Ad Spend — how much money we made for every dollar we spent on ads*).

---

## 3. The Medallion Architecture (Explained Like a Water Filtration System)

We use the **Medallion Architecture**, which is a design pattern that filters data progressively through three stages. Think of it like a water filtration plant for a city.

### 🥉 Bronze Layer (The Raw River Water)
This is where data is dumped exactly as it arrived. We never delete data here; we only append to it. Why? Because if we make a mistake downstream, we can always come back to the Bronze layer and start over.
- **What it looks like:** The `bronze.orders_cdc` table literally contains a column called `data_json` which holds messy, unparsed JSON strings representing database changes. It's ugly, but it's safe.

### 🥈 Silver Layer (The Filtration Station)
Here, we scrub out duplicates, enforce strict data types, and drop corrupt rows. We structure data into **Conformed Entities** (*standardized tables representing exactly one concept, like "One row = One Customer"*). 
- **What it looks like:** Tables like `silver.customers` and `silver.orders`. **Crucially, we do NOT join data in Silver.** We keep orders and customers perfectly separate so the data remains modular and clean.

### 🥇 Gold Layer (The Luxury Drinking Fountain)
This is the presentation layer. It is heavily optimized for fast querying by BI (*Business Intelligence*) tools. We finally smash (join) the data together here to answer business questions. 
- **What it looks like:** A Star Schema design (`gold.fact_order_items` joined to dimensions like `gold.dim_customer`) and heavily pre-calculated tables like `gold.daily_sales`. We pre-calculate rolling 7-day revenue averages here so our dashboards don't have to do the heavy math on the fly.

---

## 4. Our Storage Secret Weapon: MergeTrees & Table Design

To store our data and serve it fast, we use two massive technologies: **Apache Iceberg** (for our Bronze/Silver/Gold Lakehouse) and **ClickHouse** (for serving the final Gold data to dashboards).

### The MergeTree Engine (ClickHouse)
ClickHouse uses a storage engine called the **MergeTree**. 
- **Analogy:** Imagine someone keeps handing you unsorted decks of playing cards. You quickly throw them on the table. In the background, a robot silently and continuously sorts those cards into perfect numerical order, merging the small decks into one massive, perfectly sorted deck. 
- **How it works:** When data is written to ClickHouse, it lands in small physical parts. In the background, the engine constantly merges these parts together on disk, strictly ordering them by the Primary Key.

### Primary & Sorting Keys
In standard databases, indexes are like the index at the back of a textbook. But in ClickHouse and Iceberg, we use **Sorting Keys**.
- **Analogy:** If you are looking for "John Smith" in a perfectly alphabetized phone book, you don't read every page; you flip to the 'S' section immediately. 
- **Our setup:** Our Gold tables in ClickHouse are ordered by keys like `ORDER BY (sales_date)`. When Grafana asks for "Yesterday's sales", the database skips reading 99% of the historical data and instantly reads yesterday's block.

### Partitioning & Bucketing Strategy
- **Partitioning:** We partition `gold.fact_order_items` by `days(order_date)` in Apache Iceberg. This chops the table into neat physical folders per day. If you query for January 1st, Spark completely ignores the folders for February.
- **Bucketing:** We use `bucket(16, customer_id)`. 
  - **Analogy:** Imagine a massive post office sorting mail into 16 bins based on the customer's ZIP code. 
  - **Why:** When we join our Orders table to our Customers table, Spark already knows that Customer #55's orders and demographic data are both sitting in physical Bin #3. It joins them locally without having to shuffle data across the computer's network, saving massive amounts of compute time.

---

## 5. Common "Gotchas", Edge Cases, & Troubleshooting

### Q: What happens if an upstream API suddenly adds a new column to the `orders` table?
**A:** Thanks to our Bronze layer design (**Schema-on-Read**), the pipeline won't crash! Because `bronze.orders_cdc` just stores the raw data as a flexible JSON string (`data_json`), the new column simply rides along safely. It will just be ignored until a Data Engineer explicitly updates the `bronze_to_silver.py` script to extract it.

### Q: How does our system prevent duplicate rows if a pipeline runs twice by mistake?
**A:** Our PySpark jobs are built to be **Idempotent** (*a math concept meaning that applying an operation multiple times has the exact same result as applying it once*). When data moves from Bronze to Silver, we run a window function: `ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY captured_at DESC)`. This ensures we only ever grab the *latest* single version of a row, automatically deleting any accidental duplicate events.

### Q: What should I check first if the `nightly_batch` pipeline fails overnight?
**A:** 
1. Check the Prefect UI (`http://localhost:4200`) to see exactly which step failed.
2. If data simply looks "wrong" or is missing from Gold, check the **Quarantine** tables (`quarantine.orders`, `quarantine.customers`). If a row has a null ID or negative price, our Data Quality Gates route it there automatically, tagging it with a `_quarantine_reason` column for you to debug.

---

## 6. Freshman "Know-It-All" Glossary

1. **CDC (Change Data Capture):** A technique used to automatically track and emit database inserts, updates, and deletes as real-time events without slowing down the application.
2. **Schema Evolution:** The ability of a database table (like Apache Iceberg) to safely add, drop, or rename columns over time without breaking existing data files.
3. **Idempotency:** Designing pipelines so that if they accidentally run twice, they don't produce duplicate data.
4. **SCD Type 2 (Slowly Changing Dimension Type 2):** A method of tracking historical changes in a dimension (e.g., if a customer moves to a new address, we keep the old address row active for past orders, and create a new row for future orders).
5. **DAG (Directed Acyclic Graph):** A fancy mathematical term for a workflow pipeline where tasks run in a specific, one-way order (Task A must finish before Task B starts) without any infinite loops.
6. **OBT (One Big Table):** A highly denormalized, wide table in the Gold layer designed purely for dashboard speed, combining facts and dimensions so the BI tool doesn't have to perform joins.
