# 🎯 BUSINESS CASE, DOMAIN PROBLEMS SOLVED & ANALYTICAL USE CASES

> 🌐 **Language:** **English** | [Tiếng Việt](BUSINESS_CASE.vi.md)
>
> This document explains **why this data pipeline exists from a business perspective**, the real-world e-commerce data pitfalls it resolves in the Olist dataset, and how each **dbt Star Schema** mart and exported dataset supports **Finance, CRM/Marketing, Logistics/Operations, and Merchandising** decisions.

---

## 1. Executive Context: Why Raw Marketplace Data Is Not Ready for Decision-Making

In an e-commerce marketplace (like Olist in Brazil), operational databases emit **9 fragmented relational CSV feeds** covering orders, line items, split payments, customer reviews, buyers, products, sellers, category translations, and geolocation. Putting raw operational dumps directly into BI dashboards creates four major business risks:

1. **Revenue vs. Cash Misalignment:** Merchandise value, shipping fees, invoice value, and installment-adjusted card payments differ per order — causing Sales and Finance reports to disagree.
2. **Broken Customer & Retention Metrics:** The source system issues a **new `customer_id` for every order**, so naive `GROUP BY customer_id` queries miscount unique buyers, underestimate Customer Lifetime Value (LTV), and report 0% repeat purchases.
3. **Unmeasured Delivery SLA Impact:** Raw order timestamps (`order_purchase_timestamp`, `order_delivered_customer_date`, `order_estimated_delivery_date`) do not directly expose delay duration, SLA breach flags, or how late deliveries hurt `review_score`.
4. **Inconsistent Catalog & Untrusted Reporting:** Product categories arrive in Portuguese (`product_category_name`), raw schemas contain column typos (`product_name_lenght`), and naive batch overwrites risk wiping historical BI tables if an upstream CSV arrives empty or malformed.

This pipeline transforms those raw feeds into a **governed, event-driven Kimball Star Schema (`public_marts`)** on Amazon Redshift Serverless and exports **4 analytics-ready datasets** for BI consumption.

---

## 2. Five Core Business Problems Solved

| Domain / Stakeholder | Business Question | Source Data Pitfall | Solution Implemented in Pipeline |
| :--- | :--- | :--- | :--- |
| **Finance & Sales** | What is true merchandise revenue vs. shipping cost vs. cash collected, and how do customers pay? | Orders split across multiple `order_items` and multi-method `order_payments` (credit card installments, boleto, vouchers). | [`fct_orders.sql`](../dbt/models/marts/fct_orders.sql) separates `total_item_value`, `total_freight_value`, `total_order_cost`, and `total_payment_value`, ranks `primary_payment_type` by **highest payment value** (not alphabetical `MIN`), and tracks `max_installments`. |
| **CRM & Growth Marketing** | How many real buyers do we have, who are repeat buyers, and what is their true Lifetime Value (LTV)? | Source generates a distinct `customer_id` per order while tracking the actual person via `customer_unique_id`. | [`dim_customers.sql`](../dbt/models/marts/dim_customers.sql) aggregates full order and payment history at **`customer_unique_id` grain** (`first_order_date`, `most_recent_order_date`, `total_orders`, `lifetime_spend`) and segments `One-Time Customer` vs. `Returning Customer` — even across incremental updates. |
| **Operations & Logistics** | Are we meeting promised delivery SLAs across states/cities, and how do delays affect customer ratings? | Only raw purchase, actual delivery, and estimated delivery timestamps exist in `orders`. | [`fct_orders.sql`](../dbt/models/marts/fct_orders.sql) computes `delivery_duration_days`, `estimated_delivery_duration_days`, `delay_days`, `is_delayed` (`1`/`0`/`NULL`), and standardized `delivery_status` (`On Time`, `Delayed`, `Pending Delivery`), joined with `review_score` and customer location (`city`, `state`). |
| **Merchandising & Catalog** | Which product categories drive sales and freight weight in English-standardized reporting? | Category names are in Portuguese, some products lack translations, and raw CSV headers contain typos (`_lenght`). | [`stg_products.sql`](../dbt/models/staging/stg_products.sql) fixes column spelling (`product_name_length`, `product_description_length`), and [`dim_products.sql`](../dbt/models/marts/dim_products.sql) resolves `category_name` via `coalesce(product_category_name_english, product_category_name, 'unknown')` alongside physical dimensions (`product_weight_g`, length/height/width). |
| **Data Governance & FinOps** | Can leadership trust fresh metrics without risking wiped dashboards or runaway cloud bills? | Cron polling wastes compute; naive `TRUNCATE + COPY` wipes target tables on bad/empty CSVs; no audit trail. | Event-driven `S3 → SQS → Airflow 3 AssetWatcher` updates Redshift on arrival; `validate_csv_header` + `ensure_non_empty_stage` + atomic swaps protect existing data; [`ops.pipeline_ingestion_audit`](../scripts/create_ops_audit_table.sql) logs every batch (`SWAPPED` → `ARCHIVED` → `COMPLETED` / `FAILED`); Redshift Serverless sleeps at `$0` compute when idle. |

---

## 3. Analytical Marts & BI Consumption Contract

### 3.1. Kimball Star Schema (`public_marts`)
1. **[`fct_orders`](../dbt/models/marts/fct_orders.sql)** *(Incremental Fact — `unique_key: order_id`, `delete+insert`)*
   * **Business Grain:** 1 row per order (`order_id`).
   * **Incremental Accuracy Guarantee:** Uses an `affected_order_ids` union across all **4 upstream sources** (`stg_orders`, `stg_order_items`, `stg_payments`, `stg_reviews`) so late deliveries, delayed payment settlements, or newly posted customer reviews on older orders are automatically reconciled without rebuilding the entire table.
2. **[`dim_customers`](../dbt/models/marts/dim_customers.sql)** *(Incremental Dimension — `unique_key: customer_id`, `delete+insert`)*
   * **Business Grain:** 1 row per order-level `customer_id`, enriched with **person-level (`customer_unique_id`) lifetime metrics**.
   * **Incremental LTV Guarantee:** Identifies affected `customer_unique_id`s (new orders within 3-day lookback, new `customer_id`s, or changed `lifetime_spend`/`total_orders`/address) and recomputes their complete historical spend so `Returning Customer` flags and LTV never drift.
3. **[`dim_products`](../dbt/models/marts/dim_products.sql)** *(Dimension Table)*
   * **Business Grain:** 1 row per `product_id` with translated English `category_name`, original Portuguese name, listing metadata (`product_photos_qty`), and shipping dimensions.

### 3.2. Decoupled BI Datasets (`scripts/export_to_csv.py` → `exports/`)
To keep Power BI / offline analytics fast with **$0 idle Redshift RPU cost**, [`scripts/export_to_csv.py`](../scripts/export_to_csv.py) materializes four clean analytical CSVs:

| Exported File | Source Marts / Views | Primary Business Use Cases |
| :--- | :--- | :--- |
| **`1_master_orders_analytics.csv`** | `public_marts.fct_orders` + `dim_customers` | Executive KPI scorecards (GMV, cash collected, order volume, AOV), regional demand by `customer_state`/`city`, Delivery SLA compliance (`delivery_status`), and delay-vs-`review_score` correlation. |
| **`2_dim_products.csv`** | `public_marts.dim_products` | Category hierarchy filtering, SKU catalog profiling (weight & dimensions for freight optimization, photo/description completeness). |
| **`3_dim_customers.csv`** | `public_marts.dim_customers` | Customer segmentation (`One-Time Customer` vs. `Returning Customer`), cohort retention (`first_order_date` vs. `most_recent_order_date`), and top-LTV Pareto analysis. |
| **`4_order_items_product_sales.csv`** | `public_staging.stg_order_items` + `dim_products` | Line-item revenue & freight breakdown by English `category_name`, basket size analysis, and top-performing SKUs/categories. |

---

## 4. Data Quality Contracts Protecting Business KPIs

Every business metric exported to stakeholders is guarded across **ingestion + all 3 Medallion layers (`124` compiled dbt tests)** before reaching `exports/` or BI dashboards:

* **Ingestion & Schema Contracts (Before DWH Swap):**
  * [`validate_csv_header`](../include/ingestion_guards.py) blocks upstream schema drift before loading Redshift.
  * [`ensure_non_empty_stage`](../include/ingestion_guards.py) aborts 0-row CSVs inside an explicit transaction (`autocommit=False`), preventing accidental wipeouts of historical business data.
* **Bronze / Raw Source Tests (`44` Tests in [`dbt/models/staging/sources.yml`](../dbt/models/staging/sources.yml)):**
  * Enforces primary and composite uniqueness (`order_id`, `[order_id, order_item_id]`, `[order_id, payment_sequential]`, `[review_id, order_id]`, `customer_id`, `product_id`, `seller_id`), `non_negative` financial/weight checks, and `accepted_values` for `order_status`, `payment_type`, and `review_score` (`1–5`) before downstream models execute.
* **Silver / Staging Contract Tests (`45` Tests in [`dbt/models/staging/schema.yml`](../dbt/models/staging/schema.yml)):**
  * Validates standardized view schemas, `non_negative` derived `total_item_value`, strict referential integrity (`relationships`) across orders, items, payments, reviews, customers, and products, and `severity: warn` monitoring on untranslated product categories.
* **Gold / Marts & Cross-Layer Reconciliation (`31` Schema Tests in [`dbt/models/marts/schema.yml`](../dbt/models/marts/schema.yml) + `4` Singular Tests in [`dbt/tests/`](../dbt/tests/)):**
  * Enforces primary keys, foreign key `relationships`, `non_negative` revenue/LTV measures, and `accepted_values` on `delivery_status` & `customer_segment`.
  * Runs 4 singular SQL business invariants: row-level `total_order_cost = total_item_value + total_freight_value`, cross-layer revenue reconciliation (`fct_orders` vs. `stg_order_items`), `delivery_status` / `is_delayed` / `delay_days` consistency, and `customer_segment` vs. `total_orders` alignment.
