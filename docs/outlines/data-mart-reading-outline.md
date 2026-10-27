# Reading Outline: From Modern Data Engineering to Data Mart Architecture

---

## Part 1: The Data Engineering Landscape — Foundations & Workflows

### 1. The Core Mission of a Data Engineer
* **Operational vs. Analytical Systems**: Why transactional databases (PostgreSQL, MySQL, MongoDB — OLTP) are optimized for row-level writes, whereas analytics requires columnar, aggregated reads (OLAP).
* **The Fundamental Pillars**:
  * **Pipelines (ELT/ETL)**: Moving raw operational data (CDC, APIs, event streams) into a central repository (Warehouse/Lakehouse) without impacting production.
  * **Data Modeling**: Structuring chaotic, normalized raw data into logical, clean, and performant shapes.
  * **Orchestration**: Directing the workflow DAGs (Airflow, Dagster, Prefect) to handle dependencies, retries, backfills, and SLA monitoring.

### 2. The Position and Purpose of the Analytics Phase
* **Where Analytics Sits in the Pipeline**:
  $$\text{Sources (OLTP/Logs)} \xrightarrow{\text{Extract \& Load}} \text{Raw / Bronze (Warehouse)} \xrightarrow{\text{Transform (dbt)}} \text{Marts / Gold} \xrightarrow{\text{Consume}} \text{BI / ML / Reverse ETL}$$
* **Why the Analytics Phase is Mandatory**:
  * Raw data is messy: schema changes, duplicate events, test orders, timezone mismatches, deleted records.
  * Analysts should never write 500-line SQL queries joining 15 raw tables directly in Tableau/Looker.
  * **Goal**: Provide governed, high-performance, and trustworthy "data products" ready for business decision-making.

---

## Part 2: Data Marts — History, Evolution & Modern Alternatives

### 1. What is a Data Mart and Why Do We Need It?
* **Definition**: A subject-oriented subset of data tailored to the specific needs of a single business domain (e.g., Sales, Marketing, Customer Success, Finance).
* **Why build Data Marts?**
  * **Business Agility**: Sales teams don't need inventory logistics; they need fast, clear KPIs.
  * **Query Performance & Cost**: Scanning aggregated/denormalized tables is exponentially cheaper and faster than scanning raw event logs in modern warehouses.
  * **Governance & Access Control**: Enforcing column/row-level permissions per business function.

### 2. Historical Industry Standards
* **Bill Inmon (Top-Down / Enterprise Data Warehouse - EDW)**:
  * Central normalized (3NF) warehouse first; dependent data marts created downstream.
  * *Trade-off*: High upfront cost, very slow time-to-market.
* **Ralph Kimball (Bottom-Up / Dimensional Modeling)**:
  * Star schemas (Facts & Dimensions) unified via **Conformed Dimensions** (the Data Warehouse Bus Architecture).
  * Industry benchmark for 20+ years: resilient, intuitive for SQL, and report-friendly.

### 3. Current Developments & Modern Alternatives
* **One Big Table (OBT)**:
  * Driven by ultra-fast columnar warehouses (BigQuery, Snowflake, ClickHouse) where joins are minimized by storing pre-joined wide tables.
* **The Lakehouse Architecture**:
  * Open table formats (Apache Iceberg, Delta Lake) unifying streaming logs, unstructured data, and relational marts under one storage engine.
* **Data Mesh**:
  * Shifting from centralized data teams to treating data marts as autonomous **"Data Products"** owned by cross-functional domain teams.
* **Headless Semantic Layers (Cube, dbt Semantic Layer)**:
  * Moving metric calculation outside both the warehouse table and the BI tool into a unified semantic API layer.

---

## Part 3: The Role of dbt in Building Data Marts

### 1. Paradigm Shift: ETL $\to$ ELT
* How dbt revolutionized transformation by pushing computation inside the modern cloud warehouse using standard SQL and Jinja.
* Applying Software Engineering principles to Data Engineering:
  * **Git / Version Control**: Code reviews and traceability.
  * **Modularity (`ref()`)**: Automatic DAG dependency generation.
  * **Testing & CI/CD**: Automated schema and data assertions before code reaches production.
  * **Self-Documenting Lineage**: Automatic lineage graphs.

### 2. dbt Layering Architecture (The "Medallion" Mapping)
* **Staging (`stg_`)**: Clean, typed, 1:1 mapping with raw sources.
* **Intermediate (`int_`)**: Entity-level business logic, complex joins, single sources of truth.
* **Marts (`mart_`)**: Final consumable fact/dimension/wide-aggregation models designed for dashboards.

---

## Part 4: Code Walkthrough & Analysis of `data-mart_draft.md`

### 1. Eliminating Metric Drift via Macros (`macros/business_logic.sql`)
* Analyzing `revenue_expr()` and `is_valid_order()`.
* **Concept**: Avoiding the "Two Dashboards, Two Different Revenue Numbers" problem. If business rules change (e.g., how refunds or test accounts are flagged), the data engineer updates code in **one macro**, and all downstream marts inherit the fix.

### 2. The Intermediate Layer as the Single Source of Truth (`int_order_lines_enriched.sql`)
* Analyzing the choice of **Grain**: line-item level (`order_id` + `product_id`).
* How pre-enriching dimensions (customer segments, channels, flags) allows multiple distinct marts to branch off without redundant joining code.

### 3. Deep Dive into the Two Mart Implementations
* **Mart 1: `mart_sales_daily` (Executive / Sales Performance)**
  * **Grain**: Daily aggregate (`order_date`).
  * Additive metrics vs. derived ratios (`avg_order_value`, `refund_rate`) with zero-division handling (`NULLIF`).
* **Mart 2: `mart_customer_360` (Customer Analytics / Retention)**
  * **Grain**: Unique entity (`customer_id`).
  * Behavioral metrics: LTV, customer lifespan, RFM tiers (`one_time`, `repeat`, `loyal`), and inactivity/churn heuristics.

### 4. Semantic Layer & BI Decoupling (`mart_sales_daily.yml`)
* Differentiating **Entities**, **Measures**, and **Metrics**.
* How this prevents BI tools (Tableau, Looker, PowerBI) from calculating business metrics inconsistently.

### 5. Data Quality & Automated Assertions
* Row-count reconciliation tests (`dbt_utils.equal_rowcount`).
* Domain constraints (`not_null`, `expression_is_true: ">= 0"`).

---

## Part 5: Discussion & Critical Evaluation Questions

1. **Trade-offs in Modeling**: When should an intermediate table be materialized as an ephemeral CTE, a view, or an incremental table?
2. **Kimball vs. dbt Marts**: Does `mart_sales_daily` resemble a classic Kimball Periodic Snapshot Fact Table or an aggregated Summary Mart?
3. **Handling High-Volume Data**: How would this pipeline change if orders scale to 50M records/day (Incremental models, clustering/partitioning keys)?
4. **Warehouse Dialects**: What changes are needed to make date arithmetic (`DATE_DIFF`, `DATE_SUB`, `INTERVAL`) portable across BigQuery, Snowflake, and PostgreSQL?
