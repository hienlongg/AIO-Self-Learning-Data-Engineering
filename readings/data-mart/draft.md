# 2 Data Marts phục vụ Dashboard (dbt + SQL)

Vì bạn chưa nêu domain cụ thể, tôi chọn **bối cảnh e-commerce** (đơn hàng, khách hàng, thanh toán) — pattern này áp dụng lại được cho hầu hết business. Điểm cốt lõi của thiết kế:

- **Semantic consistency**: mọi định nghĩa metric (doanh thu, đơn hợp lệ...) chỉ nằm ở **1 nơi duy nhất** (macro + semantic model), các mart chỉ reference lại.
- **Reusable**: mart 1 (Sales) và mart 2 (Customer) đều đi từ chung 1 intermediate model `int_order_lines_enriched`.

## Cấu trúc project

```
dbt_project/
├── models/
│   ├── staging/
│   │   ├── stg_orders.sql
│   │   ├── stg_order_items.sql
│   │   └── stg_customers.sql
│   ├── intermediate/
│   │   └── int_order_lines_enriched.sql   ← single source of truth
│   └── marts/
│       ├── sales/
│       │   ├── mart_sales_daily.sql
│       │   └── mart_sales_daily.yml
│       └── customer/
│           ├── mart_customer_360.sql
│           └── mart_customer_360.yml
└── macros/
    └── business_logic.sql                 ← định nghĩa metric thống nhất
```

## 1. Macro — nơi định nghĩa metric duy nhất

```sql
-- macros/business_logic.sql

{% macro revenue_expr() %}
    -- NET REVENUE: chỉ tính đơn hoàn thành, đã trừ discount và refund
    CASE
        WHEN o.order_status = 'completed'
        THEN (oli.quantity * oli.unit_price * (1 - oli.discount_pct)) - COALESCE(oli.refund_amount, 0)
        ELSE 0
    END
{% endmacro %}

{% macro is_valid_order() %}
    o.order_status IN ('completed', 'shipped')
    AND o.is_test_order = FALSE
{% endmacro %}
```

## 2. Intermediate — single source of truth

```sql
-- models/intermediate/int_order_lines_enriched.sql

SELECT
    o.order_id,
    o.customer_id,
    o.order_date,
    o.order_status,
    oli.product_id,
    oli.quantity,
    oli.unit_price,
    oli.discount_pct,
    oli.refund_amount,
    c.customer_segment,
    c.acquisition_channel,
    c.first_order_date,
    {{ revenue_expr() }}  AS net_revenue
FROM {{ ref('stg_orders') }} o
JOIN {{ ref('stg_order_items') }} oli USING (order_id)
JOIN {{ ref('stg_customers') }} c USING (customer_id)
WHERE {{ is_valid_order() }}
```

## 3. Mart 1 — `mart_sales_daily` (Sales Performance)

```sql
-- models/marts/sales/mart_sales_daily.sql

WITH base AS (
    SELECT * FROM {{ ref('int_order_lines_enriched') }}
)
SELECT
    order_date,
    COUNT(DISTINCT order_id)                AS total_orders,
    SUM(net_revenue)                        AS net_revenue,
    SUM(quantity)                           AS items_sold,
    COUNT(DISTINCT customer_id)             AS unique_customers,
    -- AOV dùng chung công thức với mọi nơi: revenue / orders
    SUM(net_revenue) / NULLIF(COUNT(DISTINCT order_id), 0) AS avg_order_value,
    SUM(refund_amount)                      AS total_refunds,
    SUM(refund_amount) / NULLIF(SUM(net_revenue), 0)       AS refund_rate
FROM base
GROUP BY order_date
```

## 4. Mart 2 — `mart_customer_360` (Customer / RFM)

```sql
-- models/marts/customer/mart_customer_360.sql

WITH base AS (
    SELECT * FROM {{ ref('int_order_lines_enriched') }}
),
agg AS (
    SELECT
        customer_id,
        customer_segment,
        acquisition_channel,
        first_order_date,
        MAX(order_date)                                     AS last_order_date,
        COUNT(DISTINCT order_id)                            AS total_orders,
        SUM(net_revenue)                                    AS customer_lifetime_value,
        SUM(net_revenue) / NULLIF(COUNT(DISTINCT order_id), 0) AS avg_order_value,
        DATE_DIFF(MAX(order_date), MIN(order_date), DAY)    AS customer_lifespan_days
    FROM base
    GROUP BY customer_id, customer_segment, acquisition_channel, first_order_date
)
SELECT
    *,
    CASE
        WHEN total_orders = 1 THEN 'one_time'
        WHEN total_orders BETWEEN 2 AND 4 THEN 'repeat'
        ELSE 'loyal'
    END AS customer_tier,
    -- Churn risk: không mua > 90 ngày so với snapshot hiện tại
    last_order_date < DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY) AS is_churn_risk
FROM agg
```

## 5. Semantic Layer — chốt định nghĩa cho BI tool

```yaml
# models/marts/sales/mart_sales_daily.yml

semantic_models:
  - name: sales
    model: ref('mart_sales_daily')
    entities:
      - name: date
        type: time
        expr: order_date
    measures:
      - name: net_revenue
        agg: sum
        description: "Doanh thu ròng, đã trừ discount & refund, chỉ đơn hoàn thành"
      - name: total_orders
        agg: sum

metrics:
  - name: net_revenue
    type: simple
    label: Net Revenue
    type_params:
      measure: net_revenue
  - name: average_order_value
    type: ratio
    label: AOV
    type_params:
      numerator: net_revenue
      denominator: total_orders
```

## Đảm bảo semantic consistency như thế nào

1. **Metric chỉ định nghĩa 1 lần** — trong `business_logic.sql` và `semantic_models`. Dashboard (Looker/Tableau/MetricFlow) đọc từ semantic layer, không tự tính lại.
2. **dbt test** chống drift:

```yaml
# trong schema.yml của 2 mart
models:
  - name: mart_sales_daily
    tests:
      - dbt_utils.equal_rowcount:
          compare_model: ref('int_order_lines_enriched')  # theo grain phù hợp
    columns:
      - name: net_revenue
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: ">= 0"
```

## Câu hỏi để tôi chỉnh chính xác hơn

1. Domain của bạn là gì (e-commerce, SaaS, fintech...)? Schema staging thật của bạn trông như thế nào?
2. Warehouse nào — BigQuery, Snowflake, Redshift? (cú pháp `DATE_DIFF`/`INTERVAL` khác nhau đôi chút)
3. Dashboard dùng công cụ nào — có cần export sang MetricFlow/Looker hay không?

Nếu bạn gửi schema staging thật hoặc upload file dbt hiện tại, tôi sẽ map 2 mart này vào codebase của bạn luôn.
