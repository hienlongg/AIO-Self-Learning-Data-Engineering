# Tài liệu đọc: Từ Kỹ thuật Dữ liệu Hiện đại đến Kiến trúc Data Mart với dbt & SQL

---

## Lời mở đầu

Khi làm việc với dữ liệu trong doanh nghiệp, một câu hỏi muôn thuở mà các Data Engineer (DE) và Data Analyst (DA) thường gặp từ ban giám đốc là: 
> *"Tại sao dashboard của phòng Sales báo cáo doanh thu tháng này là 10 tỷ, nhưng dashboard của phòng Tài chính lại chỉ ghi nhận 9.2 tỷ?"*

Vấn đề trên không nằm ở công cụ trực quan hóa (PowerBI, Tableau hay Metabase) mà nằm ở **tầng kiến trúc dữ liệu phía dưới**: thiếu một nguồn chân lý duy nhất (Single Source of Truth), phân tán logic nghiệp vụ ở nhiều nơi và thiếu vắng tầng **Data Mart** được chuẩn hóa.

Tài liệu này được biên soạn nhằm giúp bạn:
1. Hiểu bức tranh tổng quan về công việc của một Data Engineer (Pipelines, Data Modeling, Orchestration).
2. Nắm vững vai trò của giai đoạn phân tích (Analytics Phase) và tại sao Data Mart lại đóng vai trò sống còn.
3. Nhìn lại lịch sử tiến hóa kiến trúc (Kimball, Inmon) cho tới các giải pháp hiện đại (OBT, Data Lakehouse, Data Mesh, Semantic Layer).
4. Phân tích chi tiết cách hiện thực hóa kiến trúc này qua dự án mẫu trong `data-mart_draft.md` sử dụng **dbt** và **SQL**.

---

## Phần 1: Bức tranh toàn cảnh về Data Engineering

### 1. Sứ mệnh cốt lõi của Kỹ sư Dữ liệu (Data Engineer)
Data Engineer chịu trách nhiệm xây dựng và vận hành "hệ thống đường ống dẫn nước" cho toàn bộ tổ chức, đảm bảo dữ liệu di chuyển từ nơi phát sinh đến nơi tiêu thụ một cách **chính xác, kịp thời, an toàn và có thể mở rộng**.

Về bản chất, dữ liệu trong doanh nghiệp chia làm 2 thế giới tách biệt:
* **Hệ thống vận hành (OLTP - Online Transaction Processing)**: Cơ sở dữ liệu ứng dụng (Postgres, MySQL, MongoDB). Đặc điểm: tối ưu hóa cho ghi/đọc từng dòng đơn lẻ với độ trễ mili-giây, thiết kế chuẩn hóa (3NF) để tránh trùng lặp dữ liệu khi giao dịch.
* **Hệ thống phân tích (OLAP - Online Analytical Processing)**: Kho dữ liệu (Snowflake, BigQuery, ClickHouse). Đặc điểm: tối ưu hóa cho việc quét hàng triệu dòng trên một số cột nhất định để tổng hợp dữ liệu (SUM, COUNT, AVG).

Để kết nối hai thế giới này, Data Engineer làm việc xoay quanh 3 trụ cột:
1. **Data Pipelines (ELT / ETL)**: Cơ chế trích xuất dữ liệu từ các nguồn (Operational DBs, Event Streams, 3rd party APIs) và nạp vào Cloud Data Warehouse/Data Lake.
2. **Data Modeling (Mô hình hóa dữ liệu)**: Nghệ thuật tổ chức, làm sạch và định hình lại dữ liệu thô hỗn loạn thành các bảng logic có cấu trúc phục vụ cho con người và thuật toán truy vấn.
3. **Orchestration (Điều phối quy trình)**: Nhạc trưởng (như Apache Airflow, Dagster, Prefect) quản lý đồ thị phụ thuộc (DAG - Directed Acyclic Graph), đảm bảo các tác vụ chạy đúng thứ tự, tự động retry khi gặp lỗi, cảnh báo và hỗ trợ chạy bù dữ liệu (backfill).

---

### 2. Analytics Phase nằm ở đâu và tại sao nó bắt buộc phải tồn tại?

Luồng dữ liệu hiện đại thường đi qua các chặng sau:
$$\text{Nguồn dữ liệu (OLTP, Logs, Event)} \xrightarrow{\text{Extract \& Load}} \text{Raw / Bronze (Warehouse)} \xrightarrow{\textbf{Transform / Analytics (dbt)}} \text{Marts / Gold} \xrightarrow{\text{Consume}} \text{BI, ML, Apps}$$

#### Analytics Phase là gì?
Đây chính là giai đoạn **Transform** trong mô hình **ELT modern data stack**. Thay vì để các nhà phân tích nghiệp vụ hay Data Scientist truy vấn trực tiếp vào bảng thô, Analytics Phase nhận dữ liệu thô đã nạp vào kho, sau đó:
* Làm sạch (loại bỏ bản ghi trùng, chuẩn hóa kiểu dữ liệu, đồng nhất timezone).
* Áp dụng business rules (thế nào là đơn hàng hợp lệ? đơn hủy, đơn hoàn tiền được tính ra sao?).
* Gom nhóm và tính toán các chỉ số kinh doanh cốt lõi (KPIs).

#### Tại sao chúng ta cần Analytics Phase chuyên biệt?
* **Bảo vệ hệ thống và tối ưu chi phí**: Một câu query của Data Analyst quét toàn bộ bảng log 500 triệu dòng trên BigQuery trực tiếp từ dashboard có thể tốn hàng trăm USD và làm đơ dashboard. Tầng transform tính toán sẵn và lưu lại kết quả giúp dashboard phản hồi tức thì với chi phí tối thiểu.
* **Ngăn chặn "Logic Drift" (Lệch chuẩn định nghĩa)**: Nếu không có tầng transform tập trung, mỗi analyst sẽ tự viết câu lệnh SQL riêng trong công cụ BI của họ. Một người lọc `status = 'completed'`, người khác lọc `status IN ('completed', 'shipped')` — dẫn đến số liệu không bao giờ khớp nhau.

---

## Phần 2: Data Mart — Lịch sử, Lý do tồn tại và Các giải pháp thay thế

### 1. Data Mart là gì và tại sao lại cần Data Mart?
* **Định nghĩa**: Data Mart là một tập con của Kho dữ liệu (Data Warehouse), được thiết kế tập trung phục vụ cho một **chủ đề nghiệp vụ cụ thể** (Subject-Oriented) hoặc một phòng ban nhất định (ví dụ: Marketing Mart, Sales Mart, Finance Mart, Customer 360).
* **Tại sao không để người dùng truy vấn thẳng vào Enterprise Data Warehouse (EDW)?**
  * **Độ phức tạp**: EDW chứa hàng nghìn bảng liên kết chằng chịt. Người dùng nghiệp vụ hoặc BI developer chỉ cần tập trung vào các thực thể quen thuộc của họ.
  * **Hiệu năng & Tối ưu hóa**: Data Mart thường được tinh gọn, tiền tổng hợp (pre-aggregated) hoặc phi chuẩn hóa (denormalized) phù hợp với đúng loại báo cáo cần hiển thị.
  * **Phân quyền & Bảo mật**: Dễ dàng cấp quyền cho team Marketing chỉ đọc Mart Marketing mà không truy cập được dữ liệu nhạy cảm của HR hay Finance.

---

### 2. Tiêu chuẩn công nghiệp trong quá khứ: Cuộc đối đầu Inmon vs. Kimball

Trong lịch sử hơn 30 năm của Data Warehousing, có 2 trường phái kinh điển:

```
[Inmon - Top-Down]
Sources --> Staging --> Normalized 3NF EDW (Trung tâm) --> Dependent Data Marts --> BI

[Kimball - Bottom-Up]
Sources --> Staging --> Dimensional Model (Star Schema: Facts & Shared Conformed Dims) --> BI
```

| Tiêu chí | Bill Inmon (Top-down) | Ralph Kimball (Bottom-up) |
| :--- | :--- | :--- |
| **Triết lý** | Xây dựng EDW chuẩn hóa (3NF) tập trung trước, sau đó tạo ra các Data Mart phục vụ phòng ban. | Xây dựng các Data Mart theo mô hình Dimensional (Star Schema) liên kết với nhau qua các Conformed Dimensions (Bus Matrix). |
| **Ưu điểm** | Tính toàn vẹn cao, tránh dư thừa dữ liệu tối đa. | Thời gian triển khai nhanh (Time-to-value), cực kỳ thân thiện với người dùng cuối và BI tool. |
| **Nhược điểm** | Chi phí ban đầu khổng lồ, thời gian hoàn thành kéo dài hàng năm. | Cần kỷ luật rất cao để duy trì tính nhất quán của Conformed Dimensions khi mở rộng. |

Suốt nhiều thập kỷ, **Kimball Dimensional Modeling** (Fact & Dimension) đã trở thành chuẩn mực thực tế của ngành.

---

### 3. Các giải pháp thay thế và hướng phát triển hiện đại

Hạ tầng lưu trữ và tính toán đám mây phát triển đã sinh ra những phương pháp tiếp cận mới thay thế hoặc bổ trợ cho Data Mart truyền thống:

1. **One Big Table (OBT) - Một bảng khổng lồ**:
   * *Nguyên lý*: Thay vì duy trì mô hình sao gồm 1 Fact và 5 Dimension nối với nhau qua khóa ngoại, các Data Warehouse dạng cột (BigQuery, ClickHouse, Snowflake) khuyến khích denormalize hoàn toàn thành một bảng duy nhất chứa tất cả thuộc tính.
   * *Ưu điểm*: Tốc độ query cực nhanh vì không tốn chi phí JOIN; viết query đơn giản.
   * *Nhược điểm*: Kích thước lưu trữ tăng lên (dù cơ chế nén cột giảm thiểu phần nào), dữ liệu bị lặp lại.

2. **Data Lakehouse (Apache Iceberg, Delta Lake)**:
   * Xóa nhòa ranh giới giữa Data Lake (rẻ, chứa phi cấu trúc) và Data Warehouse (nhanh, chuẩn ACID). Data Mart ngày nay có thể là các bảng Iceberg được quản lý mở, cho phép nhiều query engine (Trino, Spark, DuckDB) cùng đọc.

3. **Data Mesh (Kiến trúc hướng phi tập trung)**:
   * Thay vì một team Data Engineer trung tâm gánh toàn bộ việc xây Mart cho cả công ty, quyền sở hữu dữ liệu được chuyển giao cho các domain team (ví dụ: Domain Order sở hữu pipeline và Data Product Order). Data Mart trở thành một **Data Product** có SLA rõ ràng.

4. **Headless Semantic Layer (dbt Semantic Layer, Cube.js)**:
   * Thay vì nhét toàn bộ phép tính tổng hợp (Aggregations) vào các bảng vật lý tĩnh của Mart, ta định nghĩa logic tính toán trong một tầng metadata chung. Tầng này nhận yêu cầu từ Tableau hay PowerBI, tự động sinh ra câu lệnh SQL tối ưu đẩy xuống warehouse xử lý.

---

## Phần 3: dbt và vai trò định hình Data Mart hiện đại

### 1. Cuộc cách mạng từ ETL sang ELT
Trước đây, công cụ ETL (Informatica, Talend, SSIS) phải tự thực hiện tính toán trên server riêng rồi mới nạp vào kho. Với sức mạnh tính toán song song khổng lồ của Cloud Data Warehouse, trật tự đảo ngược: **Extract & Load toàn bộ dữ liệu thô vào kho trước, sau đó dùng chính sức mạnh của kho để Transform (ELT)**.

**dbt (data build tool)** ra đời và trở thành tiêu chuẩn vàng cho chữ **T** trong ELT. dbt áp dụng triệt để các thực hành tốt nhất của ngành Kỹ nghệ Phần mềm (Software Engineering) vào Kỹ thuật Dữ liệu:
* **Version Control**: Mọi model dữ liệu là code SQL lưu trữ trên Git.
* **Tính Module hóa & Quản lý Phụ thuộc**: Sử dụng hàm `{{ ref('model_name') }}` giúp dbt tự động biên dịch thành một DAG hoàn chỉnh, không cần hard-code tên database hay schema.
* **Automated Testing**: Viết test như unit test trong phần mềm để đảm bảo dữ liệu không bị NULL, không trùng lặp và thỏa mãn các ràng buộc logic.
* **CI/CD**: Tự động test dữ liệu trên môi trường staging trước khi merge vào production.

---

### 2. Kiến trúc phân tầng kinh điển trong dbt
Trong một dự án dbt chuẩn, mã nguồn được chia theo 3 tầng rõ rệt:

```
Sources (Raw)
   │
   ▼
[Staging (stg_)]       --> Làm sạch kỹ thuật: đổi tên cột, ép kiểu, 1:1 với nguồn
   │
   ▼
[Intermediate (int_)]  --> Single Source of Truth: JOIN các thực thể, xử lý logic phức tạp
   │
   ▼
[Marts (mart_)]        --> Tầng phục vụ: Fact, Dimension, Bảng tổng hợp theo business domain
```

---

## Phần 4: Phân tích thực tế thiết kế trong `data-mart_draft.md`

Tài liệu bản thảo `data-mart_draft.md` cung cấp một ví dụ mẫu mực về cách xây dựng Data Mart phục vụ Dashboard cho ngành thương mại điện tử (E-commerce). Dưới đây là phân tích chi tiết từng thành phần:

### 1. Macro: Chốt chặn duy nhất chống "Metric Drift" (`macros/business_logic.sql`)

```sql
{% macro revenue_expr() %}
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

* **Vấn đề giải quyết**: Trong kinh doanh, "Doanh thu" (Revenue) là khái niệm dễ gây tranh cãi nhất: Có tính đơn đang giao (shipped) không? Đơn hoàn tiền (refund) có trừ ra không? Chiết khấu (discount) tính thế nào? Có tài khoản test của nhân viên QA không?
* **Giải pháp**: dbt Macro đóng gói toàn bộ logic này thành một hàm Jinja có thể tái sử dụng. Bất kỳ model nào cần tính toán doanh thu hoặc lọc đơn hàng hợp lệ bắt buộc phải gọi `{{ revenue_expr() }}` và `{{ is_valid_order() }}`. Khi định nghĩa nghiệp vụ thay đổi, **kỹ sư dữ liệu chỉ cần sửa duy nhất 1 dòng code**.

---

### 2. Intermediate Model: Nguồn chân lý duy nhất (`models/intermediate/int_order_lines_enriched.sql`)

```sql
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

* **Khái niệm then chốt — Grain (Độ mịn của dữ liệu)**:
  * Grain của model này là **từng dòng sản phẩm trong đơn hàng** (`order_id` + `product_id`).
  * Đây là độ mịn sâu nhất có thể. Từ độ mịn này, ta có thể tổng hợp lên cấp độ toàn đơn hàng, cấp độ khách hàng, hoặc cấp độ ngày/tháng.
* **Tính tái sử dụng (Reusability)**: Bằng cách gom sẵn thông tin khách hàng (`c.customer_segment`, `c.acquisition_channel`) và tính sẵn `net_revenue` tại đây, các tầng Mart phía sau không cần phải viết lại các câu JOIN lặp đi lặp lại.

---

### 3. Hiện thực hóa 2 Data Marts chuyên biệt

#### A. Mart 1: Hiệu suất bán hàng theo ngày (`models/marts/sales/mart_sales_daily.sql`)
* **Đối tượng phục vụ**: Ban giám đốc (C-level), Trưởng phòng Sales.
* **Grain**: Cấp độ ngày (`order_date`).
* **Kỹ thuật viết SQL đáng chú ý**:
  ```sql
  SUM(net_revenue) / NULLIF(COUNT(DISTINCT order_id), 0) AS avg_order_value,
  SUM(refund_amount) / NULLIF(SUM(net_revenue), 0)       AS refund_rate
  ```
  * *Xử lý lỗi chia cho 0*: Luôn sử dụng hàm `NULLIF(..., 0)` cho mẫu số khi tính tỷ lệ (AOV, Refund Rate) để ngăn toàn bộ pipeline bị sập khi gặp ngày không có đơn hàng.

#### B. Mart 2: Khách hàng 360 & Giữ chân (`models/marts/customer/mart_customer_360.sql`)
* **Đối tượng phục vụ**: Đội ngũ CRM, Marketing, Chăm sóc khách hàng.
* **Grain**: Từng khách hàng duy nhất (`customer_id`).
* **Chỉ số hành vi & Phân nhóm (Segmentation)**:
  * **Customer Lifetime Value (LTV)**: `SUM(net_revenue)` trên toàn bộ vòng đời.
  * **Customer Tier**: Phân loại mức độ gắn bó (`one_time`, `repeat`, `loyal`) dựa trên số lượng đơn.
  * **Heuristic Churn Risk (Nguy cơ rời bỏ)**: 
    ```sql
    last_order_date < DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY) AS is_churn_risk
    ```
    Cảnh báo khách hàng đã ngừng mua sắm quá 90 ngày để team Marketing kích hoạt chiến dịch re-engagement.

---

### 4. Tách biệt tầng ngữ nghĩa (Semantic Layer) khỏi Dashboard

Trong file `mart_sales_daily.yml`:
```yaml
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
      - name: total_orders
        agg: sum

metrics:
  - name: average_order_value
    type: ratio
    label: AOV
    type_params:
      numerator: net_revenue
      denominator: total_orders
```

* **Tại sao không để công cụ BI tự tính AOV?**
  Nếu bạn để BI tool tự tính, người tạo biểu đồ có thể vô tình lấy `AVG(net_revenue)` thay vì `SUM(net_revenue) / COUNT(order_id)`. Đó là hai con số hoàn toàn khác nhau.
* Bằng cách khai báo thước đo cơ bản (`measures`) và công thức tỷ lệ (`metrics`) trong dbt Semantic Layer, BI tool chỉ đóng vai trò hiển thị (presentation layer).

---

### 5. Đảm bảo chất lượng dữ liệu và kiểm thử (Data Quality & Tests)

```yaml
models:
  - name: mart_sales_daily
    tests:
      - dbt_utils.equal_rowcount:
          compare_model: ref('int_order_lines_enriched')
    columns:
      - name: net_revenue
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: ">= 0"
```
* **Chống rò rỉ dữ liệu (Data Leakage)**: Đảm bảo doanh thu ròng không bao giờ âm (trừ khi có nghiệp vụ đặc thù).
* **Đối soát số lượng dòng (Row-count reconciliation)**: Phát hiện ngay nếu phép JOIN làm nở dữ liệu (Cartesian product/Fan-out bug) gây sai lệch toàn bộ số liệu báo cáo.

---

## Phần 5: Thảo luận thực tế và Các điểm đánh đổi (Trade-offs)

Khi đưa thiết kế trong `data-mart_draft.md` vào hệ thống thực tế (production), kỹ sư dữ liệu cần cân nhắc 3 vấn đề lớn sau:

### 1. Chiến lược Materialization (Lưu trữ vật lý trong Warehouse)
* **`view`**: Không tốn dung lượng lưu trữ, luôn cập nhật dữ liệu mới nhất, nhưng query chậm nếu bảng nguồn lớn. Thích hợp cho tầng Staging.
* **`table`**: Tốn dung lượng lưu trữ, mất thời gian build, nhưng dashboard query cực nhanh. Thích hợp cho các Mart phục vụ dashboard hàng ngày.
* **`incremental`**: Chỉ xử lý và chèn dữ liệu mới phát sinh từ lần chạy trước (ví dụ: ngày hôm qua). Bắt buộc phải áp dụng khi bảng đơn hàng chạm mốc hàng chục triệu dòng.

### 2. Xử lý khác biệt cú pháp giữa các Cloud Data Warehouse
Trong `data-mart_draft.md`, hàm xử lý ngày tháng đang dùng cú pháp của Google BigQuery:
* **BigQuery**: `DATE_DIFF(MAX(order_date), MIN(order_date), DAY)`, `DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)`
* **Snowflake**: `DATEDIFF('day', MIN(order_date), MAX(order_date))`, `DATEADD('day', -90, CURRENT_DATE())`
* **PostgreSQL / Redshift**: `MAX(order_date) - MIN(order_date)`, `CURRENT_DATE - INTERVAL '90 days'`
*(Khuyến nghị: Sử dụng package `dbt_date` hoặc `dbt.dateadd` để code có thể chạy xuyên suốt trên mọi nền tảng).*

### 3. Khi nào cần tách thêm Dimension Table theo chuẩn Kimball?
Hiện tại `mart_customer_360` đóng vai trò vừa là Dimension bảng khách hàng, vừa chứa các chỉ số tổng hợp (Aggregated Fact). Cách thiết kế dạng bảng rộng (Wide Table) này rất trực quan và tiện lợi cho dashboard CRM. Tuy nhiên, nếu quy mô dữ liệu mở rộng lớn và có nhu cầu theo dõi lịch sử thay đổi thông tin khách hàng (đổi địa chỉ, đổi hạng thành viên), chúng ta sẽ cần triển khai thêm kỹ thuật **Slowly Changing Dimensions (SCD Type 2)** của Kimball.

---

## Tổng kết

Thiết kế dữ liệu không chỉ là viết các câu lệnh SQL để ra được con số. Đó là một **quy trình kỹ thuật có hệ thống**:
1. Đảm bảo dữ liệu chảy mượt mà từ hệ thống nguồn qua các đường ống được điều phối chặt chẽ.
2. Chuẩn hóa và làm sạch tại tầng trung gian (Intermediate) với grain xác định.
3. Tập trung hóa logic tính toán chỉ số bằng macro và semantic layer để loại bỏ hoàn toàn tình trạng sai lệch số liệu.
4. Đóng gói thành các Data Mart chuyên biệt, tối ưu hiệu năng cho người dùng cuối và dashboard.
