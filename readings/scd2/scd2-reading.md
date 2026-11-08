# Tài liệu đọc: Quản trị Dữ liệu Lịch sử với Slowly Changing Dimensions (SCD Type 2) — Từ Nền tảng Quan hệ đến Kiến trúc Lakehouse Hiện đại

---

## Lời mở đầu

Trong phân tích dữ liệu doanh nghiệp, hãy tưởng tượng một tình huống thực tế sau:

> Ngày 15/01/2026, khách hàng Nguyễn Văn An (sống tại Hà Nội) đặt một đơn hàng trị giá 50.000.000 VNĐ. Đến ngày 15/03/2026, anh An chuyển vào sinh sống tại Đà Nẵng và cập nhật địa chỉ trên ứng dụng.
> 
> Vào cuối quý 1/2026, Giám đốc Kinh doanh yêu cầu Data Analyst xuất báo cáo: *"Doanh thu theo tỉnh thành trong tháng 01/2026 là bao nhiêu?"*

Nếu hệ thống cơ sở dữ liệu của bạn chỉ đơn thuần ghi đè địa chỉ mới của anh An lên địa chỉ cũ, doanh thu 50 triệu kia sẽ lập tức bị gán cho Đà Nẵng. Báo cáo tài chính tháng 1 bị sai lệch hồi tố (retroactively distorted). Đội ngũ vận hành kho bãi Hà Nội bị mất chỉ tiêu KPI, trong khi chi nhánh Đà Nẵng nhận doanh số mà họ không hề tạo ra.

Vấn đề trên dẫn tới câu hỏi kinh điển của mọi kiến trúc sư dữ liệu:
> **"Nếu một thuộc tính của thực thể thay đổi hôm nay, các báo cáo và quyết định trong quá khứ có được phép thay đổi theo không?"**

Nếu câu trả lời là **KHÔNG**, bạn bắt buộc phải bước vào thế giới của **Slowly Changing Dimensions (SCD)**, cụ thể là **SCD Type 2**. 

Tài liệu này sẽ dẫn dắt bạn qua toàn bộ hành trình: từ bản chất của Dimension, sứ mệnh bảo vệ tính toàn vẹn thời gian của Data Engineer, lịch sử ra đời của SCD, cho đến việc giải phẫu mã nguồn thực tế với **dbt snapshot** và phân tích các phương án thay thế hiện đại trong kỷ nguyên Cloud & Lakehouse.

---

## Phần 1: Bản chất của Dimension & Sứ mệnh của Data Engineer

### 1. Dimension là gì và tại sao Doanh nghiệp lại cần nó?

Trong mô hình hóa dữ liệu (Dimensional Modeling), toàn bộ hoạt động của doanh nghiệp được phân tách thành hai thành phần cơ bản: **Fact (Sự kiện/Đo lường)** và **Dimension (Bối cảnh/Thực thể)**.

* **Fact (Động từ - What happened)**: Đại diện cho các biến cố kinh doanh phát sinh tại một thời điểm nhất định (ví dụ: `orders`, `transactions`, `page_clicks`, `shipments`). Fact thường là dữ liệu số, có tính cộng gộp (additive), phát sinh liên tục với khối lượng lớn và **bất biến (immutable)** — một khi đơn hàng đã thanh toán lúc 10:00 sáng, sự thật đó không bao giờ đổi.
* **Dimension (Danh từ - Context)**: Cung cấp toàn bộ câu trả lời cho các câu hỏi: **Ai (Who)** mua? Mua **Ở đâu (Where)**? Mua sản phẩm **Gì (What)**? Thông qua chiến dịch **Nào (Why)**? Và thanh toán theo hình thức **Nào (How)**? (ví dụ: `dim_customer`, `dim_product`, `dim_store`, `dim_campaign`).

```
                +-----------------------+
                |     dim_customer      |
                | (Who: Name, City, Tier)|
                +-----------+-----------+
                            |
                            | 1
                            |
                            | n
+-----------------------+   |   +-----------------------+
|      dim_product      +---+---+      fct_orders       |
| (What: SKU, Category) |       | (Fact: Amount, Qty)   |
+-----------------------+       +-----------+-----------+
                                            |
                                            | n
                                            |
                                            | 1
                                +-----------+-----------+
                                |       dim_store       |
                                | (Where: Location, Reg)|
                                +-----------------------+
```

#### Tại sao doanh nghiệp không thể sống thiếu Dimension?
1. **Thước đo trần trụi là vô nghĩa**: Nếu một Data Engineer báo cáo với CEO: *"Hôm nay hệ thống ghi nhận 1 tỷ VNĐ doanh thu"*, câu hỏi tiếp theo của CEO luôn là: *"Đến từ khách hàng mới hay cũ? Kênh Marketing nào mang lại? Sản phẩm nào bán chạy nhất? Khu vực miền Bắc hay miền Nam?"*. Dimension chính là công cụ duy nhất giúp **cắt lát (slice)**, **xúc xắc (dice)**, **khoan sâu (drill-down)** và **cuộn lên (roll-up)** các chỉ số kinh doanh.
2. **Phân khúc khách hàng & Phân bổ (Attribution)**: Để tính Customer Lifetime Value (LTV), tỷ lệ rời bỏ (churn rate) theo độ tuổi, hoặc hiệu quả của chiến dịch tiếp thị, doanh nghiệp cần gắn kết Fact với các thuộc tính phân loại của Dimension.
3. **Pháp lý, Thuế và Kiểm toán (Compliance & Audit)**: Khi kiểm toán thuế hay điều tra gian lận, doanh nghiệp phải chứng minh được tình trạng pháp lý, địa chỉ xuất hóa đơn VAT, hoặc trạng thái KYC của khách hàng **ngay tại thời điểm giao dịch phát sinh**, chứ không phải trạng thái hiện tại.

---

### 2. Dimension Modeling nằm ở đâu trong Sứ mệnh của Data Engineer?

Nhiệm vụ tối thượng của một Data Engineer (DE) không chỉ là "chuyển dữ liệu từ A sang B" (Data Ingestion), mà là **cung cấp nền tảng dữ liệu đáng tin cậy, có thể tái lập và phản ánh trung thực thực tế kinh doanh cho phân tích**.

Ở đây xuất hiện một mâu thuẫn cốt lõi giữa hai thế giới:
* **Hệ thống giao dịch (OLTP - Applications)**: Tối ưu cho việc ghi/đọc từng dòng đơn lẻ, đề cao hiệu năng ghi tức thời. Khi người dùng đổi địa chỉ, ứng dụng chạy lệnh:
  ```sql
  UPDATE users SET city = 'Da Nang', updated_at = NOW() WHERE id = 1;
  ```
  Hệ thống OLTP chỉ quan tâm đến **trạng thái hiện tại (Current State)** để giao hàng cho đơn tiếp theo. Trạng thái cũ bị xóa bỏ hoàn toàn.
* **Hệ thống phân tích (OLAP - Analytics/Warehousing)**: Tối ưu cho việc tổng hợp dữ liệu trên diện rộng qua nhiều năm tháng. Phân tích đòi hỏi **tính trung thực lịch sử (Historical Fidelity)**. 

> **Bản hợp đồng phân tích (The Analytical Contract):**
> Sứ mệnh của Data Engineer là dựng nên một chiếc cầu nối giữa **sự biến đổi liên tục (mutability)** của OLTP và **sự bất biến lịch sử (immutability)** của OLAP. Dimension Modeling — đặc biệt là kỹ thuật SCD — chính là cơ chế bảo vệ "bản hợp đồng" này, ngăn chặn hiện tượng rò rỉ dữ liệu (data leakage) trong Machine Learning và sai lệch số liệu trong báo cáo tài chính.

---

## Phần 2: Lịch sử, Nguồn gốc và Sự tiến hóa của SCD

### 1. Thời kỳ "Tiền SCD": Các hệ thống xưa cũ đã giải quyết bài toán này ra sao?

Trước khi Ralph Kimball hệ thống hóa lý thuyết về SCD vào thập niên 1990, các kỹ sư phần mềm và quản trị cơ sở dữ liệu (DBA) đã thử nghiệm nhiều giải pháp để lưu giữ lịch sử. Hầu hết đều để lại những hệ lụy nặng nề:

#### Tiếp cận 1: Ghi đè tại chỗ (In-Place Overwrite — Tiền thân của SCD1)
* **Cách làm**: Đồng bộ dữ liệu thô từ OLTP vào Data Warehouse bằng cách `UPDATE` đè lên bản ghi cũ mỗi khi có thay đổi.
* **Hậu quả**: Toàn bộ lịch sử biến mất. Báo cáo doanh thu theo vùng miền của các năm trước bị thay đổi mỗi khi khách hàng chuyển nhà. Báo cáo năm 2024 chạy vào tháng 12/2024 cho ra một số, nhưng chạy lại vào tháng 06/2025 lại ra một số khác. Độ tin cậy của kho dữ liệu hoàn toàn sụp đổ.

#### Tiếp cận 2: Sao lưu định kỳ toàn bộ cơ sở dữ liệu (Periodic Full Dumps)
* **Cách làm**: Cứ 00:00 mỗi đêm hoặc cuối mỗi tháng, DBA xuất toàn bộ database thành một bản snapshot riêng biệt (`customers_2026_01_01`, `customers_2026_01_02`, ...).
* **Hậu quả**:
  * **Lãng phí lưu trữ khủng khiếp**: Nếu bảng có 10 triệu khách hàng nhưng mỗi ngày chỉ 1.000 người thay đổi thông tin, hệ thống vẫn nhân bản 9.999.000 bản ghi giống hệt nhau mỗi ngày.
  * **Truy vấn điểm thời gian (Point-in-Time) bất khả thi**: Nếu một đơn hàng phát sinh vào ngày 15/02/2026, câu lệnh SQL phải tìm và `JOIN` với đúng bảng của ngày hôm đó. Không một công cụ BI nào có thể tạo dashboard động nối qua hàng trăm bảng snapshot riêng rẽ như vậy.

#### Tiếp cận 3: Bảng Audit Log / Nhật ký giao dịch (Change Log / EAV Trigger)
* **Cách làm**: Tạo bảng log ghi lại mọi sự kiện thay đổi: `(entity_id, field_name, old_value, new_value, changed_at)`.
* **Hậu quả**: Đây là cấu trúc tuyệt vời cho việc điều tra lỗi phần mềm (audit trail), nhưng là thảm họa cho OLAP. Muốn biết một khách hàng có email và địa chỉ nào vào ngày 10/02/2026, câu lệnh SQL phải duyệt ngược toàn bộ chuỗi log, sử dụng hàng loạt câu lệnh `WINDOW FUNCTION` phức tạp và tự `SELF JOIN` tốn kém để tái dựng lại một dòng dữ liệu phẳng.

---

### 2. Sự ra đời của Ralph Kimball và Hệ phân loại SCD

Để giải quyết dứt điểm các bế tắc trên, **Ralph Kimball** (cha đẻ của Dimensional Modeling) đã chuẩn hóa khái niệm **Slowly Changing Dimensions (Các chiều biến đổi chậm)** — "Chậm" ở đây mang nghĩa: khách hàng không đổi địa chỉ 100 lần mỗi giây như click chuột, mà vài tháng hoặc vài năm mới đổi một lần.

Kimball đưa ra bảng phân loại chuẩn mực:

| Loại SCD | Cơ chế xử lý | Lịch sử | Trường hợp sử dụng kinh điển |
| :--- | :--- | :--- | :--- |
| **SCD Type 0** | **Giữ nguyên bản gốc (Retain Original)**: Không bao giờ cập nhật. | Không đổi | Ngày tạo tài khoản ban đầu (`original_signup_date`). |
| **SCD Type 1** | **Ghi đè (Overwrite)**: Giá trị mới đè lên giá trị cũ. | **Mất hoàn toàn** | Sửa lỗi chính tả tên khách hàng, sửa số điện thoại bị nhập sai định dạng. |
| **SCD Type 2** | **Thêm dòng mới (Add New Row)**: Giữ dòng cũ, đóng hiệu lực; tạo dòng mới với khoảng thời gian hiệu lực mới. | **Bảo tồn toàn bộ vô hạn** | Khách hàng chuyển vùng, thăng cấp bậc VIP, thay đổi người quản lý kinh doanh (Sales Rep). |
| **SCD Type 3** | **Thêm cột mới (Add New Column)**: Thêm cột `previous_city` bên cạnh `current_city`. | Chỉ giữ được **1 bước** | Chỉ quan tâm đến trạng thái liền kề trước đó; không theo dõi được chuỗi 3 lần thay đổi trở lên. |
| **SCD Type 4** | **Bảng lịch sử riêng (Mini-Dimension / History Table)**: Tách các thuộc tính biến đổi nhanh sang một bảng profile phụ. | Lưu ở bảng riêng | Theo dõi sự thay đổi dải thu nhập, nhóm tuổi hoặc điểm tín dụng của khách hàng. |
| **SCD Type 6** | **Lai ghép (Hybrid 1 + 2 + 3)**: Kết hợp cả 3 loại ($1 + 2 + 3 = 6$). Dòng vẫn sinh mới (Type 2), nhưng có thêm cột lưu giá trị hiện tại (Type 1) và giá trị trước (Type 3). | Giữ toàn bộ + xem nhanh hiện tại | Cần báo cáo song song: doanh thu theo vùng tại thời điểm mua VÀ doanh thu theo vùng hiện tại của khách hàng. |

> **Tại sao SCD Type 2 trở thành Tiêu chuẩn Vàng (Gold Standard)?**
> SCD Type 2 đạt được sự cân bằng hoàn hảo: không lãng phí lưu trữ như Full Dump, không mất lịch sử như Type 1, không giới hạn số bước như Type 3, và có cấu trúc bảng quan hệ phẳng chuẩn mực — cho phép truy vấn chính xác trạng thái tại bất kỳ thời điểm nào trong quá khứ chỉ bằng một phép so sánh khoảng ngày (`>=` và `<`).

---

### 3. Sự tiến hóa trong kỷ nguyên hiện đại (Cloud ELT & Lakehouse)

1. **Từ On-Premise ETL sang Cloud Data Warehouse (ELT)**:
   * Trước năm 2015, việc duy trì SCD2 đòi hỏi các công cụ ETL kéo-thả nặng nề (Informatica PowerCenter, IBM DataStage, Talend, SSIS). Các công cụ này phải tra cứu (lookup) từng dòng trong bộ nhớ server ETL rất chậm chạp.
   * Ngày nay, với Snowflake, BigQuery, Databricks và Redshift, toàn bộ dữ liệu thô được đổ vào Data Warehouse. Logic SCD2 được thực thi trực tiếp bên trong Warehouse bằng câu lệnh `MERGE INTO` xử lý song song hàng triệu dòng trong vài giây.
2. **Khai sinh công cụ khai báo (Declarative dbt Snapshots)**:
   * Thay vì Data Engineer phải tự viết hàng trăm dòng code SQL thủ công để tìm bản ghi thay đổi, update bản cũ và insert bản mới, dbt (data build tool) đóng gói toàn bộ logic này vào khối cấu hình `snapshot`.
3. **Open Table Formats & Lakehouse Time-Travel**:
   * Sự bùng nổ của Apache Iceberg, Delta Lake mang đến khả năng "Time Travel" ở tầng metadata file (Parquet). Dù vậy, như chúng ta sẽ phân tích ở Phần 6, SCD2 ở tầng dữ liệu nghiệp vụ vẫn giữ nguyên giá trị không thể thay thế.

---

## Phần 3: Giải phẫu Kỹ thuật SCD Type 2 (Phân tích `scd2_draft.md`)

Hãy đi sâu vào cấu trúc dữ liệu thực tế được trình bày trong bản thảo dự án `scd2_draft.md`.

### 1. Kiến trúc cột & Cơ chế Effective Dating

Khi khách hàng `customer_id = 1` thay đổi từ Hà Nội sang Đà Nẵng, sau đó đổi email, bảng Dimension SCD2 lưu trữ **3 phiên bản (versions)** độc lập:

| customer_sk (PK) | customer_id (NK) | email | city | valid_from | valid_to | is_current |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **1** | 1 | an@x.com | Ha Noi | 2026-01-01 | 2026-03-15 | 0 (false) |
| **2** | 1 | an@x.com | Da Nang | 2026-03-15 | 2026-06-01 | 0 (false) |
| **3** | 1 | an.x@gmail.com | Da Nang | 2026-06-01 | 9999-12-31 | 1 (true) |

#### A. Natural Key (Khóa tự nhiên) vs. Surrogate Key (Khóa thay thế)
* **Natural Key / Business Key (`customer_id = 1`)**: Là định danh của khách hàng trong hệ thống ứng dụng nguồn (OLTP). Trong bảng SCD2, **khóa tự nhiên không còn là duy nhất (unique)** nữa vì 1 khách hàng có thể có $N$ dòng phiên bản.
* **Surrogate Key (`customer_sk`)**: Là khóa chính nhân tạo (Primary Key) của bảng Dimension. Mỗi phiên bản của khách hàng sở hữu một `customer_sk` duy nhất (1, 2, 3). Bảng Fact bắt buộc phải lưu `customer_sk` này để liên kết chính xác đến phiên bản tại thời điểm phát sinh giao dịch.

#### B. Quy ước khoảng nửa mở: Half-Open Interval $[valid\_from, valid\_to)$
Một chi tiết kỹ thuật cực kỳ quan trọng thường bị bỏ qua:
* Phiên bản 1: có hiệu lực từ `2026-01-01` đến `2026-03-15`.
* Phiên bản 2: bắt đầu chính xác vào `2026-03-15` đến `2026-06-01`.

Làm sao để một đơn hàng phát sinh vào đúng ngày `2026-03-15` không bị rơi vào cả hai phiên bản (double-counting)?
* Ta áp dụng quy tắc toán học **nửa mở**: lấy mốc đầu (inclusive) và bỏ mốc cuối (exclusive):
  $$\text{valid\_from} \le \text{order\_date} < \text{valid\_to}$$
* Bản ghi cũ đóng đúng vào ngày bản ghi mới mở. Không có bất kỳ khoảng trống thời gian (gap) và không có sự chồng lấn (overlap).

#### C. Giá trị lính canh: Tại sao dùng `9999-12-31` thay vì `NULL`?
Nhiều kỹ sư mới vào nghề thường để cột `valid_to` của bản ghi hiện tại là `NULL`. Tuy nhiên, trong mô hình Kimball chuẩn mực:
* Nếu dùng `NULL`, mọi câu lệnh query bắt buộc phải viết: 
  `WHERE order_date >= valid_from AND (order_date < valid_to OR valid_to IS NULL)`.
  Điều này làm optimizer của Database không thể tận dụng tối đa Index hoặc cơ chế Partition/Cluster Pruning, gây chậm query nghiêm trọng.
* Sử dụng một giá trị "lính canh" trong tương lai xa như `'9999-12-31'` giúp mệnh đề so sánh luôn đồng nhất: `order_date >= valid_from AND order_date < valid_to`.

#### D. Cột cờ `is_current`
Cột `is_current` (hoặc `dbt_is_current`) chỉ mang tính chất hỗ trợ tiện ích (derived convenience flag). Nếu bạn chỉ muốn lấy thông tin mới nhất của khách hàng mà không cần quan tâm đến ngày tháng, bạn chỉ cần lọc `WHERE is_current = true`.

---

### 2. Trọng tâm: Kỹ thuật As-Of Point-in-Time Join

Tại sao bảng SCD2 lại cứu nguy cho báo cáo doanh thu? Hãy nhìn vào phép nối dữ liệu giữa Fact Orders và Dim Customer:

```sql
SELECT 
    f.order_id,
    f.order_date,
    f.amount,
    d.customer_id,
    d.city AS historical_city,
    d.email AS historical_email
FROM fct_orders f
JOIN dim_customer d
  ON f.customer_id = d.customer_id
 AND f.order_date >= d.valid_from 
 AND f.order_date <  d.valid_to;
```

#### Minh họa kết quả thực thi:
Giả sử có 2 đơn hàng:
* Đơn 101 phát sinh ngày `2026-02-10` (Trị giá 50 triệu): Rơi vào khoảng `[2026-01-01, 2026-03-15)` $\to$ Nối với phiên bản 1 $\to$ Thành phố: **Ha Noi**.
* Đơn 102 phát sinh ngày `2026-04-20` (Trị giá 30 triệu): Rơi vào khoảng `[2026-03-15, 2026-06-01)` $\to$ Nối với phiên bản 2 $\to$ Thành phố: **Da Nang**.

#### Thảm họa nếu chỉ `JOIN` bằng Natural Key (`customer_id`):
Nếu viết câu lệnh ngây thơ:
```sql
SELECT f.order_id, d.city, f.amount
FROM fct_orders f
JOIN dim_customer d ON f.customer_id = d.customer_id;
```
Bởi vì `customer_id = 1` xuất hiện **3 lần** trong bảng `dim_customer`, phép nối trên sẽ biến 1 đơn hàng thành 3 dòng kết quả (Cartesian Product / Join Explosion). Doanh thu đơn hàng 101 bị nhân ba lên thành 150 triệu! Đây là lỗi nghiêm trọng nhất mà các Data Analyst hay gặp phải khi truy vấn bảng SCD2 mà không hiểu cấu trúc temporal.

---

## Phần 4: Các Mô hình Triển khai: Từ dbt Snapshot đến SQL Thuần

### 1. Triển khai hiện đại với dbt Snapshot

Trong mã nguồn `scd2_draft.md`, dbt cung cấp giải pháp khai báo cực kỳ thanh lịch thông qua file `dim_customer_snapshot.sql`:

```yaml
{% snapshot dim_customer_snapshot %}
{{
    config(
      target_schema='snapshots',
      unique_key='customer_id',
      strategy='check',
      check_cols=['full_name', 'email', 'city'],
      invalidate_hard_deletes=true
    )
}}
select customer_id, full_name, email, city, updated_at
from {{ source('oltp', 'customers') }}
{% endsnapshot %}
```

#### Phân tích chuyên sâu các tham số cấu hình:
* `target_schema='snapshots'`: Tách bảng lịch sử ra schema quản trị riêng, tránh nhầm lẫn với tầng staging hay mart.
* `unique_key='customer_id'`: Định danh tự nhiên của thực thể nguồn để dbt biết dòng nào đang được cập nhật hay thêm mới.
* `strategy`:
  * `strategy='check'`: dbt tự động tính toán một chuỗi hash MD5 của tất cả các cột được định nghĩa trong `check_cols`. Nếu chuỗi hash hôm nay khác hôm qua $\to$ có biến động $\to$ kích hoạt SCD2.
  * `strategy='timestamp'` (phương án thay thế): Dựa vào cột `updated_at` từ nguồn. Nếu `updated_at > dbt_valid_from`, dbt hiểu rằng dòng đó đã bị sửa đổi. Phương án này chỉ nên dùng khi nguồn OLTP cam kết 100% cập nhật đúng trường `updated_at` mỗi khi có thay đổi.
* `check_cols=['full_name', 'email', 'city']`: **Tuyệt đối không đưa toàn bộ cột vào theo dõi**. Ví dụ: nếu bảng nguồn có cột `last_login_at` cập nhật mỗi giây, nếu đưa vào `check_cols`, bảng snapshot sẽ phình to khủng khiếp và tạo ra hàng triệu bản ghi SCD2 rác. Chỉ chọn những thuộc tính có giá trị phân tích nghiệp vụ.
* `invalidate_hard_deletes=true`: Nếu một khách hàng bị xóa vĩnh viễn khỏi database nguồn (`DELETE FROM customers WHERE id = 1`), dbt sẽ không xóa dòng đó trong kho dữ liệu, mà tự động set `dbt_valid_to = current_timestamp`. Điều này bảo tồn hoàn toàn vết kiểm toán (Audit Trail) phục vụ pháp lý.

Khi chạy lệnh `dbt snapshot`, dbt sẽ tự động bổ sung 4 cột siêu dữ liệu:
* `dbt_scd_id`: Khóa thay thế duy nhất (Hash của `customer_id` + thời gian chạy).
* `dbt_valid_from`: Thời điểm phiên bản bắt đầu có hiệu lực.
* `dbt_valid_to`: Thời điểm phiên bản hết hiệu lực (hoặc `NULL`/`9999-12-31` tùy config).
* `dbt_updated_at`: Thời điểm dbt thực hiện phép kiểm tra.

---

### 2. Bản chất Cơ chế SQL ngầm định (Under the Hood)

Nếu bạn không sử dụng dbt mà phải viết code ETL thủ công trong một incremental pipeline, mô hình chuẩn gồm 2 giai đoạn: **CLOSE** và **INSERT**.

```
[Staging Table (stg_customer)]
             |
             v  So sánh với bản ghi đang Active (is_current = true)
     +-------+-------+
     |               |
Có thay đổi     Chưa từng tồn tại (Mới)
     |               |
     v               |
1. CLOSE:            |
UPDATE valid_to,     |
is_current = false   |
     |               |
     +-------+-------+
             |
             v
         2. INSERT:
         Mở bản ghi mới với valid_to = '9999-12-31'
```

#### Bước 1: Đóng (CLOSE) bản ghi cũ đang active có dữ liệu thay đổi
```sql
UPDATE dim_customer d
SET valid_to = s.updated_at,
    is_current = false
FROM stg_customer s
WHERE d.customer_id = s.customer_id
  AND d.is_current = true
  AND (d.full_name, d.email, d.city) IS DISTINCT FROM (s.full_name, s.email, s.city);
```
*(Lưu ý sử dụng toán tử `IS DISTINCT FROM` trong PostgreSQL/Snowflake để xử lý an toàn giá trị `NULL` mà không làm chết điều kiện so sánh).*

#### Bước 2: Chèn (INSERT) bản ghi mới mở hiệu lực
```sql
INSERT INTO dim_customer (
    customer_id, full_name, email, city, valid_from, valid_to, is_current
)
SELECT 
    s.customer_id,
    s.full_name,
    s.email,
    s.city,
    s.updated_at AS valid_from,
    '9999-12-31'::timestamp AS valid_to,
    true AS is_current
FROM stg_customer s
LEFT JOIN dim_customer d
  ON s.customer_id = d.customer_id AND d.is_current = true
WHERE d.customer_id IS NULL -- Khách hàng hoàn toàn mới
   OR (d.full_name, d.email, d.city) IS DISTINCT FROM (s.full_name, s.email, s.city); -- Đã đóng ở bước 1
```

#### Mẫu Atomic `MERGE INTO` trong Cloud Data Warehouse (BigQuery / Snowflake):
Trong các kho dữ liệu hiện đại, ta có thể dùng lệnh `MERGE` kết hợp với bảng tạm để đảm bảo tính nguyên tử (Atomicity - ACID), ngăn chặn xung đột nếu pipeline chạy đồng thời:
```sql
MERGE INTO dim_customer target
USING (
    -- Tập hợp các bản ghi cần đóng và các bản ghi mới cần thêm
    ...
) source
ON target.customer_sk = source.customer_sk
WHEN MATCHED AND ... THEN 
    UPDATE SET valid_to = source.change_time, is_current = FALSE
WHEN NOT MATCHED THEN 
    INSERT (...) VALUES (...);
```

---

## Phần 5: Những Cạm bẫy Thực tế trong Môi trường Production

### 1. Sự kiện đến trễ (Late-Arriving Facts)
* **Tình huống**: Một thiết bị bán hàng tại cửa hàng (POS) bị mất mạng Internet trong 5 ngày. Khi có mạng trở lại, toàn bộ đơn hàng của ngày 10/01 mới đồng bộ lên Cloud vào ngày 15/01. Tuy nhiên, trong 5 ngày đó, khách hàng đã kịp đổi địa chỉ trên app vào ngày 12/01.
* **Cạm bẫy**: Nếu pipeline gán cứng Fact với bản ghi Dimension đang active (`WHERE is_current = true`), đơn hàng ngày 10/01 sẽ bị gắn nhầm với địa chỉ ngày 12/01.
* **Giải pháp**: Luôn thực hiện phép nối thời gian **As-Of Join** dựa trên `order_timestamp` của sự kiện thực tế, tuyệt đối không dùng `is_current` khi nạp Fact table.

### 2. Chiều biến đổi đến trễ (Late-Arriving Dimension Changes / Hồi tố)
* **Tình huống**: Khách hàng thực tế đã chuyển nhà vào ngày 01/02, nhưng bộ phận chăm sóc khách hàng (CRM) mãi đến ngày 15/03 mới nhập thông tin này vào hệ thống.
* **Cạm bẫy**: dbt snapshot chạy hàng ngày sẽ lấy `valid_from = 2026-03-15`. Toàn bộ đơn hàng khách mua trong khoảng từ 01/02 đến 15/03 vẫn bị gán cho địa chỉ cũ.
* **Giải pháp**: Đây là vấn đề nan giải nhất của SCD2. Nếu muốn sửa đổi hồi tố, Data Engineer phải viết script bảo trì đặc biệt (backfill/restatement) để bẻ đôi dòng lịch sử cũ, điều chỉnh lại mốc `valid_to` của dòng trước đó và chạy lại quy trình gán khóa ngoại cho các Fact table liên quan.

### 3. Va chạm mili-giây (Microsecond Collision & Ordering)
* Trong các hệ thống streaming hoặc xử lý theo lô với tần suất cao (micro-batching), một khách hàng có thể đổi trạng thái 2 lần trong cùng một giây.
* Nếu kiểu dữ liệu của `valid_from` chỉ lưu dạng `DATE` thay vì `TIMESTAMP` (với độ chính xác microsecond), bạn sẽ gặp lỗi: dòng mới mở trước khi dòng cũ kịp đóng, hoặc `valid_from == valid_to` (phiên bản có thời gian tồn tại bằng 0).
* **Quy tắc vàng**: Luôn dùng kiểu dữ liệu `TIMESTAMP_NTZ` hoặc `TIMESTAMPTZ` có độ chính xác cao nhất cho `valid_from` và `valid_to`.

### 4. Hiệu năng của Non-Equi Joins trên quy mô lớn
* Phép nối bằng dấu so sánh (`fact.order_date >= dim.valid_from AND fact.order_date < dim.valid_to`) là một phép nối không bằng nhau (**Non-Equi Join**).
* Các Query Engine dạng cột (columnar) cực kỳ tối ưu cho Equi-Joins (`ON a.id = b.id` sử dụng Hash Join). Khi gặp Non-Equi Joins trên bảng Fact hàng tỷ dòng, hiệu năng có thể sụt giảm nghiêm trọng.
* **Biện pháp tối ưu**:
  * Đặt **Clustering Key** trên bảng Dimension theo cặp `(customer_id, valid_from)`.
  * Trong tầng Data Warehouse nạp Fact, thực hiện việc Lookup tìm ra `customer_sk` cụ thể ngay lúc nạp dữ liệu (ETL pipeline gán sẵn `customer_sk` vào Fact table). Nhờ đó, người dùng cuối và Dashboard chỉ cần `JOIN` bằng Equi-Join thông thường:
    ```sql
    SELECT * FROM fct_orders f JOIN dim_customer d ON f.customer_sk = d.customer_sk;
    ```

---

## Phần 6: Các Giải pháp Thay thế và Trường phái Hiện đại

Liệu SCD Type 2 có phải là giải pháp duy nhất? Trong kỷ nguyên Cloud và Big Data, kiến trúc sư dữ liệu có thêm nhiều vũ khí mạnh mẽ khác:

| Tiêu chí | SCD Type 2 (Kimball) | One Big Table (OBT / Denormalization) | Bitemporal Modeling (2 Dòng Thời gian) | Lakehouse Time Travel (Iceberg / Delta) |
| :--- | :--- | :--- | :--- | :--- |
| **Cơ chế** | Phân phiên bản Dimension bằng `valid_from`, `valid_to`. | "Đóng băng" thuộc tính Dimension trực tiếp vào Fact khi tạo dòng Fact. | Theo dõi song song: **Valid Time** (thực tế) & **Transaction Time** (hệ thống). | Dựa vào Snapshot Metadata của định dạng file lưu trữ Parquet. |
| **Ưu điểm** | Chuẩn hóa, tiết kiệm dung lượng, tái sử dụng trên mọi Mart, hỗ trợ mô hình hóa quan hệ. | Query siêu nhanh, không cần JOIN, không sợ Join Explosion, cực hợp BigQuery/ClickHouse. | Đáp ứng kiểm toán tuyệt đối; sửa đổi quá khứ không làm mất dấu vết lúc nhập liệu. | Không cần thiết kế schema SCD2 phức tạp; xem lại trạng thái bảng bằng 1 câu lệnh SQL. |
| **Nhược điểm** | Query phức tạp (Non-Equi Join); khó xử lý khi có thay đổi hồi tố. | Lãng phí dung lượng; nếu thuộc tính bị nhập sai chính tả thì không thể sửa đồng loạt. | Cực kỳ phức tạp trong thiết kế schema và câu lệnh truy vấn SQL. | Metadata snapshot thường bị xóa sau 7-30 ngày (retention limit); không thể join động 5 năm. |
| **Khi nào nên chọn?** | Hệ thống EDW chuẩn mực, phục vụ đa phòng ban, cần quản trị dữ liệu chặt chẽ. | Dashboard phục vụ người dùng cuối đòi hỏi độ trễ mili-giây; dữ liệu log/clickstream. | Ngân hàng, bảo hiểm, thị trường chứng khoán, tính toán rủi ro tài chính. | Phục vụ khôi phục dữ liệu sau thảm họa (Disaster Recovery), audit pipeline kỹ thuật ngắn hạn. |

---

## Phần 7: Ma trận Quyết định & Câu hỏi Thảo luận Chuyên sâu

### 1. Ma trận Quyết định: Khi nào KHÔNG nên dùng SCD Type 2?

Một lỗi phổ biến của các Data Engineer trẻ là "thần thánh hóa" SCD2 và áp dụng nó lên mọi bảng dữ liệu. Hãy cẩn trọng:
1. **Dữ liệu biến động quá nhanh (Rapidly Changing Attributes)**: Số dư tài khoản ngân hàng, tọa độ GPS xe giao hàng, số bước chân đo từ smartwatch. Những dữ liệu này biến thiên liên tục từng phút. Nếu dùng SCD2, bảng Dimension sẽ tăng từ vài triệu lên hàng tỷ dòng. $\to$ **Giải pháp**: Xem các biến động này là sự kiện (Fact) hoặc tách ra bảng Mini-Dimension (SCD4).
2. **Thuộc tính không có giá trị lịch sử**: Khách hàng sửa lỗi gõ sai chữ hoa/thường trong tên ("nguyen van a" $\to$ "Nguyen Van A"), hoặc cập nhật avatar URL. $\to$ **Giải pháp**: Dùng SCD Type 1 (ghi đè).
3. **Cần tối ưu tốc độ đọc tuyệt đối cho Dashboard**: Nếu bài toán kinh doanh chỉ cần biết thông tin khách hàng tại thời điểm đặt hàng và không bao giờ có nhu cầu phân tích lại theo góc nhìn khác. $\to$ **Giải pháp**: Denormalize thẳng vào Fact table (OBT).

---

### 2. Các câu hỏi Thảo luận & Phỏng vấn Chuyên sâu

#### Câu hỏi 1: Hiện tượng "Bùng nổ dữ liệu" (Join Explosion)
> *Một Data Analyst phàn nàn: "Tôi vừa nối bảng `fct_orders` với `dim_customer` để lấy tên thành phố, nhưng tổng doanh thu tháng này bỗng nhiên tăng gấp đôi so với thực tế!" Bạn giải thích nguyên nhân cho họ như thế nào và cách khắc phục ra sao?*
* **Trả lời**: Nguyên nhân do Analyst đã `JOIN` trực tiếp qua Natural Key `customer_id` trên bảng `dim_customer` chuẩn SCD2. Do một khách hàng có nhiều phiên bản (dòng) đại diện cho các lần thay đổi thông tin trong lịch sử, phép nối đã nhân bản các đơn hàng lên nhiều lần. Cách khắc phục: Bổ sung điều kiện thời gian As-Of Join (`order_date >= valid_from AND order_date < valid_to`) hoặc chỉ lọc phiên bản hiện tại (`is_current = true`) nếu bài toán chỉ yêu cầu thông tin mới nhất.

#### Câu hỏi 2: Lựa chọn Chiến lược dbt Snapshot: `check` vs `timestamp`
> *Khi cấu hình `dbt snapshot`, khi nào bạn nên dùng `strategy='check'` thay vì `strategy='timestamp'`? Rủi ro lớn nhất của `timestamp` là gì?*
* **Trả lời**: Dùng `strategy='check'` khi hệ thống OLTP nguồn không có cột `updated_at`, hoặc cột này không đáng tin cậy (ví dụ: DBA chạy update trực tiếp bằng tay bỏ qua trigger, hoặc ứng dụng chỉ ghi nhận `updated_at` cho một số trường nhất định). Rủi ro lớn nhất của `strategy='timestamp'` là nếu bảng nguồn có cập nhật dữ liệu nhưng `updated_at` không nhảy số, dbt snapshot sẽ hoàn toàn bỏ qua thay đổi đó, dẫn tới mất dấu lịch sử vĩnh viễn.

#### Câu hỏi 3: Lakehouse Time-Travel có thay thế được SCD Type 2?
> *Một kỹ sư dữ liệu phát biểu: "Chúng tôi đã chuyển sang dùng Apache Iceberg. Định dạng này hỗ trợ query `FOR SYSTEM_TIME AS OF ...` nên chúng tôi không cần tốn công xây dựng bảng SCD Type 2 nữa." Quan điểm này đúng hay sai? Tại sao?*
* **Trả lời**: Quan điểm này sai về mặt bản chất nghiệp vụ.
  1. Time-travel trong Iceberg theo dõi **System Time / Transaction Time** (thời điểm file Parquet được ghi xuống đĩa), không phải **Business / Effective Time** (thời điểm khách hàng thực sự chuyển nhà trong đời thực).
  2. Snapshot metadata của Iceberg chỉ được lưu giữ trong một khoảng thời gian giới hạn (thường từ 7 đến 30 ngày) để tránh phình dung lượng lưu trữ (phải chạy lệnh dọn dẹp `vacuum/expire_snapshots`). Bạn không thể time-travel về 3 năm trước.
  3. Bạn không thể thực hiện một câu lệnh `JOIN` giữa 1 triệu đơn hàng trải dài trong 5 năm với một bảng Iceberg duy nhất bằng cú pháp time-travel, bởi vì mỗi đơn hàng cần một mốc snapshot thời gian khác nhau. SCD Type 2 giải quyết trọn vẹn bài toán này trong một cấu trúc bảng quan hệ phẳng duy nhất.

#### Câu hỏi 4: Tạo Surrogate Key trong Kỷ nguyên Phân tán
> *Tại sao trong các kho dữ liệu hiện đại như Snowflake hay BigQuery, việc tạo Surrogate Key bằng số tự tăng (`AUTOINCREMENT` / `IDENTITY`) thường bị tránh sử dụng, và thay vào đó người ta chuộng mã Hash (MD5 / SHA256)?*
* **Trả lời**: Trong kiến trúc tính toán phân tán (Distributed Massively Parallel Processing - MPP), việc sinh số nguyên tuần tự tự tăng đòi hỏi một node điều phối trung tâm phải khóa (lock) và đồng bộ hóa trạng thái toàn cục để đảm bảo không trùng số giữa các worker nodes, gây tắc nghẽn hiệu năng nghiêm trọng. Sử dụng mã băm:
  $$\text{surrogate\_key} = \text{MD5}(\text{customer\_id} \,\|\, \text{valid\_from})$$
  cho phép mọi worker node tính toán khóa chính một cách độc lập tuyệt đối, có tính tất định (deterministic), có thể chạy lại bao nhiêu lần vẫn ra cùng kết quả (idempotent), hoàn hảo cho mô hình ELT hiện đại.
