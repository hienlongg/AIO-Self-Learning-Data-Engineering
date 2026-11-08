## 1. Tư duy nền tảng: SCD1 vs SCD2

| | **SCD Type 1** | **SCD Type 2** |
|---|---|---|
| Thay đổi | Ghi đè bản cũ | Đóng bản cũ, mở bản mới |
| Lịch sử | **Mất** | **Giữ toàn bộ** |
| Mỗi entity | 1 dòng | Nhiều dòng, mỗi dòng = 1 "phiên bản" |
| Khi nào dùng | Sửa lỗi dữ liệu, thông tin không cần audit | Cần biết "khách hàng ở đâu **tại thời điểm** đơn hàng phát sinh" |

**Câu hỏi then chốt:** *"Nếu thuộc tính này thay đổi, các báo cáo quá khứ có được phép thay đổi theo không?"* — Nếu không → SCD2.

## 2. Bản demo chạy ở trên — kết quả SCD2

Customer 1 đổi city rồi đổi email → dim giữ **3 dòng**:

| customer_sk | customer_id | email | city | valid_from | valid_to | is_current |
|---|---|---|---|---|---|---|
| 1 | 1 | an@x.com | Ha Noi | 2026-01-01 | 2026-03-15 | 0 |
| 2 | 1 | an@x.com | Da Nang | 2026-03-15 | 2026-06-01 | 0 |
| 3 | 1 | an.x@gmail.com | Da Nang | 2026-06-01 | 9999-12-31 | 1 |

Điểm mấu chốt của **effective dating**:
- `valid_from` = ngày change event phát sinh; `valid_to` = ngày bản ghi **kết thúc** (`9999-12-31` = còn hiệu lực).
- **`valid_to` là exclusive** — bản ghi mới mở đúng ngày bản ghi cũ đóng, không overlap, không gap.
- **As-of join** (đơn hàng join dim đúng tại thời điểm đó): `order_date >= valid_from AND order_date < valid_to`. Đơn 101 (02/2026) gắn với Ha Noi, đơn 102 (04/2026) gắn Da Nang — đó chính là lý do SCD2 tồn tại.

## 3. Code dbt thực tế — dùng `snapshot` (đúng chuẩn dbt, không cần tự viết merge)

```yaml
# snapshots/dim_customer_snapshot.sql
{% snapshot dim_customer_snapshot %}
{{
    config(
      target_schema='snapshots',
      unique_key='customer_id',
      strategy='check',
      check_cols=['full_name', 'email', 'city'],   -- cột theo dõi thay đổi
      invalidate_hard_deletes=true                  -- hard delete → valid_to bị đóng
    )
}}
select customer_id, full_name, email, city, updated_at
from {{ source('oltp', 'customers') }}
{% endsnapshot %}
```

dbt tự sinh ra đúng kết cấu trên: `dbt_scd_id`, `dbt_valid_from`, `dbt_valid_to`, `dbt_is_current` — và tự chạy merge mỗi `dbt snapshot`.

Nếu bạn cần snapshot **thủ công trong model** (vd. kimball-style incremental model), pattern tương đương với demo trên:

```sql
-- 1. CLOSE: đóng bản ghi hiện tại có thay đổi
update dim_customer d
set valid_to = s.updated_at, is_current = false
from stg_customer s
where d.customer_id = s.customer_id
  and d.is_current = true
  and (d.full_name, d.email, d.city) is distinct from (s.full_name, s.email, s.city);

-- 2. INSERT: mở bản ghi mới (anti-join)
insert into dim_customer (customer_id, full_name, email, city, valid_from, valid_to, is_current)
select s.customer_id, s.full_name, s.email, s.city, s.updated_at, '9999-12-31', true
from stg_customer s
left join dim_customer d
  on d.customer_id = s.customer_id and d.is_current = true
where d.customer_id is null
   or (d.full_name, d.email, d.city) is distinct from (s.full_name, s.email, s.city);
```

## 4. Những cạm bẫy thực tế cần nhớ

- **Late-arriving changes**: event đến muộn hơn `updated_at` → cần logic backfill hoặc dùng event time thay vì load time.
- **`is_current` flag là derived** — có thể suy ra từ `valid_to = '9999-12-31'`; flag chỉ là tiện cho query, không phải nguồn sự thật.
- **SCD3/4/6**: Type 3 = thêm cột "previous value" (chỉ giữ 1 bước lịch sử); Type 4 = bảng history riêng + bảng current; Type 6 = hybrid (3 cột + Type 2). Đa số case chỉ cần Type 2.
- **Surrogate key** (`customer_sk`) bắt buộc — natural key (`customer_id`) không unique trong dim SCD2, fact table phải join bằng SK.
- **Hard delete**: `invalidate_hard_deletes=true` đóng bản ghi thay vì xóa, giữ audit trail.

Muốn mình mở rộng phần nào không — ví dụ late-arriving dimension, SCD2 trong BigQuery với `MERGE` statement, hay cách test SCD2 trong dbt?