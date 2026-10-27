# Tài liệu đọc: Làm chủ Biến đổi Dữ liệu Luồng (Streaming Transformations) — Từ Kafka Ingestion đến Lakehouse Sinks với Spark Structured Streaming

---

## Lời mở đầu

Hãy hình dung một kịch bản thực tế trong một hệ thống thương mại điện tử hoặc fintech quy mô lớn:

> 11:59:50 trưa ngày Siêu Sale 11/11, hàng triệu người dùng đồng loạt nhấn nút "Đặt hàng". 
> Các sự kiện click chuột (`ad_clicks`), giao dịch thanh toán (`payments`), và cập nhật trạng thái đơn hàng (`order_status`) liên tục đổ về hệ thống hàng trăm nghìn sự kiện mỗi giây thông qua Apache Kafka.
> 
> Bộ phận Quản trị Rủi ro (Fraud Detection) đặt ra yêu cầu: *"Cần phát hiện ngay lập tức các giao dịch có dấu hiệu gian lận — ví dụ một tài khoản phát sinh thanh toán ở hai quốc gia khác nhau trong vòng 5 phút, hoặc một giao dịch thanh toán thành công nhưng không gắn liền với bất kỳ sự kiện xem trang (ad click) nào trước đó trong vòng 10 phút."*

Nếu bạn xử lý bài toán này theo mô hình Batch truyền thống (chờ dữ liệu gom đủ 1 giờ rồi chạy Spark job), khi phát hiện ra gian lận thì kẻ xấu đã kịp rút sạch tiền khỏi tài khoản. 

Nhưng nếu chuyển sang xử lý theo thời gian thực (Real-time Stream Processing), bạn sẽ ngay lập tức đối mặt với hàng loạt cạm bẫy hóc búa:
* **Độ trễ và dữ liệu đến muộn (Late Data)**: Thiết bị di động của người dùng bị mất sóng 4G trong thang máy, sự kiện click phát sinh lúc 11:55 nhưng mãi đến 12:15 mới được đồng bộ lên Kafka. Làm sao hệ thống biết khi nào nên chờ và khi nào thì "bỏ cuộc" để không tốn bộ nhớ vô hạn?
* **Tràn bộ nhớ máy chủ (JVM Out of Memory - OOM)**: Một phép khử trùng lặp (`dropDuplicates`) hoặc một phép ghép nối hai luồng sự kiện (Stream-Stream Join) chạy được 3 ngày thì sụp đổ vì bộ nhớ lưu trữ trạng thái (State Store) phình to hàng trăm Gigabytes.
* **Sai lệch dữ liệu lịch sử (Data Drift / Stale Cache)**: Làm giàu dữ liệu đơn hàng với bảng danh mục khách hàng, nhưng khách hàng vừa nâng cấp lên hạng VIP thì luồng dữ liệu vẫn hiển thị hạng Thường suốt nhiều ngày liền.
* **Nhân bản dữ liệu khi gặp sự cố (Duplicates on Retries)**: Khi một node tính toán bị crash và khởi động lại, các bản ghi trong micro-batch đó có bị ghi đè hai lần vào Data Lake hay không?

Tài liệu này được biên soạn dựa trên bản phác thảo kỹ thuật `docs/project-drafts/streaming-transformations-draft.md`, đóng vai trò là cẩm nang chuyên sâu giúp bạn làm chủ:
1. Bản chất của xử lý luồng, sự khác biệt giữa phép biến đổi **Không lưu trạng thái (Stateless)** và **Có lưu trạng thái (Stateful)**.
2. Cơ chế kiểm soát bộ nhớ qua **Watermark (Ngưỡng mực nước)** và kỹ thuật ghép nối luồng với luồng (**Stream-Stream Joins**).
3. Các kỹ thuật lưu trữ đầu ra (**Sinks**), giải quyết bài toán chống trùng lặp dữ liệu với **`foreachBatch`** và **Idempotent Upsert (ACID MERGE)** trên Data Lakehouse.
4. Cơ chế tự phục hồi trước dữ liệu hỏng hóc qua kiến trúc **Dead-Letter Queue (DLQ)** và quản trị tiến hóa lược đồ (**Schema Evolution**).

---

## Phần 1: Nền tảng Xử lý Dòng (Streaming Paradigms) & Sứ mệnh của Real-Time Data Engineer

```
┌────────────────────────────────────────────────────────────────────────┐
│                        BATCH VS. STREAMING PARADIGM                    │
├───────────────────────────────────┬────────────────────────────────────┤
│         Batch (Bounded)           │        Streaming (Unbounded)       │
├───────────────────────────────────┼────────────────────────────────────┤
│ • Tập dữ liệu hữu hạn (chốt kỳ)   │ • Dòng dữ liệu vô hạn (bất tận)    │
│ • Xử lý theo lô định kỳ (giờ/ngày)│ • Xử lý tức thời / micro-batch nhỏ │
│ • Lỗi -> chạy lại toàn bộ lô      │ • Lỗi -> khôi phục từ checkpoint   │
│ • Thời gian thực thi tĩnh         │ • Đối mặt với độ trễ và mất trật tự│
└───────────────────────────────────┴────────────────────────────────────┘
```

### 1. Tập dữ liệu Hữu hạn (Bounded) vs. Vô hạn (Unbounded)
Trong xử lý dữ liệu truyền thống (Batch Processing), dữ liệu được coi là một tập hợp **đã đóng gói hoàn chỉnh** (Bounded Dataset). Khi bạn chạy một câu truy vấn trên phân vùng `date = '2026-03-20'`, bạn biết chắc chắn không có dòng dữ liệu nào của ngày đó tự nhiên xuất hiện thêm trong quá trình tính toán.

Ngược lại, thế giới thực hoạt động theo nguyên lý **Unbounded Dataset**: người dùng liên tục click chuột, cảm biến IoT liên tục gửi tín hiệu nhiệt độ, máy chủ tài chính liên tục ghi nhận giao dịch. Dòng dữ liệu không bao giờ có điểm kết thúc.

Nhiệm vụ của **Kỹ sư Dữ liệu Thời gian thực (Real-Time Data Engineer)** là thiết kế một hệ sinh thái tính toán có khả năng:
* Nuốt liên tục các sự kiện với thông lượng (throughput) cao.
* Duy trì tính nhất quán (consistency) ngay cả khi phần cứng mạng hay máy chủ bị sập đột ngột.
* Trả lời được câu hỏi kinh điển: *"Dữ liệu này được xử lý theo thời gian nào?"*

---

### 2. Ba trục thời gian trong Xử lý Dòng (Time Semantics)

Một trong những sai lầm phổ biến nhất của các kỹ sư mới bước vào streaming là sử dụng thời gian của máy chủ đang chạy code (`System.currentTimeMillis()`) để tính toán nghiệp vụ. 

Trong hệ thống phân tán, có 3 mốc thời gian hoàn toàn tách biệt:

```
[ Sự kiện xảy ra ] ──▶ [ Đến Kafka Broker ] ──▶ [ Executor tính toán ]
   10:00:00 AM             10:02:15 AM               10:05:00 AM
  (Event Time)           (Ingestion Time)          (Processing Time)
```

1. **Event Time (Thời gian sự kiện)**: Thời điểm thực tế mà hành động diễn ra tại nguồn (được đính kèm trực tiếp trong payload của message, ví dụ `click_timestamp`). Đây là mốc thời gian mang **giá trị nghiệp vụ duy nhất đúng**.
2. **Ingestion Time (Thời gian nạp)**: Thời điểm message được ghi nhận và lưu trữ vào phân vùng của Kafka broker.
3. **Processing Time (Thời gian xử lý)**: Thời điểm mà cỗ máy tính toán (Spark Executor) thực sự đọc và xử lý bản ghi đó.

#### Tại sao Event Time lại khiến bài toán trở nên phức tạp?
Do độ trễ mạng, thiết bị người dùng mất kết nối, hoặc nghẽn hàng đợi (backpressure), các sự kiện không bao giờ đến theo đúng thứ tự thời gian tuyến tính. Một sự kiện xảy ra lúc `10:00:00` có thể bị gửi tới Kafka muộn hơn một sự kiện xảy ra lúc `10:04:00`. Hiện tượng này được gọi là **Out-of-order data (Dữ liệu đến đảo lộn trật tự)** và **Late-arriving data (Dữ liệu đến muộn)**. 

Mọi biến đổi luồng có ý nghĩa nghiệp vụ (tính doanh thu theo khung giờ, ghép cặp đơn hàng) bắt buộc phải dựa trên **Event Time**, và vì thế, hệ thống bắt buộc phải có cơ chế xác định: *"Cần đợi dữ liệu đến muộn trong bao lâu?"* — Đó là lý do khái niệm **Watermark** ra đời.

---

## Phần 2: Giải phẫu Kiến trúc — Ingestion, State Store & Khả năng Chịu lỗi

Bản phác thảo mở đầu bằng mô hình tổng quan kinh điển:

```
┌───────┐   ┌────────────────────────────┐   ┌──────────┐
│ Kafka │──▶│ Spark Structured Streaming │──▶│ Storage  │
│ topic │   │ Engine (Micro-batch / WAL) │   │  sink    │
└───────┘   └────────────────────────────┘   └──────────┘
                  transformations
```

Để hiểu trọn vẹn cách đoạn mã khởi tạo hoạt động, chúng ta cần mổ xẻ cơ chế bên trong của từng mắt xích.

---

### 1. Apache Kafka — Tầng Ingestion
Kafka đóng vai trò là một **Append-Only Distributed Log**. Mỗi topic được chia thành nhiều **Partitions**.

Trong đoạn mã:
```python
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker1:9092") \
    .option("subscribe", "events") \
    .load()
```

Spark không hoạt động như một Kafka Consumer thông thường với consumer group quản lý offset tự động trên broker. Thay vào đó, **Spark Driver trực tiếp thăm dò (poll) Kafka metadata**, xác định các offset mới nhất trên từng partition, và phân bổ các dải offset này cho các Spark Executor đọc song song. 

**Số lượng partition của Kafka topic quyết định mức độ song song hóa (parallelism) tối đa ở tầng đọc dữ liệu của Spark.**

#### Cấu trúc dòng dữ liệu thô từ Kafka
DataFrame `df` trả về từ Kafka có cấu trúc cố định gồm các cột:
* `key` (binary): Khóa của message.
* `value` (binary): Toàn bộ nội dung payload của message.
* `topic` (string), `partition` (int), `offset` (long).
* `timestamp` (timestamp): Thời gian nạp vào Kafka (Ingestion Time) hoặc do client gán.

Vì `value` là một mảng byte nhị phân, lập trình viên bắt buộc phải thực hiện bước ép kiểu và giải mã cấu trúc:

```python
events = df.select(
    F.from_json(F.col("value").cast("string"), event_schema).alias("e"),
    F.col("timestamp")
).select("e.*", "timestamp")
```
* Bước 1: `F.col("value").cast("string")` chuyển mảng byte thành chuỗi ký tự JSON.
* Bước 2: `F.from_json(..., event_schema)` đối chiếu chuỗi JSON với lược đồ định nghĩa trước (`StructType`) để chuyển thành một đối tượng struct có kiểu dữ liệu rõ ràng.
* Bước 3: `select("e.*", "timestamp")` làm phẳng (flatten) toàn bộ các trường bên trong struct ra thành các cột riêng rẽ của DataFrame.

---

### 2. Khả năng chịu lỗi: Write-Ahead Log (WAL) và Checkpoint

Trong thế giới Streaming, một câu hỏi sống còn là: **"Nếu máy chủ bị mất điện đột ngột ngay giữa quá trình tính toán, làm sao hệ thống khởi động lại mà không làm mất dữ liệu hoặc tính trùng hai lần?"**

Câu trả lời nằm ở tùy chọn bắt buộc:
```python
.option("checkpointLocation", "/mnt/checkpoints/events")
```

Thư mục checkpoint trên hệ thống lưu trữ phân tán bền vững (S3 / HDFS / ADLS) chứa 3 thành phần cốt lõi:
1. **`offsets/`**: Lưu lại chính xác dải offset (từ offset $A$ đến offset $B$ trên từng partition của Kafka) mà micro-batch hiện tại được phân công đọc.
2. **`commits/`**: Ghi nhận một file metadata xác nhận rằng micro-batch số $N$ đã hoàn tất việc xử lý và ghi xuống sink thành công. Nếu Spark sập trước khi file commit này được tạo ra, khi khởi động lại, Spark sẽ đọc lại đúng dải offset của batch $N$ đó để tính toán lại.
3. **`state/`**: Nơi lưu trữ trạng thái trung gian của các phép biến đổi có trạng thái (Stateful operations) để phục hồi bộ nhớ.

---

### 3. Phân biệt Bản chất: Stateless vs. Stateful Transformations

Đây là ranh giới kỹ thuật quan trọng nhất trong toàn bộ tài liệu:

| Đặc tính | Phép biến đổi Không trạng thái (Stateless) | Phép biến đổi Có trạng thái (Stateful) |
|---|---|---|
| **Định nghĩa** | Đầu ra của dòng $i$ chỉ phụ thuộc duy nhất vào dữ liệu của chính dòng $i$ đó. | Đầu ra phụ thuộc vào dòng hiện tại **VÀ** các dòng đã xảy ra trong quá khứ. |
| **Ví dụ** | `filter()`, `select()`, `withColumn()`, `map()`. | `dropDuplicates()`, `groupBy().count()`, Stream-Stream Joins. |
| **Bộ nhớ (Memory)** | **Bằng 0**. Xử lý xong dòng nào giải phóng bộ nhớ dòng đó ngay lập tức. | **Tích lũy theo thời gian**. Cần một State Store để lưu lại lịch sử. |
| **Khả năng mở rộng** | Vô hạn (Horizontally Infinite). Dễ dàng tăng số lượng core để scale. | Bị giới hạn bởi kích thước State Store và chi phí đồng bộ hóa I/O. |
| **Sự cố thường gặp** | Gần như không thể gây sập bộ nhớ nếu kích thước micro-batch hợp lý. | Dễ dẫn tới **OOM (Out Of Memory)** nếu không kiểm soát việc giải phóng trạng thái (State Eviction). |

---

## Phần 3: Phân tích Kỹ thuật Chuyên sâu qua Mã nguồn `streaming-transformations-draft.md`

Bản phác thảo chia các biến đổi thành 4 trụ cột kỹ thuật. Chúng ta sẽ giải phẫu từng trụ cột dưới góc nhìn nội tại của engine.

---

### Trụ cột 1: Phép lọc (Filtering) & Khử trùng lặp (Deduplication)

#### 1. Phép lọc vô hướng (Stateless Filtering)
```python
# Keep only high-value transactions
stream = events.filter(
    (F.col("amount") > 1000) &
    (F.col("country").isin("US", "DE", "JP")) &
    F.col("event_type").isNotNull()
)
```
* **Bản chất**: Đây là thao tác lọc thuần túy cấp độ dòng (row-level predicate).
* **Cơ chế tối ưu**: Spark Catalyst Optimizer có thể đẩy trực tiếp (pushdown) các biểu thức lọc này vào các phân vùng thực thi mà không tiêu tốn thêm bất kỳ byte RAM nào giữa các micro-batch. Bạn có thể nối hàng chục bộ lọc liên tiếp mà không sợ làm suy giảm hiệu năng bộ nhớ.

#### 2. Khử trùng lặp dòng (Stateful Deduplication)
```python
# Khử trùng lặp theo event_id
deduped_stream = events.withWatermark("timestamp", "1 hour") \
                       .dropDuplicates(["event_id", "timestamp"])
```

* **Cạm bẫy tiềm ẩn**: Khi bạn gọi `dropDuplicates(["event_id"])`, làm thế nào Spark biết một `event_id` đã từng xuất hiện hay chưa? Nó bắt buộc phải lưu toàn bộ danh sách `event_id` đã từng nhìn thấy vào trong **State Store**!
* Nếu một topic Kafka nhận 100 triệu sự kiện mỗi ngày, sau 10 ngày, State Store sẽ phải chứa 1 tỷ khóa. Bộ nhớ của JVM sẽ nhanh chóng cạn kiệt, dẫn đến hiện tượng Garbage Collection (GC) tê liệt và sập executor.
* **Quy tắc bắt buộc**: Khi sử dụng `dropDuplicates` trong môi trường streaming sản xuất, **bắt buộc phải đi kèm với `withWatermark` và đưa cột thời gian sự kiện vào trong khóa khử trùng lặp**. Khi đó, Spark sẽ tự động dọn dẹp (evict) các `event_id` đã cũ hơn ngưỡng watermark ra khỏi bộ nhớ.

---

### Trụ cột 2: Làm giàu Dữ liệu (Enrichment - Stream-Static Joins)

Trong các kiến trúc xử lý dữ liệu, stream sự kiện thường chỉ chứa các định danh trần trụi (ví dụ `customer_id: "C123"`). Để tạo ra các bảng phân tích có nghĩa, ta cần "làm giàu" sự kiện đó với thông tin chi tiết của khách hàng (`name`, `email`, `segment`).

```python
# Load reference data as a batch DataFrame
customers = spark.read.parquet("/data/customers")  # broadcast if small

enriched = events.join(
    F.broadcast(customers),
    "customer_id",
    "left"
)
```

```
┌───────────────────────────────────────────────┐
│              BROADCAST HASH JOIN              │
│                                               │
│  [customers] (Small Table ~ 50MB)             │
│       │                                       │
│       ├──▶ Driver đọc Parquet                 │
│       └──▶ Sao chép (Broadcast) đến TẤT CẢ    │
│            các Executor Node trong cluster    │
│                                               │
│  [Stream Events] ──▶ Join trực tiếp tại RAM  │
│  (Không cần Shuffle qua mạng giữa các máy)    │
└───────────────────────────────────────────────┘
```

#### Các lỗi kinh điển trong Stream-Static Joins:

##### Lỗi 1: Cạm bẫy Dữ liệu Tĩnh bị Đóng băng (The Stale Cache Bug)
* **Hiện tượng**: Bạn khởi chạy streaming query. Một khách hàng mới đăng ký tài khoản lúc 14:00. Bảng `customers` dưới kho dữ liệu đã được cập nhật bản ghi này. Nhưng luồng `enriched` ghi nhận đơn hàng lúc 14:05 vẫn có trường thông tin khách hàng bị `NULL`!
* **Nguyên nhân**: Lệnh `customers = spark.read.parquet("/data/customers")` được thực thi **một lần duy nhất tại thời điểm khởi động query**. Spark Driver nạp dữ liệu này vào bộ nhớ và giữ nguyên bản snapshot đó. Trong suốt nhiều tuần chạy liên tục, streaming query hoàn toàn "mù" trước các thay đổi mới của bảng `customers`.
* **Giải pháp khắc phục**:
  1. *Khởi động lại định kỳ (Scheduled Restart)*: Restart streaming query mỗi đêm (phù hợp nếu bảng danh mục chỉ cập nhật theo ngày).
  2. *Tái nạp trong `foreachBatch`*: Chuyển thao tác đọc bảng tĩnh vào bên trong hàm thực thi của từng micro-batch (xem chi tiết ở Phần 4).
  3. *Truy vấn trực tiếp Cache ngoài (External Key-Value Lookup)*: Với dữ liệu yêu cầu cập nhật tức thì (hot dimension), thực hiện truy vấn điểm (point-lookup) qua Redis hoặc Cassandra.

##### Lỗi 2: Bùng nổ Dòng Dữ liệu (The Fan-Out Explosion)
* Nếu trong bảng `customers`, vì một lỗi nghiệp vụ nào đó mà `customer_id = 'C123'` xuất hiện 3 dòng (do trùng lặp dữ liệu nguồn), một sự kiện giao dịch đơn lẻ của khách hàng này khi đi qua phép `LEFT JOIN` sẽ lập tức bị nhân bản thành 3 dòng giao dịch giống hệt nhau!
* **Quy tắc an toàn**: Luôn luôn đảm bảo bảng tham chiếu tĩnh được khử trùng lặp tuyệt đối trên khóa join trước khi đưa vào biểu thức làm giàu.

---

### Trụ cột 3: Ghép nối Luồng với Luồng (Stream-Stream Joins)

Đây là đỉnh cao của độ phức tạp trong xử lý luồng, nơi cả hai vế tham gia phép nối đều là những dòng dữ liệu di chuyển liên tục theo thời gian.

Hãy xem xét đoạn mã trong bản phác thảo:
```python
left = leftStream.withWatermark("left_ts", "2 hours")
right = rightStream.withWatermark("right_ts", "2 hours")

joined = left.join(
    right,
    expr(""" 
        click_id = view_id AND
        right_ts >= left_ts - INTERVAL 10 MINUTES AND
        right_ts <= left_ts + INTERVAL 10 MINUTES
    """),
    "leftOuter"
)
```

```
           Stream Left (Clicks)                  Stream Right (Views)
                    │                                     │
                    ▼                                     ▼
        ┌──────────────────────┐              ┌──────────────────────┐
        │ State Store Buffer A │              │ State Store Buffer B │
        └──────────┬───────────┘              └──────────┬───────────┘
                   │                                     │
                   └───────────────┬─────────────────────┘
                                   │
                      Khớp theo click_id = view_id
                     VÀ trong khoảng thời gian [-10m, +10m]
                                   │
                                   ▼
                             [ Joined Stream ]
```

#### 1. Tại sao Stream-Stream Join lại cần Bộ đệm Kép (Dual Buffer)?
Khi một sự kiện click chuột xuất hiện trên `leftStream`, sự kiện xem quảng cáo (`rightStream`) tương ứng có thể chưa tới (hoặc đã tới từ 5 phút trước). 
Spark bắt buộc phải đưa sự kiện click vào một **Bộ đệm trạng thái (Buffer A)** và chờ đợi. Ngược lại, khi sự kiện xem quảng cáo xuất hiện, nó cũng được đưa vào **Bộ đệm trạng thái (Buffer B)**. Với mỗi sự kiện mới ở một bên, Spark sẽ quét qua bộ đệm của bên kia để tìm kiếm bản ghi trùng khớp.

#### 2. Điều kiện Khoảng thời gian (Interval Constraint) là gì?
Nếu người dùng click vào một quảng cáo, họ sẽ chỉ mua hàng hoặc xem chi tiết trong vòng vài phút hoặc vài giờ sau đó. Không bao giờ có chuyện họ xem quảng cáo hôm nay nhưng 3 năm sau mới click vào đúng phiên đó.
Biểu thức:
```sql
right_ts >= left_ts - INTERVAL 10 MINUTES AND right_ts <= left_ts + INTERVAL 10 MINUTES
```
thiết lập một **ranh giới thời gian hữu hạn** cho phép nối. Nó tuyên bố với Spark rằng: một sự kiện bên phải chỉ có thể khớp với sự kiện bên trái nếu độ lệch thời gian giữa chúng không vượt quá 10 phút.

#### 3. Vai trò Tối thượng của Watermark trong Stream-Stream Outer Join
Nếu bạn thực hiện phép `leftOuter` join (giữ lại toàn bộ sự kiện click, ngay cả khi không có lượt xem tương ứng):
* Làm sao Spark biết được một lượt click **chắc chắn sẽ không bao giờ** có lượt xem xuất hiện trong tương lai để phát ra dòng kết quả với các trường bên phải là `NULL`?
* Nếu không có Watermark, Spark sẽ phải chờ đợi vĩnh viễn! Bản ghi click sẽ nằm trong bộ đệm RAM mãi mãi.
* **Nguyên lý Watermark**: Khi Watermark của `rightStream` vượt qua mốc `left_ts + 10 MINUTES`, Spark chính thức kết luận: *"Hạn chót 10 phút đã hết, dữ liệu bên phải quá hạn sẽ bị loại bỏ, do đó sự kiện click này sẽ không bao giờ tìm thấy bạn đồng hành nữa"*. Lúc này, Spark mới phát ra dòng `leftOuter` join với các cột bên phải mang giá trị `NULL`, đồng thời **xóa vĩnh viễn bản ghi click ra khỏi bộ nhớ trạng thái**.

---

## Phần 4: Các Mẫu Lưu trữ (Sink Patterns), Chế độ Xuất & Bảo đảm Exactly-Once

Sau khi dữ liệu đã được lọc, làm giàu và ghép nối, bước cuối cùng là ghi kết quả ra hệ thống lưu trữ bên ngoài.

---

### 1. Phân loại Chế độ Xuất (Output Modes)

Spark Structured Streaming cung cấp 3 chế độ xuất:

| Chế độ (Output Mode) | Cơ chế hoạt động | Phù hợp nhất cho | Điều kiện áp dụng |
|---|---|---|---|
| **`append`** | Chỉ xuất các dòng dữ liệu mới được chốt và đảm bảo sẽ không bao giờ bị sửa đổi trong tương lai. | Ghi log bất biến, ghi sự kiện đã hoàn tất làm giàu vào Data Lake. | Không thể dùng cho Aggregation nếu không có Watermark. |
| **`update`** | Chỉ xuất các dòng có sự thay đổi giá trị (thêm mới hoặc cập nhật giá trị tổng) trong micro-batch hiện tại. | Đồng bộ các chỉ số đếm/tổng hợp sang cơ sở dữ liệu Key-Value (Redis, Cassandra). | Cho phép xuất kết quả trung gian trước khi watermark đóng. |
| **`complete`** | Ghi đè toàn bộ bảng kết quả tính toán lại từ đầu ra sink ở mỗi micro-batch. | Bảng tổng hợp nhỏ (ví dụ top 10 sản phẩm bán chạy nhất toàn hệ thống). | **Cảnh báo**: Tuyệt đối không dùng cho tập dữ liệu lớn vì gây bùng nổ I/O ghi đĩa. |

---

### 2. Kỹ thuật "Cửa thoát hiểm" với `foreachBatch`

Mặc dù Spark cung cấp sẵn các connector tích hợp (`format("delta")`, `format("parquet")`, `format("kafka")`), nhưng trong môi trường sản xuất thực tế, hầu hết các đường ống chuyên nghiệp đều sử dụng **`foreachBatch`**.

```python
def upsertToLake(batch_df, batch_id):
    # batch_df tại đây là một BATCH DataFrame thông thường!
    # Mọi API của batch Spark (kể cả MERGE, repartition, JDBC) đều sử dụng được.
    (batch_df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save("/data/enriched_events"))

stream = enriched.writeStream \
    .foreachBatch(upsertToLake) \
    .option("checkpointLocation", "/mnt/ckpt/enrich") \
    .trigger(processingTime="30 seconds") \
    .start()
```

#### Tại sao `foreachBatch` lại là một vũ khí tối thượng?
1. **Biến Đổi Dòng thành Lô**: Bên trong hàm `upsertToLake`, đối số `batch_df` không còn là một Streaming DataFrame bị giới hạn bởi các ràng buộc khắt khe nữa, mà trở thành một **Standard Batch DataFrame**. Bạn có thể áp dụng toàn bộ sức mạnh của Spark Batch.
2. **Ghi đồng thời ra nhiều đích (Fan-out Writes)**: Bạn có thể vừa ghi `batch_df` vào Data Lake dạng Parquet, vừa lọc các dòng lỗi đẩy vào Kafka, vừa cập nhật chỉ số lên Elasticsearch trong cùng một lần duyệt dữ liệu.
3. **Hiện thực hóa Idempotent Upsert (Chống trùng lặp tuyệt đối)**: Đây là chìa khóa để đạt được **End-to-End Exactly-Once**.

---

### 3. Đạt được Exactly-Once nhờ Delta Lake `MERGE`

Khi một Spark Streaming job gặp sự cố mạng ở giây thứ 29 của một micro-batch và bị khởi động lại:
* Spark sẽ đọc lại toàn bộ dữ liệu của batch đó từ Kafka (theo nguyên lý At-Least-Once).
* Nếu sink của bạn là một lệnh `INSERT` thông thường vào PostgreSQL hoặc file Parquet, **dữ liệu của batch đó sẽ bị ghi đè thành 2 lần (Duplicate Records)**.

Để biến At-Least-Once của tầng vận chuyển thành **Exactly-Once** ở tầng lưu trữ, thao tác ghi bắt buộc phải có tính **Khả đẳng (Idempotent)** — nghĩa là ghi 1 lần hay ghi 10 lần một batch thì kết quả cuối cùng trong bảng đích vẫn hoàn toàn đồng nhất.

Mẫu thiết kế chuẩn mực trên kiến trúc Lakehouse (sử dụng Delta Lake):

```python
from delta.tables import DeltaTable

def merge_to_delta(micro_batch_df, batch_id):
    delta_table = DeltaTable.forPath(spark, "/data/lake/events_gold")
    
    # Khử trùng lặp nội bộ trong chính micro-batch trước khi merge
    deduped_batch = micro_batch_df.dropDuplicates(["event_id"])
    
    (delta_table.alias("target")
        .merge(
            deduped_batch.alias("source"),
            "target.event_id = source.event_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
```
Bằng cách sử dụng phép `MERGE INTO` dựa trên khóa định danh tự nhiên (`event_id`), nếu micro-batch bị tính toán lại do sự cố, các bản ghi đã tồn tại trong Delta Lake sẽ chỉ được ghi đè giá trị giống hệt, ngăn chặn 100% hiện tượng nhân bản dữ liệu rác.

---

## Phần 5: Tiến hóa Lược đồ (Schema Evolution), Bản ghi Hỏng & Dead-Letter Queues (DLQ)

Trong môi trường sản xuất thực tế, dữ liệu từ các nhà sản xuất (Kafka Producers) không bao giờ hoàn hảo. Một phiên bản ứng dụng di động mới cập nhật có thể vô tình gửi sai kiểu dữ liệu, hoặc gửi các chuỗi JSON bị cắt cụt do lỗi mạng.

Nếu đường ống streaming của bạn không được trang bị cơ chế tự vệ, chỉ cần **một bản ghi lỗi duy nhất** (Poison Pill) cũng có thể làm sập toàn bộ cụm Spark đang chạy 24/7.

```
                   Kafka Topic ('events')
                             │
                             ▼
              Spark Schema Validation & Parser
                             │
            ┌────────────────┴────────────────┐
            │                                 │
     Dữ liệu Hợp lệ                    Dữ liệu Hỏng / Lỗi
    (_corrupt_record IS NULL)         (_corrupt_record IS NOT NULL)
            │                                 │
            ▼                                 ▼
   Main Pipeline & Lake               Dead-Letter Queue (DLQ)
   (Delta Gold / Marts)             (Kafka Topic 'events-dlq' hoặc
                                     Quarantine Delta Table)
```

---

### 1. Bẫy ngầm của hàm `from_json` mặc định
Theo mặc định, nếu bạn định nghĩa trường `amount` là kiểu `DoubleType`, nhưng producer lại gửi lên chuỗi ký tự `"amount": "INVALID_NUMBER"`, hàm `from_json` của Spark sẽ:
* Không ném ra ngoại lệ (exception) để báo lỗi.
* Âm thầm gán toàn bộ struct `e` hoặc trường `amount` thành giá trị `NULL`.
* Hậu quả: Toàn bộ các bộ lọc `amount > 1000` phía sau sẽ âm thầm loại bỏ bản ghi này. Dữ liệu bị thất thoát trong im lặng mà đội ngũ vận hành không hề hay biết!

---

### 2. Thiết lập Cột Bắt Lỗi: `columnNameOfCorruptRecord`
Spark cung cấp một cơ chế bắt lỗi cực kỳ mạnh mẽ tích hợp sẵn trong bộ đọc JSON. Bạn có thể yêu cầu Spark: nếu một dòng JSON không thể parse được, hãy giữ nguyên toàn bộ chuỗi ký tự thô của dòng đó và đưa vào một cột riêng biệt mang tên `_corrupt_record`.

```python
# 1. Định nghĩa Schema bao gồm cột _corrupt_record
event_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("country", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("_corrupt_record", StringType(), True) # Cột hứng dữ liệu lỗi
])

# 2. Đọc và parse với cấu hình bắt lỗi
parsed_events = raw_kafka_stream.select(
    F.from_json(
        F.col("value").cast("string"),
        event_schema,
        {"columnNameOfCorruptRecord": "_corrupt_record"}
    ).alias("data"),
    F.col("timestamp").alias("kafka_ts")
).select("data.*", "kafka_ts")
```

---

### 3. Triển khai Kiến trúc Dead-Letter Queue (DLQ)
Khi đã phân tách được dữ liệu lành lặn và dữ liệu hỏng, trong hàm `foreachBatch`, chúng ta rẽ nhánh dòng chảy dữ liệu:

```python
def process_with_dlq(batch_df, batch_id):
    # Tách nhánh dữ liệu sạch và dữ liệu lỗi
    valid_records = batch_df.filter(F.col("_corrupt_record").isNull())
    corrupt_records = batch_df.filter(F.col("_corrupt_record").isNotNull())
    
    # 1. Xử lý dữ liệu sạch vào Main Data Lake
    if not valid_records.isEmpty():
        (valid_records.drop("_corrupt_record")
            .write.format("delta")
            .mode("append")
            .save("/data/lake/events_valid"))
            
    # 2. Định tuyến dữ liệu hỏng sang Dead-Letter Queue (DLQ)
    if not corrupt_records.isEmpty():
        (corrupt_records.select(
            F.col("_corrupt_record").alias("raw_payload"),
            F.col("kafka_ts"),
            F.lit(batch_id).alias("batch_id"),
            F.current_timestamp().alias("quarantine_ts")
        ).write.format("delta")
         .mode("append")
         .save("/data/lake/quarantine_dlq"))
```

#### Lợi ích của kiến trúc DLQ:
* **Tính liên tục của dịch vụ**: Streaming job không bao giờ bị dừng đột ngột bởi các tin nhắn rác.
* **Khả năng điều tra và khắc phục (Observability & Replay)**: Các kỹ sư có thể truy vấn bảng `quarantine_dlq` để xem chính xác các bản ghi JSON nào bị hỏng, thông báo cho đội ngũ Mobile/Backend sửa lỗi, và sau đó chạy một batch job để tái xử lý (replay) các bản ghi này khi cần thiết.

---

### 4. Quản trị Tiến hóa Lược đồ (Schema Evolution)
Khi hệ thống phát triển, các trường thông tin mới sẽ liên tục được thêm vào message Kafka (ví dụ thêm trường `device_type: "iOS"`).
Trên tầng lưu trữ Data Lakehouse, việc bổ sung thêm tùy chọn:
```python
.option("mergeSchema", "true")
```
giúp Delta Lake tự động bổ sung cột mới vào bảng đích mà không làm gián đoạn luồng dữ liệu và không yêu cầu phải xóa bảng để tạo lại từ đầu.

---

## Phần 6: Thực tế Vận hành, Các Trường hợp Biên & Chẩn đoán Sự cố

Duy trì một hệ thống Spark Streaming chạy ổn định 24/7/365 đòi hỏi sự am hiểu sâu sắc về tài nguyên và các hiện tượng thoái hóa hiệu năng theo thời gian.

---

### 1. Hiện tượng Tràn Bộ Nhớ State Store (Memory Blowups)

#### Triệu chứng:
* Job chạy rất mượt mà trong 2 ngày đầu tiên. Đến ngày thứ 3, thời gian thực thi của mỗi micro-batch tăng từ 2 giây lên 45 giây.
* Biểu đồ Spark UI ở tab **Structured Streaming** cho thấy chỉ số `memoryUsedBytes` và `numRowsTotal` ở mục State Store tăng trưởng theo một đường dốc thẳng đứng không hề có điểm dừng.
* Cuối cùng, các executor liên tục bị chết với lỗi `java.lang.OutOfMemoryError: Java heap space`.

```
Kích thước State
     ▲                                   / (Không có Watermark: OOM!)
     │                                  /
     │                                 /
     │                                /
     │            ═══════════════════/ (Có Watermark: Cân bằng trạng thái)
     │           /
     │          /
     └─────────/─────────────────────────────▶ Thời gian
```

#### Nguyên nhân & Cách khắc phục:
1. **Quên đặt Watermark**: Mọi phép toán `dropDuplicates`, Stream-Stream Join hoặc Aggregation đều tích lũy trạng thái vĩnh viễn nếu không có ranh giới Watermark.
2. **Khoảng thời gian Watermark quá lớn**: Đặt `withWatermark("ts", "7 days")` đồng nghĩa với việc Spark phải giữ toàn bộ dữ liệu của 7 ngày trong RAM. Cần đánh giá thực tế: liệu dữ liệu kinh doanh có thực sự đến trễ tới 7 ngày hay không? Nếu chỉ trễ tối đa 2 giờ, hãy giảm watermark xuống `2 hours`.
3. **Chuyển đổi Backend sang RocksDB**: Theo mặc định, Spark sử dụng `HDFSBackedStateStoreProvider`, lưu toàn bộ trạng thái trong heap memory của JVM. Khi trạng thái vượt quá vài Gigabytes, hiện tượng Garbage Collection sẽ làm treo executor. Giải pháp là chuyển sang sử dụng **RocksDB State Store** (lưu trạng thái trên đĩa SSD cục bộ của node và chỉ nạp các block nóng vào RAM):
   ```python
   spark.conf.set(
       "spark.sql.streaming.stateStore.providerClass",
       "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider"
   )
   ```

---

### 2. Nghẽn Cổ Chai Phân Vùng Shuffle (`spark.sql.shuffle.partitions`)

Trong Spark Batch, giá trị mặc định của `spark.sql.shuffle.partitions` là **200**. 
Nếu micro-batch của bạn chỉ kéo về 1.000 bản ghi mỗi 5 giây, nhưng Spark lại chia nhỏ 1.000 bản ghi này ra cho **200 partitions** để thực hiện phép Join hoặc GroupBy:
* Mỗi task xử lý vỏn vẹn... 5 dòng dữ liệu.
* Chi phí khởi tạo task, lập lịch CPU và trao đổi mạng (network overhead) lớn gấp hàng trăm lần thời gian tính toán thực tế.
* **Quy tắc điều chỉnh**: Với các streaming query có độ trễ thấp và lưu lượng vừa phải, hãy chủ động giảm số shuffle partitions xuống một con số phù hợp với số lượng CPU cores khả dụng (ví dụ: 8, 16 hoặc 32):
  ```python
  spark.conf.set("spark.sql.shuffle.partitions", "16")
  ```

---

### 3. Kiểm soát Áp lực Dội ngược (Backpressure)

Khi hệ thống gặp sự cố mạng hoặc lưu lượng người dùng tăng đột biến (flash sale), Kafka topic có thể tích tụ hàng chục triệu tin nhắn tồn đọng (lag).
Nếu Spark khởi động lại và cố gắng đọc toàn bộ số tin nhắn này trong **một micro-batch duy nhất**, cụm máy chủ sẽ ngay lập tức bị quá tải và sập nguồn.

Để kiểm soát điều này, bạn cần thiết lập giới hạn chặn trên cho lượng dữ liệu đọc vào:
```python
df = spark.readStream \
    .format("kafka") \
    .option("maxOffsetsPerTrigger", 50000) \  # Đọc tối đa 50,000 records mỗi batch
    .load()
```
Tùy chọn này đảm bảo kích thước của từng micro-batch luôn nằm trong giới hạn chịu đựng an toàn của bộ nhớ, cho phép hệ thống "bắt kịp" tiến độ (catch up) một cách êm ái mà không bị sập.

---

## Phần 7: Tổng kết, Ma trận Quyết định & Câu hỏi Thảo luận / Phỏng vấn Chuyên sâu

---

### 1. Ma trận Quyết định: Khi nào nên dùng Spark Structured Streaming?

| Yêu cầu Kỹ thuật | Spark Structured Streaming | Apache Flink | Kafka Streams |
|---|---|---|---|
| **Mô hình Xử lý** | Micro-batch (mặc định) / Continuous | Native Event-driven (Từng dòng) | Native Event-driven (Từng dòng) |
| **Độ trễ tối thiểu (Latency)** | 100ms – 1s | Siêu thấp (< 10ms) | Rất thấp (< 20ms) |
| **Tích hợp Data Lakehouse** | Tối ưu tuyệt đối (Native Delta/Iceberg) | Đòi hỏi connector cấu hình ngoài | Không phù hợp cho hồ dữ liệu lớn |
| **Vận hành hạ tầng** | Cụm máy tính phân tán (YARN/K8s/Databricks) | Cụm phân tán riêng (Flink Cluster) | Ứng dụng nhúng nhẹ (Java App/K8s pod) |
| **Khả năng làm giàu Stream-Static** | Cực mạnh với cơ chế Broadcast Join | Cần cấu hình Async I/O hoặc cache ngoài | Sử dụng KTable lưu trên đĩa bộ nhớ local |

> **Quy tắc ngón tay cái:**
> * Nếu bạn cần **độ trễ dưới 20ms** (giao dịch chứng khoán tần suất cao, điều khiển thiết bị y tế): Hãy chọn **Apache Flink**.
> * Nếu bạn đang xây dựng một **kiến trúc vi dịch vụ (Microservices)** thuần túy xoay quanh Kafka: Hãy chọn **Kafka Streams**.
> * Nếu bạn đang xây dựng một **Nền tảng Dữ liệu Hiện đại (Modern Data Lakehouse)**, nơi dữ liệu streaming cần được làm giàu, ghép nối và ghi trực tiếp vào các định dạng mở (Delta Lake, Iceberg) phục vụ cho Business Intelligence và AI/ML: **Spark Structured Streaming là lựa chọn tối ưu nhất về mặt kiến trúc và chi phí vận hành.**

---

### 2. Bảng Tóm tắt Ghi nhớ (Cheat-Sheet Summary)

```
Kafka Source  ──▶  Filter       (Stateless: Miễn phí tài nguyên, mở rộng vô hạn)
              ──▶  Enrich       (Stream-Static: Broadcast table nhỏ, chú ý bẫy Stale Cache)
              ──▶  Join         (Stream-Stream: Ràng buộc khoảng thời gian + BẮT BUỘC có Watermark)
              ──▶  Deduplicate  (Stateful: Đưa timestamp vào key + gắn kèm Watermark)
              ──▶  DLQ Routing  (Bắt dữ liệu hỏng bằng columnNameOfCorruptRecord)
              ──▶  Sink         (Idempotent MERGE trong foreachBatch để bảo đảm Exactly-Once)

4 Điều luật Bất di bất dịch trong Sản xuất:
1. Luôn khai báo checkpointLocation trên một storage bền vững (S3 / ADLS / HDFS).
2. Luôn định nghĩa Watermark cho mọi phép toán tích lũy trạng thái.
3. Luôn bảo đảm tính Khả đẳng (Idempotence) ở tầng ghi dữ liệu cuối cùng.
4. Luôn kiểm soát kích thước State Store và cấu hình maxOffsetsPerTrigger để chống nghẽn.
```

---

### 3. Câu hỏi Thảo luận & Phỏng vấn Chuyên sâu (Interview-Style Questions)

#### Câu hỏi 1: Sự cố "Bản ghi biến mất bí ẩn" trong Stream-Stream Join
* **Tình huống**: Bạn xây dựng một pipeline ghép nối giữa luồng Click (`left`) và luồng Mua hàng (`right`) sử dụng phép `leftOuter` join với watermark 1 giờ và interval join 15 phút. Khi kiểm tra kết quả ghi xuống Data Lake, bạn phát hiện có khoảng 30% số lượt click không hề xuất hiện trong bảng đích (ngay cả dưới dạng dòng có giá trị bên phải là `NULL`).
* **Câu hỏi**: Điều gì đã xảy ra với 30% bản ghi click này? Lỗi nằm ở cách cấu hình Watermark hay cách khai báo Output Mode?
* *Gợi ý trả lời*: Trong phép `leftOuter` join, một dòng bên trái chỉ được xuất ra khi nó tìm thấy dòng bên phải khớp, HOẶC khi Watermark của luồng bên phải đã vượt qua khoảng thời gian tối đa của interval (`left_ts + 15m`). Nếu bản ghi click đến quá trễ so với chính Watermark của luồng bên trái (trễ hơn 1 giờ), Spark sẽ lập tức **thả rơi (drop) bản ghi click đó ngay từ cửa vào** mà không đưa vào state store, khiến nó biến mất hoàn toàn khỏi kết quả cuối cùng.

#### Câu hỏi 2: Giải quyết bài toán Dimension Thay đổi Nhanh trong Stream-Static Join
* **Tình huống**: Hệ thống của bạn cần làm giàu luồng giao dịch ngân hàng với trạng thái tài khoản (`status: 'ACTIVE'`, `'LOCKED'`, `'SUSPENDED'`). Bảng trạng thái tài khoản thay đổi liên tục trong ngày và được đồng bộ về Data Lake dưới dạng một Delta table.
* **Câu hỏi**: Nếu áp dụng đoạn mã `spark.read.table("account_status")` như trong bản phác thảo, hệ thống sẽ gặp lỗi gì? Làm thế nào để thiết kế lại giải pháp để đảm bảo giao dịch phát sinh sau khi tài khoản bị khóa 1 giây phải được ghi nhận trạng thái `'LOCKED'` chính xác mà không cần khởi động lại streaming query?
* *Gợi ý trả lời*: Đọc tĩnh ngoài stream sẽ bị dính bẫy Stale Cache. Để giải quyết, có hai hướng:
  1. Nếu chấp nhận độ trễ vài chục giây: Đọc lại Delta table `account_status` bên trong hàm `foreachBatch(batch_df, batch_id)`. Do Delta Lake hỗ trợ ACID snapshot reads, mỗi micro-batch sẽ tự động nhìn thấy phiên bản commit mới nhất của bảng tài khoản.
  2. Nếu yêu cầu độ trễ mili-giây (thực sự real-time): Không lưu dimension này ở dạng file trên Data Lake nữa, mà lưu trên một In-memory Key-Value Store (như Redis). Trong `foreachBatch` hoặc qua hàm ánh xạ phân tán, thực hiện point-lookup trực tiếp vào Redis để lấy trạng thái mới nhất tại thời điểm xử lý.

#### Câu hỏi 3: Tại sao Plain JDBC Sink không thể đảm bảo Exactly-Once?
* **Tình huống**: Một kỹ sư đề xuất cấu hình ghi kết quả streaming trực tiếp vào cơ sở dữ liệu PostgreSQL thông qua connector chuẩn: `df.writeStream.format("jdbc").start()`.
* **Câu hỏi**: Tại sao kiến trúc này chỉ đạt được mức bảo đảm **At-Least-Once**, và điều gì sẽ xảy ra với dữ liệu trong PostgreSQL khi một executor bị mất kết nối mạng và Spark tự động retry lại micro-batch đó?
* *Gợi ý trả lời*: Connector JDBC chuẩn của Spark thực hiện các câu lệnh `INSERT INTO table VALUES (...)` phân tán trên từng executor. Khi một micro-batch bị lỗi ở giữa chừng, một số executor có thể đã thực hiện lệnh `INSERT` thành công vào PostgreSQL trước khi crash. Khi driver kích hoạt cơ chế retry, toàn bộ micro-batch đó sẽ được phân bổ lại và các lệnh `INSERT` sẽ được chạy thêm một lần nữa trên các bản ghi đó. Vì bảng PostgreSQL không có cơ chế tự động phân giải xung đột khóa chính ở tầng JDBC connector của Spark, cơ sở dữ liệu sẽ chứa đầy các dòng dữ liệu bị nhân bản (duplicate rows). Để khắc phục, bắt buộc phải dùng `foreachBatch` và tự viết câu lệnh `INSERT ... ON CONFLICT DO UPDATE` (Upsert).
