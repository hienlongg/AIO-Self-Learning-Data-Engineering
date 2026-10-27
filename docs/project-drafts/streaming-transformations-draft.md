# Streaming Transformations: A Practical Guide (Spark Structured Streaming + Kafka)

A high-level mental model first, then the four pillars — filtering, enrichment, streaming joins, and sink patterns.

---

## Pipeline Mental Model

```
┌───────┐   ┌────────────────────┐   ┌──────────┐
│ Kafka │──▶│ Spark Structured   │──▶│ Storage  │
│ topic │   │ Streaming engine   │   │ sink     │
└───────┘   └────────────────────┘   └──────────┘
                 transformations
```

Every transformation you write is **stateless or stateful** — that distinction drives almost all design decisions in streaming (checkpoints, state stores, watermarking).

```python
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker1:9092") \
    .option("subscribe", "events") \
    .load()

events = df.select(
    F.from_json(F.col("value").cast("string"), event_schema).alias("e"),
    F.col("timestamp")
).select("e.*", "timestamp")

# ... transformations ...

query = output.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/mnt/checkpoints/events") \
    .outputMode("append") \
    .start()
```

---

## 1. Filtering

Cheap, stateless, and infinitely re-scalable. You can chain filters freely — they don't grow state.

```python
# Keep only high-value transactions
stream = events.filter(
    (F.col("amount") > 1000) &
    (F.col("country").isin("US", "DE", "JP")) &
    F.col("event_type").isNotNull()
)
```

**Common patterns:**
- **Deduplication** (`dropDuplicates(["event_id"])`) — deceptively simple; it *is* stateful, with TTL controlled by your watermark.
- **Late-arrival handling** — combine `withWatermark("timestamp", "10 minutes")` with aggregations; drop events older than the watermark (they'd otherwise poison state or be silently included).

---

## 2. Enrichment

Joining a stream with static/reference data to add context. This is the everyday workhorse.

```python
# Load reference data as a batch DataFrame
customers = spark.read.parquet("/data/customers")  # broadcast if small

enriched = events.join(
    F.broadcast(customers),
    "customer_id",
    "left"
)

query = enriched.writeStream ... 
```

**Key rule:** the static side must be reloaded periodically, or you serve stale data forever. Patterns:

| Pattern | How | When to use |
|---|---|---|
| **Broadcast join** | `spark.read` of the reference table; restarted/re-fetched per micro-batch or trigger | Small dimension (< a few hundred MB) |
| **Scheduled refresh** | Read reference table *inside* `foreachBatch` so each trigger re-reads it | Medium tables, moderate staleness tolerance |
| **Stream-stream join** | Both sides are Kafka streams | True real-time both sides (see §3) |
| **Lookup via external store** | `foreachBatch` + point lookups in Redis/Cassandra/Postgres | Hot data, high QPS, need freshest data |

Gotchas in stream-static joins:
- Spark caches the broadcast side — if the dimension changes, you must restart the query (or use foreachBatch). Forgetting this is a classic bug: "why are new customers showing up as null?"
- If the reference DataFrame has duplicates on the join key, your stream explodes (one event → many rows).

---

## 3. Streaming Joins (stream ↔ stream)

This is where semantics get serious, because both sides advance in time.

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

**What you must understand:**

- **Watermark is mandatory** for any stream-stream outer join. Without it, state grows forever (Spark can't know when a side will never arrive).
- **Watermark limits join window.** Rows older than `max(watermarks) − allowed lateness` are never matched. Choose your watermark ≥ your maximum expected event lateness, otherwise data silently drops.
- **State TTL = watermark + window**, roughly. Tune state store size accordingly.
- **Interval joins** (time-bounded equality joins, as above) are supported; **arbitrary non-equi joins across streams are not** — Spark needs equality predicates to partition state.

**Sink-side caution for joins:** `append` output mode only works when the join is watermark-bounded on both sides; otherwise use `update`.

---

## 4. Sink Patterns

| Output mode | Semantics | Best for |
|---|---|---|
| `append` | Only new rows, never updated | Event logs, raw immutable streams |
| `update` | Updated rows emitted every trigger | Aggregations, sessionization |
| `complete` | Full result table every trigger | Small aggregate tables only (danger: full rewrite per batch) |

**Sink selection:**

- **Append-only data lake** (Delta/Iceberg/Hudi): `foreachBatch` + `merge` for upserts, or plain `writeStream.format("delta").mode("append")`. Delta gives exactly-once + ACID via `foreachBatch` idempotent merges keyed on `event_id`.
- **Idempotent upserts to RDBMS:** always use `foreachBatch` with a transaction or an upsert on a natural key. Raw `JDBC` sink is at-least-once.
- **Kafka → Kafka:** `writeStream.format("kafka")`, set `checkpointLocation` — restart-safe.
- **Elasticsearch/OpenSearch:** fine for lookups/serving, poor as a primary sink.
- **No-op sink:** useful for testing (`format("noop")`) — skips write entirely.

### foreachBatch as your escape hatch

Most production sinks are custom:

```python
def upsertToLake(batch_df, batch_id):
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

This gives you batch DataFrames, so anything batch can do (MERGE, arbitrary connectors, partition overrides) you can now do in streaming.

---

## Cheat-Sheet Summary

```
Kafka source ──▶ filter (stateless, free)
              ──▶ enrich (static join, watch staleness)
              ──▶ join (stream-stream, watermark mandatory)
              ──▶ aggregate (watermark controls state + late data)
              ──▶ sink (append / update / foreachBatch)

Non-negotiables:
  • checkpointLocation on every query
  • watermarks wherever state accumulates
  • idempotency on the sink (natural key upsert or MERGE)
  • state store monitoring (Spark UI → Streaming tab → state size)
```

Want me to go deeper on any piece — e.g., a full stream-stream join with sessionization, Delta MERGE upserts, or troubleshooting state-store blowups?