# Reading Outline: Kafka Delivery Semantics, Replay, Duplicate Reproduction & Deduplication Strategies

---

## Part 1: Foundations — Message Delivery Semantics in Distributed Systems

### 1. Realities of Distributed Networks
* **The Physics of Distributed Systems**:
  * Network unpredictability: packet loss, latency spikes, intermittent disconnects.
  * The Two Generals' Problem and the FLP Impossibility Result applied to distributed messaging.
* **The Three Critical Failure Boundaries**:
  * **Producer $\rightarrow$ Broker**: Network timeout while sending, or broker crashing before/after persisting.
  * **Broker Persistence**: Disk fsync guarantees, replica replication lags, leader election failover.
  * **Broker $\rightarrow$ Consumer**: Message processed but consumer crashes before offset commit; consumer heartbeat timeout causing rebalance.

### 2. Core Delivery Semantics Compared
* **At-most-once Delivery**:
  * **Mechanism**: Offsets committed *before* processing message payload.
  * **Failure Scenario**: Consumer pulls record $\rightarrow$ commits offset $\rightarrow$ encounters unhandled exception/crash. On reboot, record is skipped forever (Data Loss).
  * **Viable Use Cases**: Ephemeral telemetry, loss-tolerant metrics, IoT sensor high-frequency pings.
* **At-least-once Delivery (The Industry Standard)**:
  * **Mechanism**: Message processed *first*, offset committed *afterwards*. Producers retry on dropped/missing ACKs.
  * **Failure Scenario**: Record processed successfully $\rightarrow$ crash or rebalance before commit $\rightarrow$ new consumer picks up same uncommitted offset (Data Duplication).
  * **Viable Use Cases**: Financial transactions, order processing, auditing pipelines (where losing data is unacceptable).
* **Exactly-once Processing (EOS)**:
  * **Debunking the Myth**: Physical network packets cannot guarantee exactly-once delivery; exactly-once is an *effective system state guarantee*.
  * **The End-to-End Contract**: Requires coordinated transactionality across Source $\rightarrow$ Streaming Engine $\rightarrow$ Sink Storage.

---

## Part 2: Log-Centric Architecture & Replayability in Kafka

### 1. Traditional Message Queues vs. Distributed Commit Log
* **Destructive Reads (JMS / RabbitMQ)**:
  * Messages are deleted or hidden from queues once acknowledged.
  * Cannot rewind history; reprocessing requires dedicated dead-letter queues (DLQ) or manual repush.
* **Immutable Append-Only Log (Apache Kafka)**:
  * Partitions are ordered, append-only logs persisted to disk.
  * Reads are non-destructive pointer movements (`consumer offsets`).
  * Retention policies (`log.retention.hours`, `log.retention.bytes`, Log Compaction).

### 2. Mechanics of Event Replay
* **Offset Manipulation**:
  * Rewinding consumer offsets to specific points: beginning (`earliest`), timestamp-based (`offsetsForTimes`), or absolute offset (`seek`).
* **Operational Replay Scenarios**:
  * **Bug Recovery & Code Hotfixes**: Fixing downstream business logic bugs and recalculating past analytical aggregates.
  * **Data Backfill & Schema Evolution**: Spinning up a brand-new consumer group to ingest historical data into a newly provisioned data lake/warehouse.
  * **Disaster Recovery**: Rebuilding downstream state or database read-models after corruption or database loss.

---

## Part 3: Duplicate Failure Points & Hands-on Simulation (Reproduction)

### 1. Anatomy of Duplicate Generation
* **Producer-Side Duplication**:
  * Broker successfully appends message to commit log $\rightarrow$ Broker sends ACK $\rightarrow$ Network partition drops ACK $\rightarrow$ Producer timeout triggers retry $\rightarrow$ Second identical record appended.
* **Consumer-Side Duplication (At-least-once Reprocessing)**:
  * Consumer polls batch $\rightarrow$ writes records to database $\rightarrow$ process killed (`SIGKILL`/OOM) before `commitSync()` runs $\rightarrow$ consumer restarts and re-processes uncommitted records.
* **Rebalance-Induced Duplication**:
  * Consumer encounters heavy computation / blocking I/O $\rightarrow$ processing exceeds `max.poll.interval.ms` $\rightarrow$ Group Coordinator evicts consumer $\rightarrow$ partition reassigned $\rightarrow$ secondary consumer re-reads uncommitted offsets.

### 2. Reproduction Lab Scenarios
* **Lab Scenario A: Producer Network Retry Simulation**:
  * Configuration: `enable.idempotence=false`, `acks=all`, `retries=3`, `request.timeout.ms=1000`.
  * Fault Injection: Simulating network drops/delays on ACK responses via Toxiproxy or mock transport layer.
* **Lab Scenario B: Consumer Hard-Crash Simulation**:
  * Pattern: Consumer loop writing to relational table without auto-commit (`enable.auto.commit=false`).
  * Fault Injection: Forcing sudden process termination (`os._exit(1)`) after writing record 5 of a 10-message batch.
* **Lab Scenario C: Slow Consumer & Session Timeout Simulation**:
  * Configuration: Low `max.poll.interval.ms` (e.g., 10 seconds).
  * Fault Injection: Injecting arbitrary sleep delay (e.g., 20 seconds) in message processing loop to trigger rebalance.
* **Lab Scenario D: Intentional Replay Simulation**:
  * Executing offset rewinds using Kafka CLI tools (`kafka-consumer-groups --reset-offsets --to-offset 0 --execute`).

---

## Part 4: Designing End-to-End Deduplication Strategies

### 1. Kafka-Native Deduplication Mechanisms
* **Kafka Idempotent Producer**:
  * Internals: Producer ID (`PID`), monotonic Sequence Numbers per partition, Producer Epoch.
  * Duplicate suppression at broker memory/log level.
  * Scope & Boundaries: Scoped only to a single producer session and single partition; cannot protect against consumer replay or application crashes.
* **Kafka Transactional API (Read-Process-Write)**:
  * Two-Phase Commit (`2PC`) managed by the Transaction Coordinator and `__transaction_state` topic.
  * Synchronized atomic commit of transformed messages and source consumer offsets.
  * Consumer configuration: `isolation.level=read_committed`.
  * Scope: Confined strictly within Kafka-to-Kafka streaming topologies.

### 2. Application & Sink-Level Deduplication (Consumer Idempotency)
* **Strategy 1: Natural / Mathematical Idempotency**:
  * Designing operations where $f(f(x)) = f(x)$.
  * Replacing relative increments (`UPDATE balance = balance + 10`) with absolute state assignments (`SET balance = 150`).
  * Utilizing native database Upserts (`INSERT ... ON CONFLICT DO UPDATE`).
* **Strategy 2: Transactional Deduplication Table (Relational Sink)**:
  * Dedicated metadata table: `processed_messages(message_id PRIMARY KEY, processed_at TIMESTAMP)`.
  * Atomic execution: Ghi bản ghi nghiệp vụ và chèn `message_id` trong cùng một single RDBMS transaction.
  * Collision handling: Catching primary key violations (`UniqueViolationException`) and rolling back/skipping gracefully.
* **Strategy 3: Idempotent State Machine (Conditional Updates)**:
  * Guarding against out-of-order execution or duplicate state transitions.
  * SQL conditional checks: `UPDATE orders SET status = 'SHIPPED' WHERE id = :id AND status = 'PROCESSING'`.
  * Versioning / Optimistic Locking: Checking monotonic version columns (`WHERE version = :expected_version`).
* **Strategy 4: External Distributed Dedup Store (Redis / Key-Value)**:
  * Architecture for non-transactional or high-throughput sinks (Elasticsearch, S3, Third-party Webhook APIs).
  * Atomic reservation pattern: `SET dedup:<event_id> "IN_FLIGHT" NX EX <ttl>`.
  * Transitioning state to `COMPLETED` upon task finalization; handling failure states and TTL expiration edge cases.

---

## Part 5: Production Architecture, Trade-Offs & Pitfalls

### 1. Architectural Trade-off Matrix
* Comparing Deduplication Patterns across:
  * **Latency**: In-line check vs. single transactional commit vs. distributed lock roundtrip.
  * **Throughput**: Impact of database locks, Redis network hops, transaction overhead.
  * **Complexity**: Operational maintenance of external state stores vs. schema design.
  * **Storage Overhead**: Storage footprint of deduplication history and retention cleanup.

### 2. Production Pitfalls & Edge Cases
* **Non-Transactional Side Effects**:
  * External REST API calls, email dispatches, SMS notifications triggered mid-pipeline cannot be rolled back by database transactions.
  * Pattern: Transactional Outbox Pattern combined with downstream idempotent consumers.
* **Deduplication State Explosion & Expiration (TTL Traps)**:
  * Replaying a 30-day-old event when Redis deduplication key TTL is 24 hours.
  * Managing state size in dedup tables: partitioning by date, rolling archival, log compaction.
* **Out-of-Order Retries**:
  * Duplicate messages arriving after newer state updates have already been committed.
  * Enforcing monotonically increasing sequence timestamps.

### 3. Production Readiness Checklist
* Producer configuration audit (`enable.idempotence=true`, `acks=all`, `max.in.flight.requests.per.connection <= 5`).
* Consumer loop guidelines (`enable.auto.commit=false`, deterministic error handling, manual commit boundaries).
* Idempotency key generation standards: Synthetic business keys vs. hash of immutable event payload.
