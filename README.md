# Kafka Ad Stream Consumer - Complete Setup Guide

## Quick Start

```bash
cd C:\Users\DELL 7380\Desktop\kafka-develop
python -m consumer.simple_consumer
```

---

## Project Overview

**What it does:**
- Reads ad events from Kafka
- Parses Avro messages
- Checks event lateness (2-hour threshold)
- Writes on-time events → PostgreSQL
- Writes late events → Parquet files
- Computes 5-minute event aggregations (event_type + device counts)
- Writes aggregations → PostgreSQL

**Technology Stack:**
- PySpark 3.5.0 (Structured Streaming)
- Kafka (message broker)
- PostgreSQL (data storage)
- Python 3.11.9

---

## Architecture

### Dual Stream Processing

```
┌─────────────────┐
│  Kafka Topic    │
│  (ads_events)   │
└────────┬────────┘
         │
         ├─────────────────────────┬──────────────────────────┐
         │                         │                          │
         ▼                         ▼                          ▼
    ┌─────────────┐          ┌──────────────┐
    │   STREAM 1  │          │  STREAM 2    │
    │   EVENTS    │          │ AGGREGATION  │
    └─────────────┘          └──────────────┘
         │                         │
    Deserialize              Parse → Watermark
    Parse JSON               5-min Windows
    Check Lateness           Group by event_type,device
    Separate On-time/Late    Count events
         │                         │
         ├─ On-time (≤2h)         │
         │  → PostgreSQL           │
         │  (ad_events table)      │
         │                         │
         ├─ Late (>2h)            │
         │  → Parquet             │
         │                         │
         ├─ Bad records           │
         │  → JSON                │
         │                         │
         └─────────────────────────┴──────────────────────────┘
                                  │
                                  ▼
                        ┌──────────────────┐
                        │  PostgreSQL      │
                        │  event_type_     │
                        │  device_stats    │
                        └──────────────────┘
```

---

## PostgreSQL Tables

### 1. event_schema.ad_events
Stores all on-time events (≤ 2 hours lateness)

```sql
event_id (VARCHAR, PK)
event_time (TIMESTAMP)
user_id (VARCHAR)
campaign_id (INT)
ad_id (INT)
device (VARCHAR)
country (VARCHAR)
event_type (VARCHAR)
ad_creative_id (VARCHAR)
```

### 2. event_schema.event_type_device_stats
Pre-computed 5-minute aggregations

```sql
event_type (VARCHAR)
device (VARCHAR)
event_count (INT)
window_start (TIMESTAMP)
window_end (TIMESTAMP)
aggregation_time (TIMESTAMP)
```

---

## File Structure

```
kafka-develop/
├── README.md                       ← Complete documentation
├── docker-compose.yaml             ← Docker setup (Kafka, PostgreSQL)
├── init_db.sql                     ← Database initialization
├── log4j.properties                ← Spark logging config
├── pyproject.toml                  ← Python dependencies
├── .env.example                    ← Environment variables template
│
├── consumer/                       ← CONSUMER MODULE
│   ├── simple_consumer.py          ← MAIN FILE (run this!)
│   ├── avro_deserializer.py        ← Avro parsing utility
│   └── __init__.py
│
├── ad_stream_producer/             ← PRODUCER MODULE
│   ├── run_producer.py
│   ├── config.py
│   ├── kafka_producer.py
│   ├── producer_service.py
│   ├── logger.py
│   ├── schema.py
│   └── __init__.py
│
├── data_generator/                 ← EVENT GENERATOR
│   ├── event_generator.py
│   └── __init__.py
│
├── schema/                         ← AVRO SCHEMAS
│   ├── ad_event_update.avsc
│   └── __init__.py
│
├── tests/                          ← TEST SUITE
│   ├── test_avro_serde.py
│   ├── test_event_generator.py
│   ├── test_producer_service.py
│   └── __init__.py
│
├── jars/                           ← DOWNLOADED JARS
│   ├── kafka-clients-3.4.1.jar
│   └── postgresql-42.6.0.jar
│
└── output/                         ← RUNTIME OUTPUT
    ├── checkpoints_final_robust/   (events stream checkpoint)
    ├── checkpoints_aggregation/    (aggregation stream checkpoint)
    ├── late_events_parquet/        (late events data)
    ├── corrupted_records/          (bad records)
    └── consumer.log                (log file)
```

---

## Configuration

All settings in `ad_stream_producer/config.py`:

```python
# Kafka
KAFKA_BOOTSTRAP_SERVERS = ["localhost:9095", "localhost:9093", "localhost:9094"]
TOPIC = "ads_events"

# Schema Registry
SCHEMA_REGISTRY_URL = "http://localhost:8081"
SCHEMA_PATH = "ad_event_update.avsc"

# PostgreSQL
POSTGRES_URL = "jdbc:postgresql://localhost:5432/postgres"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "postgres"
POSTGRES_TABLE = "event_schema.ad_events"
```

---

## Stream 1: Events Processing

**Function:** `process_events_batch(batch_df, epoch_id)`

**Steps:**
1. Deserialize Avro → JSON string
2. Parse JSON using OUTPUT_SCHEMA
3. Separate good/bad records
4. Calculate lateness in seconds:
   ```
   lateness = current_time - event_time
   ```
5. Classify:
   - On-time: lateness ≤ 7200 seconds (2 hours)
   - Late: lateness > 7200 seconds
6. Write to PostgreSQL (on-time):
   - Create temp table via JDBC
   - UPSERT into main table (ON CONFLICT)
   - Drop temp table
7. Write late events to Parquet
8. Write corrupted records to JSON

**Example Log:**
```
[BATCH 123] Processing 450 records
[BATCH 123] On-time: 440, Late: 10, Bad: 0
[BATCH 123] ✓ Wrote 440 on-time records to PostgreSQL
[BATCH 123] ✓ Wrote 10 late records to Parquet
[BATCH 123] ✓ Done in 2.34s
```

---

## Stream 2: Windowed Aggregation

**Function:** `write_aggregation_batch(batch_df, batch_id)`

**Processing:**
1. Parse events (same as Stream 1)
2. Convert `event_time` → timestamp
3. Apply watermark: 2-minute grace period
   - Allows late data to update existing windows
4. Create 5-minute tumbling windows
5. Group by: event_type + device
6. Aggregate: count(events)
7. Write to PostgreSQL:
   - UPSERT based on (event_type, device, window_start, window_end)

**Example Output:**
```
event_type | device  | event_count | window_start        | window_end
-----------+---------+-------------+---------------------+--------------------
view       | mobile  | 145         | 2026-04-18 14:00:00 | 2026-04-18 14:05:00
view       | desktop | 89          | 2026-04-18 14:00:00 | 2026-04-18 14:05:00
click      | mobile  | 23          | 2026-04-18 14:00:00 | 2026-04-18 14:05:00
purchase   | tablet  | 5           | 2026-04-18 14:00:00 | 2026-04-18 14:05:00
```

---

## SQL Queries

### Get Latest Aggregations (Last 5 Minutes)
```sql
SELECT event_type, device, SUM(event_count) as total
FROM event_schema.event_type_device_stats
WHERE window_end >= NOW() - INTERVAL '5 minutes'
GROUP BY event_type, device
ORDER BY total DESC;
```

### Get Event Trend Over Time
```sql
SELECT 
    window_end,
    event_type,
    device,
    event_count
FROM event_schema.event_type_device_stats
WHERE window_end >= NOW() - INTERVAL '1 hour'
ORDER BY window_end DESC, event_type, device;
```

### Get Device Distribution for 'view' Events
```sql
SELECT 
    device,
    SUM(event_count) as total,
    ROUND(100.0 * SUM(event_count) / SUM(SUM(event_count)) OVER (), 2) as pct
FROM event_schema.event_type_device_stats
WHERE event_type = 'view' AND window_end >= NOW() - INTERVAL '10 minutes'
GROUP BY device
ORDER BY total DESC;
```

### Get Recent Events from PostgreSQL
```sql
SELECT event_id, event_type, device, event_time, campaign_id
FROM event_schema.ad_events
WHERE event_time >= NOW() - INTERVAL '1 hour'
LIMIT 100;
```

---

## What simple_consumer.py Contains

### 1. Constants
- Output paths (checkpoints, parquet dirs)
- Event schema definition
- Configuration paths

### 2. create_spark_session()
- Sets up Spark with Kafka + PostgreSQL support
- Downloads JARs if needed
- Configures logging
- Returns SparkSession

### 3. process_events_batch(batch_df, epoch_id)
- Main event processing logic
- ~150 lines
- Handles all error cases

### 4. write_aggregation_batch(batch_df, batch_id)
- Writes aggregated stats to PostgreSQL
- Creates schema/table if needed
- Creates indexes
- Performs UPSERT

### 5. main()
- Initializes Spark session
- Loads Avro schema
- Connects to Kafka
- Starts Stream 1 (Events)
- Starts Stream 2 (Aggregation)
- Awaits termination

---

## Error Handling

### PostgreSQL Connection Failures
- Logs error with full traceback
- Continues processing
- Doesn't crash the entire consumer

### Bad Records
- Captured automatically
- Saved to JSON files in corrupted_records/
- Logged with count

### Empty Batches
- Skipped with warning log
- No processing overhead

### Aggregation Stream Failures
- Caught and logged
- Consumer continues with events-only stream
- No data loss

---

## Monitoring

### Expected Logs on Startup
```
================================================================================
SIMPLIFIED Kafka Consumer Starting
================================================================================
✓ Spark 3.5.0 session created
✓ Loaded Avro schema from http://localhost:8081
✓ Kafka stream connected
Starting Stream 1: Events Processing
✓ Events stream started (query_id=...)
Starting Stream 2: Windowed Aggregation
✓ Aggregation stream started (query_id=...)
================================================================================
✓ DUAL STREAMS ACTIVE:
  1. Events → PostgreSQL
  2. 5-min Aggregations → PostgreSQL
================================================================================
```

### During Execution
```
[BATCH 1] Processing 450 records
[BATCH 1] On-time: 440, Late: 10, Bad: 0
[BATCH 1] ✓ Wrote 440 on-time records to PostgreSQL
[BATCH 1] ✓ Wrote 10 late records to Parquet
[BATCH 1] ✓ Done in 2.34s
[AGG BATCH 0] Processing 24 aggregation records
[AGG BATCH 0] ✓ Wrote 24 records in 0.45s
```

---

## Event Types & Devices

From `data_generator/event_generator.py`:

**Event Types (weighted):**
- view (50% - most common)
- click (30%)
- purchase (20% - rarest)

**Devices:**
- mobile
- desktop
- tablet

---

## Lateness Logic

```
lateness_seconds = UNIX_TIMESTAMP(current_time) - UNIX_TIMESTAMP(event_time)

if lateness_seconds <= 7200:  # 2 hours
    → Write to PostgreSQL
else:
    → Write to Parquet (late events)
```

**Why 2 hours?**
- Grace period for late-arriving events
- Allows correction window
- Beyond that, treated as historical data

---

## Watermark Explanation

**Watermark: 2 minutes**
- Events can arrive up to 2 minutes late
- Spark will hold state for 2 minutes past window end
- After 2 minutes, window is finalized
- Late data arriving after watermark:
  - Within 2 min grace → Updates existing window
  - After 2 min grace → Dropped (state cleaned up)

**Why 2 minutes?**
- Network latency buffer
- Allows corrections
- Balances freshness vs accuracy

---

## Window Configuration

**5-minute tumbling windows:**
```
14:00:00 - 14:05:00
14:05:00 - 14:10:00
14:10:00 - 14:15:00
...
```

No overlap, back-to-back windows.

---

## Dependencies

**Required packages:**
```
pyspark==3.5.0
confluent-kafka[avro]
fastavro
psycopg2-binary
python-dotenv
```

**Java:**
- Apache Spark requires Java 8+
- Kafka clients JAR downloaded automatically
- PostgreSQL JDBC driver downloaded automatically

---

## Troubleshooting

### "Port already in use" Error
- Another Spark/Kafka process running
- Kill it: `netstat -ano | findstr :9095`

### "Connection refused to PostgreSQL"
- PostgreSQL not running
- Check: `psql -U postgres -h localhost`
- Start: `pg_ctl -D /path/to/data start`

### "No records being written"
- Check Kafka has events
- Verify schema matches
- Check PostgreSQL is accessible
- Review logs for errors

### "Aggregation stream not starting"
- Check PostgreSQL connection
- Verify schema parsing works
- Ensure event_time field exists

---

## Performance Notes

- **Batch processing:** Micro-batches every 500ms (configurable)
- **Latency:** Events to PostgreSQL within 1-2 seconds
- **Throughput:** Depends on Kafka + PostgreSQL
- **Memory:** Typical: 4GB heap (-Xmx4g)
- **Checkpoint recovery:** ~30-60 seconds on restart

---

## Stopping the Consumer

```bash
Ctrl+C
```

Both streams will gracefully stop.
Checkpoints saved for recovery.

---

## Restarting from Last Position

Run the same command:
```bash
python simple_consumer.py
```

Spark will resume from last checkpoint.
No data loss between restarts.

---

## That's It!

One file. Two streams. Real-time processing.

Questions? Check the logs - they're detailed and descriptive.

