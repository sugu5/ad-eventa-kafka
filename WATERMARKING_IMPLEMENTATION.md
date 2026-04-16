# Late Data Handling with Watermarking

## Overview

This implementation adds watermarking and late data handling to your Kafka consumer with Spark Structured Streaming. Events arriving more than **2 hours** after their event timestamp are automatically separated for batch correction processing.

## Architecture

```
Kafka Stream
    ↓
Deserialize (Avro → JSON)
    ↓
Parse & Type-Cast
    ↓
Watermark Check (event_time vs current_time)
    ├─ On-Time Events (≤ 2 hours late)
    │   ├─ PostgreSQL (Primary)
    │   └─ Parquet (Fallback)
    │
    └─ Late Events (> 2 hours late)
        └─ Separate Parquet Storage for Batch Correction
```

## Key Components

### 1. **sink.py** (Modified)
Main batch processing logic with watermarking:
- Deserializes and parses incoming events
- Calculates lateness for each event: `lateness_seconds = current_time - event_timestamp`
- Separates records into **on-time** (≤ 2 hours) and **late** (> 2 hours)
- On-time events → PostgreSQL with UPSERT logic
- Late events → Parquet storage for batch correction

**Key Parameters:**
```python
watermark_delay_hours: int = 2  # Threshold for late events
```

### 2. **late_events_handler.py** (New)
Handles storage and retrieval of late-arriving events:
- Stores late events to separate Parquet location
- Adds metadata: `processed_at`, `batch_id`, `lateness_hours`
- Provides statistics API for monitoring
- Structured for easy batch processing

**Key Methods:**
```python
process_late_events(late_df, epoch_id)     # Store late events
get_late_events_stats()                     # Get statistics
```

### 3. **late_batch_corrector.py** (New)
Batch correction processor for manual or scheduled processing:
- Loads late events from Parquet storage
- Performs UPSERT into PostgreSQL to correct records
- Generates correction reports
- Can be run manually or scheduled (cron/Airflow)

**Usage:**
```bash
python consumer/late_batch_corrector.py
```

### 4. **late_events_monitor.py** (New)
Monitoring and metrics collection:
- Collects late events statistics
- Exports metrics in Prometheus format
- Provides dashboard-friendly metrics
- Calculates lateness distribution buckets

**Metrics:**
- `late_events_total` - Total late events
- `late_events_avg_lateness_hours` - Average lateness
- `late_events_max_lateness_hours` - Maximum lateness
- `events_by_lateness_bucket` - Distribution (2-4h, 4-12h, 12-24h, >24h)

## Storage Locations

All outputs are stored in `output/` directory:

```
output/
├── checkpoints_final_robust/          # Spark checkpoint for recovery
├── ads_events_parquet/                # On-time events (Parquet fallback)
├── corrupted_records/                 # Malformed/unparseable records
├── late_events_parquet/               # Late-arriving events (for batch correction)
└── spark-warehouse/                   # Spark metadata
```

## File Structure

```
late_events_parquet/
├── _SUCCESS                           # Spark success marker
└── part-00000-*.parquet              # Parquet files with late events
```

**Late Event Schema:**
```
event_id: string
event_time: timestamp
user_id: string
campaign_id: int
ad_id: int
device: string
country: string
event_type: string
ad_creative_id: string
lateness_seconds: long
lateness_hours: double
processed_at: timestamp
batch_id: string
```

## Workflow

### Step 1: Stream Processing (Continuous)
```python
# In run_consumer.py - automatically handles streaming
spark_df = read_from_kafka()
            .readStream
            .format("kafka")
            .option("subscribe", topic)
            .load()

# Batch processing callback
@foreachBatch
def process_batch(batch_df, epoch_id):
    # Watermark check happens here
    late_events_handler.process_late_events(late_df, epoch_id)
```

### Step 2: Late Data Detection
```python
# In sink.py process_batch()
on_time_records = good_records_with_time.where(
    col("lateness_seconds") <= watermark_seconds  # 2 hours * 3600
)

late_records = good_records_with_time.where(
    col("lateness_seconds") > watermark_seconds
)
```

### Step 3: Storage
- **On-Time:** → PostgreSQL (with fallback to Parquet)
- **Late:** → `output/late_events_parquet/`

### Step 4: Batch Correction (Manual/Scheduled)
```bash
# Run correction to update main database
python consumer/late_batch_corrector.py
```

Output:
```
===============================================================================
Correction Report
===============================================================================
timestamp: 2026-03-29T17:30:00
total_late_events: 45
correction_success: True
records_corrected: 45
avg_lateness_hours: 5.3
max_lateness_hours: 18.5
===============================================================================
```

## Example Logs

```
[BATCH 28] Starting — 2026-03-29 17:09:46
[BATCH 28] Records in batch: 197
[BATCH 28] Parsed: 195 good, 2 bad
[BATCH 28] Time-based filtering: 188 on-time, 7 late (threshold: 2 hours)
[BATCH 28] Creating temp table: temp_ad_events_28_1234567890
[BATCH 28] ✓ UPSERT completed: 188 rows affected in 0.42s
[LATE EVENTS BATCH 28] Processing 7 late events
[LATE EVENTS BATCH 28] ✓ Stored 7 late events in 0.15s at output/late_events_parquet
[BATCH 28] Done in 0.65s (on-time: 188, late: 7, bad: 2)
```

## Monitoring & Metrics

### Prometheus Metrics
```
# Late events tracking
late_events_total 245
late_events_avg_lateness_hours 5.2
late_events_max_lateness_hours 22.1
late_events_bucket{bucket="2-4h"} 120
late_events_bucket{bucket="4-12h"} 100
late_events_bucket{bucket="12-24h"} 20
late_events_bucket{bucket=">24h"} 5
```

### Collect Metrics
```python
from consumer.late_events_monitor import export_late_events_metrics

metrics = export_late_events_metrics(spark, "output/late_events_parquet")
print(metrics["dashboard_metrics"])
# Output:
# {
#     'timestamp': '2026-03-29T17:30:00',
#     'summary': {
#         'total_late_events': 245,
#         'avg_lateness_hours': 5.2,
#         'max_lateness_hours': 22.1,
#         'min_lateness_hours': 2.1
#     },
#     'distribution': {
#         '2-4h': 120,
#         '4-12h': 100,
#         '12-24h': 20,
#         '>24h': 5
#     }
# }
```

## Configuration

### Adjusting Watermark Threshold

**In `run_consumer.py`:**
```python
# Change the watermark threshold (default: 2 hours)
WATERMARK_DELAY_HOURS = 2  # Modify this value

# Pass to sink
process_batch = make_process_batch(
    ...
    watermark_delay_hours=WATERMARK_DELAY_HOURS,
)
```

**Examples:**
- `1` - Very strict, almost all slightly late events are flagged
- `2` - Default (current)
- `4` - More lenient, allows up to 4 hours lateness
- `24` - Very lenient, allows up to 1 day lateness

## Integration with Grafana Dashboard

### Add Data Source
1. Prometheus → `http://localhost:9090`
2. Create panel with query:
   ```
   late_events_total
   late_events_avg_lateness_hours
   ```

### Dashboard Queries
```
# Late Events Count
SELECT late_events_total

# Average Lateness Trend
SELECT rate(late_events_total[5m])

# Lateness Distribution
SELECT late_events_bucket
GROUP BY bucket
```

## Scheduled Batch Correction with Airflow

```python
# Example DAG snippet
from airflow import DAG
from airflow.operators.python import PythonOperator
from consumer.late_batch_corrector import run_batch_correction

def correct_late_events():
    success, report = run_batch_correction(
        late_events_path="output/late_events_parquet",
        target_table="ad_events"
    )
    return report

dag = DAG('late_events_correction', schedule_interval='0 2 * * *')  # 2 AM daily

correct_task = PythonOperator(
    task_id='correct_late_events',
    python_callable=correct_late_events,
    dag=dag
)
```

## Troubleshooting

### No Late Events Being Stored
**Check:** Event time format in your data
```python
# Event time should be parseable by Spark
from pyspark.sql.functions import to_timestamp

# Debug: print event times
df.select(col("event_time")).show()
```

### Correction Fails
**Check:** PostgreSQL connectivity
```python
import psycopg2
conn = psycopg2.connect(
    host="localhost",
    port="5432",
    database="postgres",
    user="postgres",
    password="your_password"
)
conn.close()  # If fails, check PostgreSQL service
```

### High Number of Late Events
**Solution 1:** Increase watermark
```python
WATERMARK_DELAY_HOURS = 4  # or higher
```

**Solution 2:** Investigate producer/network delays
```bash
# Check Kafka offset lag
./kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group your_group --describe
```

## Performance Considerations

1. **Storage:** Late events are in Parquet format (compressed)
   - ~100k events ≈ 50-100 MB

2. **Correction Process:** UPSERT to PostgreSQL
   - ~10k events ≈ 5-10 seconds
   - Can be parallelized with multiple micro-batches

3. **Memory:** LateEventsHandler maintains metadata in memory
   - Minimal overhead (<10 MB)

## Next Steps

1. **Run the consumer** with watermarking enabled:
   ```bash
   python consumer/run_consumer.py
   ```

2. **Monitor** late events:
   ```bash
   python consumer/late_events_monitor.py
   ```

3. **Schedule** batch corrections:
   ```bash
   python consumer/late_batch_corrector.py
   ```

4. **Integrate** with Grafana for visualization

## Summary

- ✅ Events are separated based on 2-hour threshold
- ✅ Late events stored for batch correction
- ✅ Automatic UPSERT maintains data consistency
- ✅ Metrics available for monitoring
- ✅ Manual or automated correction workflow
- ✅ Full audit trail of late events

