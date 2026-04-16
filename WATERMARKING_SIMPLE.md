# Watermarking Implementation - Simplified

## Overview

Spark Structured Streaming with **2-hour watermark** to handle late-arriving events. All records (on-time and late) are written to PostgreSQL.

## How It Works

```
Kafka Stream
    ↓
.withWatermark("event_time", "2 hours")  ← Handles late data up to 2 hours
    ↓
Deserialize & Parse
    ↓
Write to PostgreSQL (all records)
```

## Configuration

In `run_consumer.py`, the watermark is applied before streaming:

```python
# Add watermark: allow 2 hours late data
df_with_watermark = (
    df
    .select(
        to_timestamp(col("timestamp")).alias("event_time"),
        col("value")
    )
    .withWatermark("event_time", "2 hours")  # 2-hour grace period
)
```

## What the Watermark Does

1. **Accepts events** that arrive up to 2 hours after their event_time
2. **Triggers results** for time-windowed operations after the grace period
3. **Allows late updates** to be merged into the database via UPSERT
4. **Removes old state** from memory after watermark + grace period

## Database Behavior

All records (including late arrivals) are written to PostgreSQL using **UPSERT** logic:

```sql
INSERT INTO ad_events (...)
VALUES (...)
ON CONFLICT (event_id) DO UPDATE SET
    event_time = EXCLUDED.event_time,
    user_id = EXCLUDED.user_id,
    ...  -- All fields are updated if event_id exists
```

**Result:** Late-arriving events automatically correct their records in the database.

## Example Scenario

```
Event with ID: "evt_123"
Generated at: 2026-03-29 14:00:00 (2 hours ago)
Received at: 2026-03-29 16:15:00 (late by 15 minutes)

Processing:
- Arrives late, but within 2-hour watermark ✓
- UPSERT into database
- If evt_123 already exists → updated with new data
- If evt_123 is new → inserted
```

## Storage Locations

```
output/
├── checkpoints_final_robust/    # Spark checkpoint for recovery
├── ads_events_parquet/          # Parquet fallback (if DB write fails)
└── corrupted_records/           # Malformed records (JSON)
```

## Logs

When running the consumer:

```
[BATCH 28] Processing 195 records
[BATCH 28] Good: 195, Bad: 0
[BATCH 28] ✓ Wrote 195 records to ad_events
[BATCH 28] ✓ Done in 0.42s
```

## Adjusting Watermark Threshold

To change from 2 hours to something else, edit `run_consumer.py`:

```python
# Change this line:
.withWatermark("event_time", "2 hours")

# Examples:
.withWatermark("event_time", "1 hour")     # Stricter
.withWatermark("event_time", "4 hours")    # More lenient
.withWatermark("event_time", "1 day")      # Very lenient
```

## Monitoring

Check logs for batch processing stats:

```bash
tail -f output/consumer.log | grep "BATCH"
```

Look for:
- **BATCH X] Processing N records** - Total received
- **[BATCH X] Good: N, Bad: M** - Parse results
- **[BATCH X] ✓ Wrote N records** - Successfully written
- **[BATCH X] ✗** - Any errors

## Running the Consumer

```bash
python consumer/run_consumer.py
```

The consumer will:
1. Connect to Kafka
2. Apply 2-hour watermark
3. Deserialize Avro records
4. Write to PostgreSQL (with fallback to Parquet)
5. Log bad records to JSON

## Troubleshooting

### High disk usage in checkpoints
- Normal for Spark Structured Streaming
- Delete `output/checkpoints_final_robust/` if needed (will reset from earliest)

### Records not updating
- Check PostgreSQL connection
- Verify `event_id` uniqueness constraint exists
- Look for SQL errors in logs

### All records marked as "Bad"
- Check event deserialization
- Verify Avro schema matches producer
- Inspect output/corrupted_records/ JSON files

## Summary

✅ **Simple** - Uses native Spark watermarking
✅ **Automatic** - Late events update records via UPSERT
✅ **Scalable** - No separate processing needed
✅ **Reliable** - Checkpoints enable recovery

