# Late Events Handling - Simple Implementation

## Overview

Events are automatically separated based on arrival time:
- **On-time** (≤ 2 hours late) → PostgreSQL
- **Late** (> 2 hours late) → Separate Parquet files
- **Bad** (corrupted) → JSON files

## How It Works

```
Event arrives
    ↓
Parse & validate
    ↓
Calculate: current_time - event_time
    ↓
    ├─ ≤ 2 hours → Write to PostgreSQL (UPSERT)
    │
    └─ > 2 hours → Write to late_events_parquet/
```

## Configuration

**Watermark Threshold:** 2 hours (in `run_consumer.py`)

```python
WATERMARK_HOURS = 2  # Change this to adjust threshold
```

**Paths:**
- On-time records: PostgreSQL `ad_events` table
- Late records: `output/late_events_parquet/`
- Bad records: `output/corrupted_records/`

## Example Logs

```
[BATCH 28] Processing 200 records
[BATCH 28] Separated: 188 on-time, 12 late (threshold: 2h), Bad: 0
[BATCH 28] ✓ Wrote 188 on-time records to ad_events
[BATCH 28] ✓ Wrote 12 late records to output/late_events_parquet
[BATCH 28] ✓ Done in 0.45s (on-time: 188, late: 12, bad: 0)
```

## Running

```bash
python consumer/run_consumer.py
```

## Storage

Late events are stored as Parquet files:
```
output/late_events_parquet/
├── _SUCCESS
└── part-00000-*.parquet  ← Late events here
```

Each late event contains:
- event_id, event_time, user_id, campaign_id, ad_id
- device, country, event_type, ad_creative_id

## Processing Late Events Later

To process late events in batch:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("LateBatchProcessor").getOrCreate()

# Read late events
late_df = spark.read.parquet("output/late_events_parquet")

# Do whatever you need
late_df.show()
late_df.write.jdbc(...)  # Write to database if needed
```

## Adjusting Threshold

To change from 2 hours to something else:

1. Edit `run_consumer.py`:
   ```python
   WATERMARK_HOURS = 4  # Change to 4 hours, for example
   ```

2. The watermark in the stream will automatically use this:
   ```python
   .withWatermark("event_time", "2 hours")  # This is still 2 hours in the stream
   ```

   Actually, update the stream watermark too:
   ```python
   .withWatermark("event_time", f"{WATERMARK_HOURS} hours")
   ```

## What Happens to Late Events?

Late events are stored separately but **NOT** automatically written to PostgreSQL. You can:

1. **Option A: Batch correct later**
   - Read from `late_events_parquet/`
   - Process and validate
   - Write to PostgreSQL manually

2. **Option B: Write late events to DB immediately**
   - Modify `sink.py` to also write late events to PostgreSQL
   - Add same UPSERT logic for late records

3. **Option C: Archive for later analysis**
   - Keep late events in Parquet for investigation
   - Analyze patterns of late arrivals

## Metrics to Monitor

- **On-time records per batch:** Should be majority
- **Late records per batch:** Watch for spikes
- **Late events path growth:** Monitor disk usage

## Troubleshooting

**All events marked as late?**
- Check event_time format in your data
- Verify current_time calculation
- Look at logs for timestamps

**No late events despite old timestamps?**
- Check watermark threshold setting
- Verify event_time vs current time difference

**Disk filling up?**
- Archive old late events
- Adjust watermark threshold
- Process late events regularly

## Summary

✅ **Simple** - Automatic separation based on time
✅ **Clean** - Late events in separate storage
✅ **Flexible** - Easy to adjust threshold
✅ **Scalable** - No extra processing overhead

