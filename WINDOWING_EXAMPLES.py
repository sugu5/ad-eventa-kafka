"""
WINDOWING WITH WATERMARK + LATE EVENTS EXAMPLE

This shows how to use windowing/aggregations with watermark.
Useful when you need to count/aggregate events per time window.

IMPORTANT:
- Watermark is ONLY for windowing/aggregations
- Watermark + Grace Period handles late event updates
- Simple streaming (current approach) doesn't need watermark
"""

# ============================================================================
# EXAMPLE 1: Simple windowing (count events per 5-minute window)
# ============================================================================

from pyspark.sql.functions import (
    to_timestamp, col, window, count, sum as spark_sum,
    avg, min as spark_min, max as spark_max
)

# Step 1: Add event_time and watermark
df_with_watermark = (
    df
    .select(
        to_timestamp(col("timestamp")).alias("event_time"),
        col("value")
    )
    .withWatermark("event_time", "2 hours")  # Grace period for late events
)

# Step 2: Create 5-minute windows
df_windowed = (
    df_with_watermark
    .groupBy(window(col("event_time"), "5 minutes"))  # 5-min windows
    .agg(
        count("*").alias("event_count")  # Count events per window
    )
)

# Step 3: Write results
query = (
    df_windowed.writeStream
    .outputMode("update")  # Update: sends new + changed windows
    .format("console")
    .option("truncate", False)
    .start()
)


# ============================================================================
# EXAMPLE 2: Windowing with late event handling
# ============================================================================

df_with_watermark = (
    df
    .select(
        to_timestamp(col("timestamp")).alias("event_time"),
        col("campaign_id"),
        col("ad_id"),
        col("value")
    )
    .withWatermark("event_time", "2 hours")
)

# Window by campaign - count events per campaign per 5-minute window
df_campaign_windows = (
    df_with_watermark
    .groupBy(
        window(col("event_time"), "5 minutes"),
        col("campaign_id")
    )
    .agg(
        count("*").alias("total_events"),
        count(col("ad_id")).alias("ad_impressions")
    )
)


# ============================================================================
# HOW WATERMARK + LATE EVENTS WORKS IN WINDOWING:
# ============================================================================

"""
Timeline:
---------
Event time: 10:00 AM (event generates)
Received:   10:02 AM (arrives 2 min late)  ← Within grace period ✓

Window:     10:00-10:05 (already sent at 10:05)
Late event: 10:02 (arrives late)
Grace:      2 hours (10:05 + 2 hours = 12:05)

Result: Event UPDATES the window output ✓


Timeline 2:
-----------
Event time: 10:00 AM
Received:   3 hours later (10:00 + 3 hours = 1:00 PM)  ← After grace period ✗

Window:     10:00-10:05 (already sent at 10:05)
Late event: At 1:00 PM
Grace:      Expired at 12:05 PM

Result: Event is DROPPED by Spark ✗
        → Goes to late_events_parquet/ via sink.py logic


Output Modes with Windows:
---------
.outputMode("append")  
  - Final windows only (after watermark passes)
  
.outputMode("update")  
  - New windows + updated windows (from late events)
  
.outputMode("complete")
  - ALL windows (can be expensive)
"""


# ============================================================================
# COMBINED APPROACH: Windowing + Late Event Storage
# ============================================================================

"""
STRATEGY:
1. Add watermark for windowing (2 hours grace)
2. Send windowed results to database/console
3. Late events (after grace period) go to sink.py → Parquet storage

Flow:
    df (ALL records)
        ↓
    .withWatermark("event_time", "2 hours")
        ↓
    .groupBy(window(col("event_time"), "5 minutes"))
    .agg(count("*"))
        ↓
    Spark automatically:
    - Outputs window at watermark + grace period
    - Drops old state (prevents memory bloat)
    - Late events (> 2h) are dropped by Spark
        ↓
    sink.py catches those records and stores to Parquet ✓
"""

# Implementation:
# 1. Create watermarked stream with windowing
df_windowed = (
    df
    .select(
        to_timestamp(col("timestamp")).alias("event_time"),
        col("value")
    )
    .withWatermark("event_time", "2 hours")  # Grace period
    .groupBy(window(col("event_time"), "5 minutes"))
    .agg(count("*").alias("count"))
)

# 2. Stream windowed results
windowed_query = (
    df_windowed.writeStream
    .outputMode("update")
    .format("memory")
    .queryName("windowed_aggregation")
    .start()
)

# 3. Separately, stream raw records to sink for late event capture
# (This is the current approach in run_consumer.py)
raw_query = (
    df.writeStream
    .foreachBatch(process_batch)  # Captures late events
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .start()
)

