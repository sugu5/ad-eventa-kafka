# Iceberg-Only Setup for Local Ad Events

This guide explains the Iceberg-based architecture. PostgreSQL has been completely removed.

## Architecture

All ad events are written directly to **Iceberg tables** for ACID guarantees, partition-aware queries, and time-travel.

### Tables Created Automatically

| Table | Purpose | Data Source |
|-------|---------|-------------|
| `ad_events` | On-time events (≤2h latency) | Kafka stream |
| `ad_events_late` | Late-arriving events (>2h latency) | Kafka stream |
| `ad_events_aggregation` | 5-minute window aggregations | Kafka stream |

---

## Quick Start: Run with Iceberg

```bash
python consumer/simple_consumer.py
```

**That's it!** The consumer will:  
✅ Read from Kafka  
✅ Deserialize Avro events  
✅ Separate on-time vs. late events  
✅ Write directly to Iceberg tables in `output/iceberg_warehouse/`  
✅ Create 5-minute windowed aggregations  

---

## Query Iceberg Tables

### 1. Using Spark SQL

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("QueryIceberg") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "output/iceberg_warehouse") \
    .getOrCreate()

# Query on-time events by date range
spark.sql("""
    SELECT event_type, device, COUNT(*) as count
    FROM local.ad_events
    WHERE event_date >= '2024-04-20' AND event_date <= '2024-04-21'
    GROUP BY event_type, device
    ORDER BY count DESC
""").show()

# Query aggregations
spark.sql("""
    SELECT event_type, device, event_count, window_start, window_end
    FROM local.ad_events_aggregation
    ORDER BY window_end DESC
    LIMIT 20
""").show()
```

### 2. Time-Travel (Go Back in Time)

```python
spark.sql("""
    SELECT COUNT(*) as event_count
    FROM local.ad_events
    FOR SYSTEM_TIME AS OF TIMESTAMP '2024-04-20 10:30:00'
""").show()

# Or by snapshot ID:
spark.sql("""
    SELECT COUNT(*) as event_count
    FROM local.ad_events
    FOR SYSTEM_VERSION AS OF 42
""").show()
```

### 3. View Table Metadata & History

```python
# Table history
spark.sql("SELECT * FROM local.ad_events.history").show()

# Snapshots
spark.sql("SELECT * FROM local.ad_events.snapshots").show()

# Partitions & Splits (for query planning)
spark.sql("SELECT * FROM local.ad_events.partitions").show()
```

---

## File Layout

```
output/iceberg_warehouse/
├── ad_events/                   # On-time events table
│   ├── metadata/
│   │   ├── version-*.json
│   │   ├── snap-*.avro
│   │   └── ...
│   └── data/
│       ├── event_date=2024-04-20/parquet files...
│       └── event_date=2024-04-21/parquet files...
├── ad_events_late/              # Late events table
├── ad_events_aggregation/       # Aggregation stats table
└── corrupted_records/           # (JSON logs of bad Avro records)
```

---

## Iceberg Features Used

### ✅ ACID Writes
Multiple write streams append atomically. No conflicts or data loss.

### ✅ Partition Pruning
Queries on `event_date` automatically skip irrelevant partitions.

### ✅ Snappy Compression
Each Parquet file inside Iceberg is compressed.

### ✅ Time-Travel
Restore state at any snapshot or timestamp.

### ✅ Schema Evolution
Add/remove columns without rewriting data.

---

## Performance Tips

### 1. Filter by Partition Key
Always filter by `event_date` when possible:
```sql
-- ⭐ FAST: Partition-aware
SELECT * FROM local.ad_events
WHERE event_date = '2024-04-20'
LIMIT 100;

-- 🐢 SLOW: Full table scan
SELECT * FROM local.ad_events
WHERE event_id = 'abc-123';
```

### 2. Use Aggregate Pushdown
Let Spark handle large aggregations on Iceberg:
```sql
-- Efficient: aggregated at read time
SELECT event_type, COUNT(*)
FROM local.ad_events
GROUP BY event_type;
```

### 3. Limit Date Range
```sql
SELECT * FROM local.ad_events
WHERE event_date BETWEEN '2024-04-15' AND '2024-04-20'
LIMIT 1000;
```

---

## Configuration

No environment variables needed! The consumer auto-configures Iceberg catalogwith:
- **Warehouse:** `output/iceberg_warehouse/`  
- **Catalog:** `local` (Hadoop file-based)  
- **Format Version:** 2 (full ACID support)  

To customize the warehouse location:
```bash
export ICEBERG_WAREHOUSE=/custom/path
python consumer/simple_consumer.py
```

---

## Comparison: Iceberg vs. PostgreSQL

| Aspect | Iceberg | PostgreSQL |
|--------|---------|-----------|
| **Write Speed** | ⭐⭐⭐⭐⭐ Fast (parallel partitions) | ⭐⭐⭐ Moderate (upsert overhead) |
| **Query Speed** | ⭐⭐⭐⭐⭐ Fast (partition pruning) | ⭐⭐⭐⭐ Good (indexes) |
| **Storage** | 1x (Parquet) | 2x+ (indexes + WAL) |
| **ACID Guarantees** | ✅ Full | ✅ Full |
| **Time-Travel** | ✅ Native | ❌ No |
| **Schema Changes** | ✅ Easy | ❌ Locks table |
| **Network Required** | ❌ No | ✅ Yes |
| **Partition Pruning** | ✅ Automatic | ❌ Manual |

---

## Troubleshooting

**Q: Where are my Iceberg files?**  
A: Check `output/iceberg_warehouse/ad_events/data/event_date=YYYY-MM-DD/`

**Q: How do I inspect Iceberg metadata?**  
```python
spark.sql("SELECT * FROM local.ad_events.metadata_log_entries").show()
```

**Q: Can I convert existing Parquet to Iceberg?**  
Yes! See [Spark Iceberg Migration Docs](https://iceberg.apache.org/docs/latest/spark-quickstart/)

**Q: How do I delete a table?**  
```sql
DROP TABLE local.ad_events;
```

**Q: How do I reset the warehouse?**  
```bash
rm -rf output/iceberg_warehouse/
python consumer/simple_consumer.py  # Recreates tables
```

---

## Next Steps

1. Run the consumer: `python consumer/simple_consumer.py`
2. Produce some events to Kafka
3. Query Iceberg tables from another Spark session
4. Explore time-travel and snapshots
5. Migrate other streaming pipelines to Iceberg!


