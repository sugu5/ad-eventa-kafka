# Kafka Ad Stream Pipeline

PySpark Structured Streaming pipeline for ad events from Kafka into Apache Iceberg tables, with real-time watermarked metrics and nightly batch reconciliation.

## Quick Start

```bash
cd C:\Users\DELL 7380\Desktop\kafka-develop
python -m consumer.simple_consumer
```

## What It Does

- Reads ad events from Kafka.
- Stores every incoming Kafka record in `raw_ad_events`.
- Parses valid business events into `ad_events`.
- Applies a Spark event-time watermark for streaming campaign metrics.
- Writes serving metrics to `campaign_conversion_metrics`.
- Supports nightly batch reconciliation from the raw source table.

## Iceberg Tables

### raw_ad_events

Source-of-truth table. Every Kafka message is stored before business filtering.

Important columns:

```sql
kafka_topic STRING
kafka_partition INT
kafka_offset BIGINT
kafka_timestamp TIMESTAMP
kafka_timestamp_type INT
key BINARY
value BINARY
deserialized_payload STRING
parse_status STRING
kafka_date DATE
ingestion_batch_id BIGINT
ingested_at TIMESTAMP
```

The natural key is `kafka_topic`, `kafka_partition`, and `kafka_offset`.

### ad_events

Curated valid events parsed from `raw_ad_events` / Kafka microbatches.

Important columns:

```sql
event_id STRING
event_time TIMESTAMP
campaign_id INT
event_type STRING
event_timestamp TIMESTAMP
event_date DATE
ingestion_batch_id BIGINT
ingested_at TIMESTAMP
```

### campaign_conversion_metrics

Serving table for 30-second campaign conversion windows.

Important columns:

```sql
window_start TIMESTAMP
window_end TIMESTAMP
window_date DATE
campaign_id INT
clicks BIGINT
conversions BIGINT
attributed_conversions BIGINT
orphan_conversions BIGINT
conversion_rate DOUBLE
aggregation_time TIMESTAMP
```

## Streaming Flow

Run:

```bash
python -m consumer.simple_consumer
```

The streaming consumer starts two queries:

1. Source and curated event ingestion:
   - Reads Kafka microbatches.
   - Upserts every Kafka record into `raw_ad_events`.
   - Upserts valid parsed events into `ad_events`.

2. Watermarked metrics:
   - Parses valid event rows from Kafka.
   - Applies `withWatermark("event_timestamp", WATERMARK_DELAY)`.
   - Aggregates 30-second campaign windows.
   - Upserts metric windows into `campaign_conversion_metrics`.

Watermarking is not applied to `raw_ad_events`. The raw table must keep all records for reconciliation and audit.

## Daily Reconciliation Batch

Run yesterday's reconciliation:

```bash
python -m consumer.reconciliation_job
```

Backfill or rerun a specific event date:

```bash
python -m consumer.reconciliation_job --date 2026-05-02
```

The batch job:

1. Reads `raw_ad_events`.
2. Parses valid source records for the target event date.
3. Upserts curated rows into `ad_events`.
4. Deletes that day from `campaign_conversion_metrics`.
5. Recomputes and appends corrected 30-second metric windows.

The reconciliation job does not apply a watermark. It is meant to correct the serving table from complete source data.

## Windows Daily Schedule

Use the provided runner:

```powershell
powershell.exe -ExecutionPolicy Bypass -File scripts\run_daily_reconciliation.ps1
```

Schedule it in Windows Task Scheduler for your nightly run time.

## Useful Queries

```sql
SELECT parse_status, COUNT(*)
FROM raw_ad_events
GROUP BY parse_status;

SELECT *
FROM campaign_conversion_metrics
WHERE window_date = DATE '2026-05-02'
ORDER BY window_start, campaign_id;
```

## Verification

Compile the Python entrypoints:

```bash
python -m py_compile consumer\simple_consumer.py consumer\reconciliation_job.py
```
