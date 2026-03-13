from prometheus_client import Counter, Gauge, start_http_server

# Total records processed / failed
records_processed = Counter("consumer_records_processed_total", "Total records successfully processed")
records_failed = Counter("consumer_records_failed_total", "Total corrupted/failed records")

# Throughput
records_per_second = Gauge("consumer_records_per_second", "Records processed per second")

# Latest consumed offset per topic/partition
consumer_offset = Gauge(
    "consumer_offset",
    "Latest processed consumer offset",
    ["topic", "partition"]
)

# Lag behind broker head per topic/partition
consumer_lag = Gauge(
    "consumer_lag",
    "Consumer lag by topic and partition (broker end offset - processed offset)",
    ["topic", "partition"]
)

# Batch processing duration
batch_processing_seconds = Gauge(
    "consumer_batch_processing_seconds",
    "Time taken to process the last micro-batch"
)


def start_metrics_server(port=8005):
    start_http_server(port)

