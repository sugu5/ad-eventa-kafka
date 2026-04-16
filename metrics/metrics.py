from prometheus_client import Counter, Histogram, Gauge, start_http_server, Info

# ============================================================================
# PRODUCER METRICS
# ============================================================================

# counters
events_sent = Counter("events_sent_total", "Total events produced")
events_failed = Counter("events_failed_total", "Total failed events")

# rate
events_per_second = Gauge("events_per_second", "Events produced per second")

# latency
producer_latency = Histogram(
    "producer_latency_seconds",
    "Kafka producer delivery latency",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0)
)

# queue backlog
producer_queue_size = Gauge(
    "producer_queue_size",
    "Kafka producer queue backlog"
)

# producer offset per partition
producer_offset = Gauge(
    "producer_offset",
    "Current producer offset per partition",
    labelnames=["topic", "partition"]
)

# ============================================================================
# CONSUMER METRICS
# ============================================================================

# consumer offset per partition
consumer_offset = Gauge(
    "consumer_offset",
    "Current consumer offset per partition",
    labelnames=["topic", "partition", "group"]
)

# consumer lag per partition
consumer_lag = Gauge(
    "consumer_lag",
    "Consumer lag per partition (distance from head)",
    labelnames=["topic", "partition", "group"]
)

# total consumer lag per group
consumer_lag_total = Gauge(
    "consumer_lag_total",
    "Total consumer lag for group",
    labelnames=["topic", "group"]
)

# consumer processing rate
consumer_records_processed = Counter(
    "consumer_records_processed_total",
    "Total records processed by consumer",
    labelnames=["topic", "group"]
)

# ============================================================================
# SYSTEM METRICS
# ============================================================================

# topic info
topic_info = Info("kafka_topic", "Topic metadata", labelnames=["topic"])

# partition count
topic_partitions = Gauge(
    "topic_partitions",
    "Number of partitions per topic",
    labelnames=["topic"]
)

# ============================================================================
# BATCH PROCESSING METRICS (for streaming)
# ============================================================================

batch_size = Gauge(
    "batch_size",
    "Records in current batch",
    labelnames=["topic", "group"]
)

batch_processing_time = Histogram(
    "batch_processing_seconds",
    "Batch processing time",
    labelnames=["topic", "group"],
    buckets=(0.1, 0.5, 1.0, 2.5, 5.0, 10.0)
)

def start_metrics_server():
    """Start the Prometheus metrics HTTP server on port 8001."""
    start_http_server(8001)