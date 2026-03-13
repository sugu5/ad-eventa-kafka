from prometheus_client import Counter, Histogram, Gauge, start_http_server

# counters
events_sent = Counter("events_sent_total", "Total events produced")
events_failed = Counter("events_failed_total", "Total failed events")

# rate
events_per_second = Gauge("events_per_second", "Events produced per second")

# latency
producer_latency = Histogram(
    "producer_latency_seconds",
    "Kafka producer delivery latency"
)

# lag simulation
producer_lag = Gauge(
    "producer_queue_size",
    "Kafka producer queue backlog"
)

# latest acked producer offset per topic/partition
producer_offset = Gauge(
    "producer_offset",
    "Latest acknowledged producer offset",
    ["topic", "partition"]
)

# latest processed consumer offset per topic/partition
consumer_offset = Gauge(
    "consumer_offset",
    "Latest processed consumer offset",
    ["topic", "partition"]
)

# consumer lag per topic/partition (broker end offset - processed offset)
consumer_lag = Gauge(
    "consumer_lag",
    "Consumer lag by topic and partition",
    ["topic", "partition"]
)


def start_metrics_server(port=8085):
    start_http_server(port)
