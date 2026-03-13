from prometheus_client import Counter, Histogram, Gauge, start_http_server

# Total events produced / failed
events_sent = Counter("events_sent_total", "Total events produced")
events_failed = Counter("events_failed_total", "Total failed events")

# Throughput
events_per_second = Gauge("events_per_second", "Events produced per second")

# Delivery latency
producer_latency = Histogram(
    "producer_latency_seconds",
    "Kafka producer delivery latency",
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5]
)

# Producer queue backlog
producer_queue_size = Gauge(
    "producer_queue_size",
    "Kafka producer queue backlog"
)

# Latest acked offset per topic/partition
producer_offset = Gauge(
    "producer_offset",
    "Latest acknowledged producer offset",
    ["topic", "partition"]
)


def start_metrics_server(port=8001):
    start_http_server(port)

