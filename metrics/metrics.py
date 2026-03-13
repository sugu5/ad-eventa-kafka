from prometheus_client import start_http_server

# Producer metrics
from metrics.producer_metrics import (
    events_sent,
    events_failed,
    events_per_second,
    producer_latency,
    producer_queue_size as producer_lag,   # backward-compat alias
    producer_queue_size,
    producer_offset,
)

# Consumer metrics
from metrics.consumer_metrics import (
    records_processed,
    records_failed,
    records_per_second,
    consumer_offset,
    consumer_lag,
    batch_processing_seconds,
)


def start_metrics_server(port=8001):
    start_http_server(port)
