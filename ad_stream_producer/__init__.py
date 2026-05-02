"""
This file makes the 'ad_stream_producer' directory a Python package
and promotes key classes to the package level for easier imports.
"""
from ad_stream_producer.producer_service import ProducerService
from ad_stream_producer.schema import AdEvent
from ad_stream_producer.kafka_producer import AdKafkaProducer
from ad_stream_producer.config import Config
__all__ = [
    "ProducerService",
    "AdEvent",
    "Config",
    "AdKafkaProducer"
]