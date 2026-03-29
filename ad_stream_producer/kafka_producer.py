import traceback
from confluent_kafka import Producer
from .logger import get_logger
from metrics.metrics import events_sent, events_failed

logger = get_logger("producer")


class AdKafkaProducer:
    """
    Kafka producer wrapper using confluent_kafka.Producer.
    Uses asynchronous produce() with delivery callbacks for high throughput.
    """

    def __init__(self, bootstrap_servers):
        try:
            conf = {
                "bootstrap.servers": ",".join(bootstrap_servers),
                "acks": "all",
                "retries": 10,
                "linger.ms": 20,
                "batch.size": 32768,
                "compression.type": "gzip",
                "max.in.flight.requests.per.connection": 5,
            }
            self.producer = Producer(conf)
            logger.info("Kafka producer initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {e}")
            logger.error(traceback.format_exc())
            raise

    @staticmethod
    def _delivery_callback(err, msg):
        """Callback invoked once per message to indicate delivery result."""
        if err is not None:
            events_failed.inc()
            logger.error(
                f"Delivery failed for key '{msg.key()}': {err}"
            )
        else:
            events_sent.inc()
            logger.debug(
                f"Delivered key='{msg.key()}' to "
                f"{msg.topic()}[{msg.partition()}] @offset {msg.offset()}"
            )

    def send_event(self, topic, key, value):
        """
        Asynchronously produce a serialized event to Kafka.
        Calls poll(0) to trigger any pending delivery callbacks.
        """
        try:
            self.producer.produce(
                topic=topic,
                key=key.encode("utf-8") if isinstance(key, str) else key,
                value=value,
                callback=self._delivery_callback,
            )
            # Serve delivery callbacks from previous produce() calls
            self.producer.poll(0)
        except BufferError:
            logger.warning("Producer queue full — flushing before retry")
            self.producer.flush(timeout=5)
            self.producer.produce(
                topic=topic,
                key=key.encode("utf-8") if isinstance(key, str) else key,
                value=value,
                callback=self._delivery_callback,
            )
        except Exception as e:
            events_failed.inc()
            logger.error(f"Failed to produce event with key '{key}' to topic {topic}: {e}")
            logger.error(traceback.format_exc())

    def close(self):
        """Flush remaining messages and clean up."""
        logger.info("Closing Kafka producer")
        try:
            remaining = self.producer.flush(timeout=30)
            if remaining > 0:
                logger.warning(f"{remaining} message(s) were not delivered")
            logger.info("Kafka producer closed successfully")
        except Exception as e:
            logger.error(f"Error closing Kafka producer: {e}")