import traceback
import time
from confluent_kafka import Producer
from .logger import get_logger

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
                "linger.ms": 50,
                "batch.size": 32768,
                "compression.type": "gzip",
                # Increase local queue size to handle bursts better
                "queue.buffering.max.messages": 100000,
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
            logger.error(
                f"Delivery failed for key '{msg.key()}': {err}"
            )
        else:
            logger.debug(
                f"Delivered key='{msg.key()}' to "
                f"{msg.topic()}[{msg.partition()}] @offset {msg.offset()}"
            )

    def send_event(self, topic, key, value):
        """
        Asynchronously produce a serialized event to Kafka.
        Calls poll(0) to trigger any pending delivery callbacks.
        """
        max_retries = 5
        retry_delay = 0.1  # 100ms

        for attempt in range(max_retries):
            try:
                self.producer.produce(
                    topic=topic,
                    key=key.encode("utf-8") if isinstance(key, str) else key,
                    value=value,
                    callback=self._delivery_callback,
                )
                self.producer.poll(0)
                return
            except BufferError:
                logger.warning(f"Producer queue full (attempt {attempt+1}/{max_retries}). Polling and backing off...")
                # Serve callbacks to free up space
                self.producer.poll(1) 
                time.sleep(retry_delay * (2 ** attempt)) # Exponential backoff
            except Exception as e:
                logger.error(f"Failed to produce event with key '{key}': {e}")
                break

        logger.error(f"Dropped event with key '{key}' after {max_retries} retries due to BufferError")

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