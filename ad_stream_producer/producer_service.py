import time
import traceback
from confluent_kafka.serialization import SerializationContext, MessageField
from ad_stream_producer.kafka_producer import AdKafkaProducer
from ad_stream_producer.logger import get_logger
from ad_stream_producer.schema import AdEvent
from data_generator.event_generator import EventGenerator

logger = get_logger("producer_service")


class ProducerService:
    """
    ProducerService manages the production of ad events to Kafka.
    Orchestrates event generation, serialization, and producer delivery.
    """

    def __init__(self, topic, bootstrap_servers, avro_serializer):
        """
        Initialize the ProducerService.

        Args:
            topic (str): Kafka topic to produce to
            bootstrap_servers (list): List of Kafka bootstrap servers
            avro_serializer: AvroSerializer for event serialization
        """
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.avro_serializer = avro_serializer
        self.producer = AdKafkaProducer(bootstrap_servers)
        self.event_generator = EventGenerator()
        self.events_count = 0
        self.start_time = None

        logger.info(f"ProducerService initialized for topic: {topic}")

    def serialize_event(self, event):
        """
        Serialize an event using the Avro serializer.
        Includes retry logic for transient Schema Registry communication errors.

        Args:
            event (dict): The event to serialize

        Returns:
            bytes: Serialized event data
        """
        max_retries = 3
        for attempt in range(max_retries):
            try:
                return self.avro_serializer(
                    event, 
                    SerializationContext(self.topic, MessageField.VALUE)
                )
            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 0.5
                    logger.warning(f"Serialization failed (attempt {attempt+1}/{max_retries}): {e}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Failed to serialize event after {max_retries} attempts: {e}")
                    logger.error(traceback.format_exc())
                    raise

    def produce_event(self):
        """
        Generate, validate, and produce a single event to Kafka.
        """
        try:
            # Generate event
            event = self.event_generator.generate_event()

            # Validate and convert to dict. Use mode="json" because the 
            # current Avro schema expects event_time as a string.
            validated = AdEvent(**event)
            event_data = validated.model_dump(mode="json")
            
            # Use user_id as partition key to ensure order for the same user
            key = event_data.get("user_id", str(self.events_count))
            
            # Serialize the event
            serialized_value = self.serialize_event(event_data)
            
            # Send to Kafka (librdkafka handles batching in the background)
            self.producer.send_event(
                topic=self.topic,
                key=key,
                value=serialized_value
            )

            self.events_count += 1
            
            if self.events_count % 100 == 0:
                elapsed = time.time() - self.start_time
                current_rate = self.events_count / elapsed if elapsed > 0 else 0
                logger.info(
                    f"Produced {self.events_count} events | "
                    f"Rate: {current_rate:.2f} events/sec | "
                    f"Elapsed: {elapsed:.2f}s"
                )
                
        except Exception as e:
            logger.error(f"Error producing event: {e}")
            logger.error(traceback.format_exc())

    def run(self, rate_per_sec=10):
        """
        Run the producer service to continuously produce events at a specified rate.

        Args:
            rate_per_sec (int): Target production rate in events per second
        """
        logger.info(f"Starting producer with rate: {rate_per_sec} events/sec")
        self.start_time = time.time()
        self.events_count = 0
        
        interval = 1.0 / rate_per_sec
        last_event_time = self.start_time
        
        try:
            while True:
                current_time = time.time()
                time_since_last = current_time - last_event_time
                
                if time_since_last >= interval:
                    self.produce_event()
                    last_event_time = current_time
                else:
                    # Sleep for a short duration to avoid busy-waiting
                    sleep_time = interval - time_since_last
                    time.sleep(min(sleep_time, 0.001))  # Sleep at most 1ms
                    
        except KeyboardInterrupt:
            logger.info("Producer interrupted by user")
        except Exception as e:
            logger.error(f"Critical error in producer run loop: {e}")
            logger.error(traceback.format_exc())
            raise
        finally:
            self.shutdown()

    def shutdown(self):
        """
        Gracefully shutdown the producer service.
        """
        try:
            logger.info(f"Shutting down producer service...")
            logger.info(f"Total events produced: {self.events_count}")
            
            if self.start_time:
                elapsed = time.time() - self.start_time
                avg_rate = self.events_count / elapsed if elapsed > 0 else 0
                logger.info(f"Average rate: {avg_rate:.2f} events/sec")
                logger.info(f"Total runtime: {elapsed:.2f}s")
            
            if self.producer:
                self.producer.close()
                logger.info("Producer closed successfully")
                
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")
            logger.error(traceback.format_exc())
