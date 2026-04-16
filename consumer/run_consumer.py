"""
Kafka → PySpark Structured Streaming → PostgreSQL consumer.

Usage:
    python -m consumer.run_consumer
"""
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Path setup — must happen before any local imports
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from ad_stream_producer.config import Config
from ad_stream_producer.logger import get_logger
from consumer.avro_deserializer import load_avro_schema, make_deserialize_udf
from consumer.sink import make_process_batch

logger = get_logger("consumer")

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
CHECKPOINT_PATH = str(PROJECT_ROOT / "output" / "checkpoints_final_robust")
CORRUPTED_RECORDS_PATH = str(PROJECT_ROOT / "output" / "corrupted_records")
OUTPUT_PARQUET_PATH = str(PROJECT_ROOT / "output" / "ads_events_parquet")
LATE_EVENTS_PATH = str(PROJECT_ROOT / "output" / "late_events_parquet")
LOCAL_SCHEMA_PATH = PROJECT_ROOT / "schema" / Config.SCHEMA_PATH


def create_spark_session():
    """Build and return a SparkSession configured for Kafka + PostgreSQL."""
    import pyspark
    from pyspark.sql import SparkSession
    import urllib.request
    from pathlib import Path
    import logging
    
    # Suppress known Kafka warnings
    logging.getLogger("org.apache.kafka").setLevel(logging.ERROR)
    logging.getLogger("kafka").setLevel(logging.ERROR)
    
    # Path to log4j.properties
    log4j_path = Path(__file__).resolve().parent.parent / "log4j.properties"
    
    # Create a jars directory if it doesn't exist
    jars_dir = Path(__file__).resolve().parent.parent / "jars"
    jars_dir.mkdir(exist_ok=True)
    
    # Download kafka-clients JAR if not present
    kafka_jar_path = jars_dir / "kafka-clients-3.4.1.jar"
    if not kafka_jar_path.exists():
        logger.info("Downloading kafka-clients JAR...")
        url = "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar"
        try:
            urllib.request.urlretrieve(url, kafka_jar_path)
            logger.info(f"✓ Downloaded kafka-clients JAR to {kafka_jar_path}")
        except Exception as e:
            logger.warning(f"Failed to download kafka-clients: {e}")
    
    # Download postgresql JAR if not present
    pg_jar_path = jars_dir / "postgresql-42.6.0.jar"
    if not pg_jar_path.exists():
        logger.info("Downloading postgresql JAR...")
        url = "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar"
        try:
            urllib.request.urlretrieve(url, pg_jar_path)
            logger.info(f"✓ Downloaded postgresql JAR to {pg_jar_path}")
        except Exception as e:
            logger.warning(f"Failed to download postgresql: {e}")
    
    # Build jars string
    jars = f"{kafka_jar_path},{pg_jar_path}"

    spark_version = pyspark.__version__
    kafka_packages = ",".join(
        [
            f"org.apache.spark:spark-sql-kafka-0-10_2.12:{spark_version}",
        ]
    )

    builder = (
        SparkSession.builder
        .appName("KafkaAdStreamConsumer")
        .config("spark.jars.packages", kafka_packages)
        .config("spark.jars", jars)
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.streaming.kafka.consumer.cache.enabled", "false")
        .config(
            "spark.sql.streaming.forceDeleteTempCheckpointLocation", "true"
        )
        .config("spark.driver.extraJavaOptions", f"-Xss4m -Dlog4j.logger.org.apache.kafka=ERROR -Dlog4j.logger.org.apache.kafka.common.network.KafkaDataConsumer=ERROR")
        .config("spark.executor.extraJavaOptions", "-Xss4m")
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    logger.info(f"✓ Spark {spark.version} session created")
    return spark


def main():
    logger.info("=" * 80)
    logger.info("Kafka Consumer Starting")
    logger.info("=" * 80)

    # ---- Spark -----------------------------------------------------------
    spark = create_spark_session()

    # ---- Avro schema (registry → local fallback) -------------------------
    avro_schema, source = load_avro_schema(
        Config.SCHEMA_REGISTRY_URL, Config.TOPIC, LOCAL_SCHEMA_PATH
    )
    logger.info(f"✓ Loaded Avro schema from {source}")
    schema_broadcast = spark.sparkContext.broadcast(avro_schema)
    deserialize_udf = make_deserialize_udf(schema_broadcast)

    # ---- Kafka source ---------------------------------------------------
    bootstrap_csv = ",".join(Config.KAFKA_BOOTSTRAP_SERVERS)
    logger.info(f"Connecting to Kafka ({bootstrap_csv}) topic: {Config.TOPIC}")

    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_csv)
        .option("subscribe", Config.TOPIC)
        .option("startingOffsets", "earliest")
        .option("kafka.session.timeout.ms", "30000")
        .option("kafka.request.timeout.ms", "40000")
        .option("kafka.max.poll.interval.ms", "300000")
        .option("failOnDataLoss", "false")
        .load()
    )
    logger.info("✓ Kafka stream connected")

    # ---- Optional: Add watermark if using windowing/aggregations --------
    # Uncomment below if adding window().groupBy().agg() operations
    # Watermark: 2 hours grace period for late events in aggregations
    # from pyspark.sql.functions import to_timestamp, col, window
    # df = (
    #     df
    #     .select(
    #         to_timestamp(col("timestamp")).alias("event_time"),
    #         col("value")
    #     )
    #     .withWatermark("event_time", "2 hours")  # Grace period + state cleanup
    # )
    #
    # Example windowing query (if needed):
    # df_windowed = (
    #     df
    #     .groupBy(window(col("event_time"), "5 minutes"))
    #     .count()  # Aggregate: count events per 5-min window
    # )

    # ---- Sink callback ---------------------------------------------------
    process_batch = make_process_batch(
        deserialize_udf=deserialize_udf,
        postgres_url=Config.POSTGRES_URL,
        postgres_properties=Config.POSTGRES_PROPERTIES,
        postgres_table=Config.POSTGRES_TABLE,
        output_parquet_path=OUTPUT_PARQUET_PATH,
        corrupted_records_path=CORRUPTED_RECORDS_PATH,
        late_events_path=LATE_EVENTS_PATH,
        logger=logger,
    )

    # ---- Start stream -------------------------------------------------------
    query = (
        df.writeStream
        .foreachBatch(process_batch)
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .start()
    )

    logger.info(f"✓ Stream started  query_id={query.id}")
    logger.info("Awaiting termination …")
    query.awaitTermination()


if __name__ == "__main__":
    main()
