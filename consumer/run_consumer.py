"""
Kafka → PySpark Structured Streaming → PostgreSQL consumer.

Usage:
    python -m consumer.run_consumer
"""
import sys
import logging
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
LOCAL_SCHEMA_PATH = PROJECT_ROOT / "schema" / Config.SCHEMA_PATH


def create_spark_session():
    """Build and return a SparkSession configured for Kafka + PostgreSQL."""
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName("KafkaAdStreamConsumer")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.postgresql:postgresql:42.6.0",
        )
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.streaming.kafka.consumer.cache.enabled", "false")
        .config(
            "spark.sql.streaming.forceDeleteTempCheckpointLocation", "true"
        )
        .getOrCreate()
    )
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

    # ---- Kafka source ----------------------------------------------------
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

    # ---- Sink callback ---------------------------------------------------
    process_batch = make_process_batch(
        deserialize_udf=deserialize_udf,
        postgres_url=Config.POSTGRES_URL,
        postgres_properties=Config.POSTGRES_PROPERTIES,
        postgres_table=Config.POSTGRES_TABLE,
        output_parquet_path=OUTPUT_PARQUET_PATH,
        corrupted_records_path=CORRUPTED_RECORDS_PATH,
        logger=logger,
    )

    # ---- Start stream ----------------------------------------------------
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

