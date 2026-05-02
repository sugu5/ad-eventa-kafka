"""
Kafka -> PySpark Structured Streaming -> Iceberg Tables
Single file - Raw events + campaign conversion metrics.

Usage:
    python simple_consumer.py
"""
import sys
import traceback
from datetime import datetime
from pathlib import Path

import logging
import pyspark
import urllib.request
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
    greatest,
    least,
    lit,
    max as max_,
    min as min_,
    sum as sum_,
    to_date,
    to_timestamp,
    unix_timestamp,
    when,
    window,
)
from pyspark.sql.types import DoubleType, FloatType, IntegerType, StringType, StructField, StructType

# Path setup
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from ad_stream_producer.config import Config
from ad_stream_producer.logger import get_logger
from consumer.avro_deserializer import load_avro_schema, make_deserialize_udf

logger = get_logger("simple_consumer")

# Output paths
CHECKPOINT_PATH = str(PROJECT_ROOT / "output" / "checkpoints_final_robust")
CORRUPTED_RECORDS_PATH = str(PROJECT_ROOT / "output" / "corrupted_records")
ICEBERG_WAREHOUSE = str(PROJECT_ROOT / "output" / "iceberg_warehouse")
LOCAL_SCHEMA_PATH = PROJECT_ROOT / "schema" / Config.SCHEMA_PATH
CONVERSION_METRICS_CHECKPOINT = str(PROJECT_ROOT / "output" / "checkpoints_campaign_conversion_v1")

WATERMARK_DELAY = "2 hours"
DASHBOARD_WINDOW = "30 seconds"
DASHBOARD_TRIGGER = "30 seconds"
SHUFFLE_PARTITIONS = "8"
CLICK_EVENT_TYPES = ("click",)
CONVERSION_EVENT_TYPES = ("purchase",)

RAW_EVENT_COLUMNS = [
    "event_id",
    "event_time",
    "user_id",
    "campaign_id",
    "ad_id",
    "device",
    "country",
    "event_type",
    "ad_creative_id",
    "engagement_score",
    "user_segment",
    "conversion_value",
    "geo_latitude",
    "event_timestamp",
    "event_date",
    "ingestion_batch_id",
    "ingested_at",
]

CAMPAIGN_METRIC_COLUMNS = [
    "window_start",
    "window_end",
    "window_date",
    "campaign_id",
    "clicks",
    "conversions",
    "attributed_conversions",
    "orphan_conversions",
    "conversion_rate",
    "aggregation_time",
]

# Event schema - v3 (backward compatible)
OUTPUT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("campaign_id", IntegerType(), True),
        StructField("ad_id", IntegerType(), True),
        StructField("device", StringType(), True),
        StructField("country", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("ad_creative_id", StringType(), True),
        StructField("engagement_score", FloatType(), True),
        StructField("user_segment", StringType(), True),
        StructField("conversion_value", DoubleType(), True),
        StructField("geo_latitude", DoubleType(), True),
    ]
)


def create_spark_session():
    """Create Spark session with Kafka and Iceberg support."""
    logging.getLogger("org.apache.kafka").setLevel(logging.ERROR)
    logging.getLogger("kafka").setLevel(logging.ERROR)

    jars_dir = PROJECT_ROOT / "jars"
    jars_dir.mkdir(exist_ok=True)

    kafka_jar_path = jars_dir / "kafka-clients-3.4.1.jar"
    if not kafka_jar_path.exists():
        logger.info("Downloading kafka-clients JAR...")
        try:
            urllib.request.urlretrieve(
                "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar",
                kafka_jar_path,
            )
        except Exception as exc:
            logger.warning(f"Failed to download kafka client jar: {exc}")

    jars = str(kafka_jar_path)
    spark_version = pyspark.__version__

    spark = (
        SparkSession.builder.appName("KafkaIcebergConsumer")
        .config(
            "spark.jars.packages",
            ",".join(
                [
                    f"org.apache.spark:spark-sql-kafka-0-10_2.12:{spark_version}",
                    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2",
                ]
            ),
        )
        .config("spark.jars", jars)
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.streaming.kafka.consumer.cache.enabled", "false")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", ICEBERG_WAREHOUSE)
        .config("spark.sql.defaultCatalog", "local")
        .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    logger.info(f"Spark {spark.version} session created with Iceberg")
    return spark


def ensure_iceberg_tables(spark):
    """Create required Iceberg tables for raw events and dashboard metrics."""
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS ad_events (
            event_id STRING,
            event_time TIMESTAMP,
            user_id STRING,
            campaign_id INT,
            ad_id INT,
            device STRING,
            country STRING,
            event_type STRING,
            ad_creative_id STRING,
            engagement_score FLOAT,
            user_segment STRING,
            conversion_value DOUBLE,
            geo_latitude DOUBLE,
            event_timestamp TIMESTAMP,
            event_date DATE,
            ingestion_batch_id BIGINT,
            ingested_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (event_date)
        TBLPROPERTIES ('format-version'='2')
        """
    )

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS ad_events_late (
            event_id STRING,
            event_time TIMESTAMP,
            user_id STRING,
            campaign_id INT,
            ad_id INT,
            device STRING,
            country STRING,
            event_type STRING,
            ad_creative_id STRING,
            engagement_score FLOAT,
            user_segment STRING,
            conversion_value DOUBLE,
            geo_latitude DOUBLE,
            event_timestamp TIMESTAMP,
            event_date DATE,
            ingestion_batch_id BIGINT,
            ingested_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (event_date)
        TBLPROPERTIES ('format-version'='2')
        """
    )

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS campaign_conversion_metrics (
            window_start TIMESTAMP,
            window_end TIMESTAMP,
            window_date DATE,
            campaign_id INT,
            clicks BIGINT,
            conversions BIGINT,
            attributed_conversions BIGINT,
            orphan_conversions BIGINT,
            conversion_rate DOUBLE,
            aggregation_time TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (window_date)
        TBLPROPERTIES ('format-version'='2')
        """
    )

    for table_name in ("ad_events", "ad_events_late", "campaign_conversion_metrics"):
        spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES ('format-version'='2')")


def align_to_target_columns(spark, source_df: DataFrame, table_name, all_columns):
    """Ensure the merge source has every target column, filling missing optional columns with nulls."""
    target_schema = {field.name: field.dataType for field in spark.table(table_name).schema.fields}

    aligned_df = source_df
    for column in all_columns:
        if column not in aligned_df.columns:
            aligned_df = aligned_df.withColumn(column, lit(None).cast(target_schema[column]))

    return aligned_df.select(*all_columns)


def merge_into_iceberg_table(spark, source_df, table_name, key_columns, all_columns, temp_view_name):
    """Merge a microbatch into an Iceberg table using primary business keys."""
    aligned_source_df = align_to_target_columns(spark, source_df, table_name, all_columns)

    if aligned_source_df.isEmpty():
        return

    aligned_source_df.createOrReplaceTempView(temp_view_name)

    merge_condition = " AND ".join([f"target.{column} = source.{column}" for column in key_columns])
    update_clause = ", ".join([f"{column} = source.{column}" for column in all_columns])
    insert_columns = ", ".join(all_columns)
    insert_values = ", ".join([f"source.{column}" for column in all_columns])

    spark.sql(
        f"""
        MERGE INTO {table_name} AS target
        USING {temp_view_name} AS source
        ON {merge_condition}
        WHEN MATCHED THEN UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})
        """
    )

    spark.catalog.dropTempView(temp_view_name)


def prepare_event_records(events_df, epoch_id):
    """Normalize and annotate event records before writing to Iceberg."""
    return (
        events_df.dropDuplicates(["event_id"])
        .withColumn("event_time", to_timestamp(col("event_time")))
        .withColumn("event_timestamp", to_timestamp(col("event_timestamp")))
        .withColumn("campaign_id", col("campaign_id").cast(IntegerType()))
        .withColumn("ad_id", col("ad_id").cast(IntegerType()))
        .withColumn("event_date", to_date(col("event_timestamp")))
        .withColumn("ingestion_batch_id", lit(int(epoch_id)))
        .withColumn("ingested_at", current_timestamp())
        .select(*RAW_EVENT_COLUMNS)
    )


def recompute_campaign_conversion_metrics_for_range(spark, min_event_timestamp, max_event_timestamp):
    """Recompute dashboard metrics from the authoritative raw Iceberg table for affected windows."""
    metrics_source_df = (
        spark.table("ad_events")
        .where(col("event_timestamp").between(lit(min_event_timestamp), lit(max_event_timestamp)))
        .where(col("event_type").isin(*(CLICK_EVENT_TYPES + CONVERSION_EVENT_TYPES)))
        .select("event_timestamp", "campaign_id", "event_type")
        .withColumn(
            "is_click",
            when(col("event_type").isin(*CLICK_EVENT_TYPES), lit(1)).otherwise(lit(0)),
        )
        .withColumn(
            "is_conversion",
            when(col("event_type").isin(*CONVERSION_EVENT_TYPES), lit(1)).otherwise(lit(0)),
        )
    )

    if metrics_source_df.isEmpty():
        return 0

    aggregated_df = (
        metrics_source_df.groupBy(window(col("event_timestamp"), DASHBOARD_WINDOW), col("campaign_id"))
        .agg(
            sum_("is_click").alias("clicks"),
            sum_("is_conversion").alias("conversions"),
        )
        .select(
            col("window").getField("start").alias("window_start"),
            col("window").getField("end").alias("window_end"),
            to_date(col("window").getField("start")).alias("window_date"),
            col("campaign_id"),
            col("clicks"),
            col("conversions"),
            least(col("clicks"), col("conversions")).alias("attributed_conversions"),
            greatest(col("conversions") - col("clicks"), lit(0)).alias("orphan_conversions"),
            when(
                col("clicks") > 0,
                least(col("clicks"), col("conversions")).cast("double") / col("clicks").cast("double"),
            )
            .otherwise(lit(0.0))
            .cast("double")
            .alias("conversion_rate"),
            current_timestamp().alias("aggregation_time"),
        )
    )

    merge_into_iceberg_table(
        spark,
        aggregated_df,
        "campaign_conversion_metrics",
        ["campaign_id", "window_start", "window_end"],
        CAMPAIGN_METRIC_COLUMNS,
        f"campaign_conversion_metrics_recompute_{int(datetime.now().timestamp())}",
    )
    return aggregated_df.count()


def process_events_batch(batch_df, epoch_id, deserialize_udf):
    """Process raw events, deduplicate by event_id, and write to Iceberg."""
    batch_start = datetime.now()
    spark = batch_df.sparkSession

    if batch_df.isEmpty():
        logger.warning(f"[BATCH {epoch_id}] Empty batch")
        return

    total = batch_df.count()
    logger.info(f"[BATCH {epoch_id}] Processing {total} Kafka records")

    try:
        deserialized = batch_df.select(deserialize_udf(col("value")).alias("json_str"))
        parsed = deserialized.withColumn("data", from_json(col("json_str"), OUTPUT_SCHEMA))

        valid_records = (
            parsed.where("data is not null")
            .select("data.*")
            .withColumn("event_timestamp", to_timestamp(col("event_time")))
            .filter(
                col("event_id").isNotNull()
                & col("campaign_id").isNotNull()
                & col("event_type").isNotNull()
                & col("event_timestamp").isNotNull()
            )
        )

        bad_records = parsed.where("data is null OR data.event_id IS NULL").select(
            col("json_str").alias("corrupted_payload")
        )

        good_count = valid_records.count()
        bad_count = bad_records.count()

        if good_count > 0:
            records_with_time = (
                valid_records.withColumn("current_time", current_timestamp()).withColumn(
                    "lateness_seconds",
                    unix_timestamp(col("current_time")) - unix_timestamp(col("event_timestamp")),
                )
            )

            on_time_records = records_with_time.where(col("lateness_seconds") <= 7200)
            late_records = records_with_time.where(col("lateness_seconds") > 7200)

            on_time_count = on_time_records.count()
            late_count = late_records.count()

            logger.info(
                f"[BATCH {epoch_id}] Valid: {good_count}, On-time: {on_time_count}, "
                f"Late: {late_count}, Bad: {bad_count}"
            )

            if on_time_count > 0:
                try:
                    prepared_on_time = prepare_event_records(on_time_records, epoch_id)
                    merge_into_iceberg_table(
                        spark,
                        prepared_on_time,
                        "ad_events",
                        ["event_id"],
                        RAW_EVENT_COLUMNS,
                        f"ad_events_batch_{epoch_id}",
                    )
                    logger.info(
                        f"[BATCH {epoch_id}] Upserted {prepared_on_time.count()} on-time records into Iceberg table 'ad_events'"
                    )

                    affected_range = prepared_on_time.agg(
                        min_("event_timestamp").alias("min_event_timestamp"),
                        max_("event_timestamp").alias("max_event_timestamp"),
                    ).collect()[0]
                    min_event_timestamp = affected_range["min_event_timestamp"]
                    max_event_timestamp = affected_range["max_event_timestamp"]

                    if min_event_timestamp is not None and max_event_timestamp is not None:
                        metrics_rows = recompute_campaign_conversion_metrics_for_range(
                            spark,
                            min_event_timestamp,
                            max_event_timestamp,
                        )
                        logger.info(
                            f"[BATCH {epoch_id}] Recomputed {metrics_rows} campaign conversion windows"
                        )
                except Exception as exc:
                    logger.error(f"[BATCH {epoch_id}] Writing to Iceberg 'ad_events' failed: {exc}")
                    logger.error(traceback.format_exc())
                    raise

            if late_count > 0:
                try:
                    prepared_late = prepare_event_records(late_records, epoch_id)
                    merge_into_iceberg_table(
                        spark,
                        prepared_late,
                        "ad_events_late",
                        ["event_id"],
                        RAW_EVENT_COLUMNS,
                        f"ad_events_late_batch_{epoch_id}",
                    )
                    logger.info(
                        f"[BATCH {epoch_id}] Upserted {prepared_late.count()} late records into Iceberg table 'ad_events_late'"
                    )
                except Exception as exc:
                    logger.error(f"[BATCH {epoch_id}] Writing to Iceberg 'ad_events_late' failed: {exc}")
                    logger.error(traceback.format_exc())
                    raise

        if bad_count > 0:
            try:
                bad_records.coalesce(1).write.mode("append").format("json").save(CORRUPTED_RECORDS_PATH)
                logger.info(f"[BATCH {epoch_id}] Wrote {bad_count} corrupted records")
            except Exception as exc:
                logger.error(f"[BATCH {epoch_id}] Corrupted records failed: {exc}")
                logger.error(traceback.format_exc())

        duration = (datetime.now() - batch_start).total_seconds()
        logger.info(f"[BATCH {epoch_id}] Done in {duration:.2f}s")

    except Exception as exc:
        logger.error(f"[BATCH {epoch_id}] Batch processing failed: {exc}")
        logger.error(traceback.format_exc())
        raise


def query_exception(query):
    if query is None:
        return None

    try:
        return query.exception()
    except Exception as exc:
        logger.warning(f"Could not read exception for query {query.id}: {exc}")
        return None


def log_query_failure(stream_name, query):
    if query is None:
        return

    failure = query_exception(query)
    if failure:
        logger.error(f"{stream_name} stream failed (query_id={query.id}): {failure}")
    else:
        logger.info(f"{stream_name} stream status: active={query.isActive}, query_id={query.id}")


def stop_query(stream_name, query):
    if query is None:
        return

    try:
        if query.isActive:
            logger.info(f"Stopping {stream_name} stream...")
            query.stop()
    except Exception as exc:
        logger.warning(f"Failed to stop {stream_name} stream cleanly: {exc}")


def start_events_stream(df, deserialize_udf):
    query = (
        df.writeStream.queryName("ad_events_ingestion")
        .foreachBatch(lambda batch_df, epoch_id: process_events_batch(batch_df, epoch_id, deserialize_udf))
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime=DASHBOARD_TRIGGER)
        .start()
    )

    logger.info(f"Events stream started (query_id={query.id})")
    return query


def run_consumer():
    logger.info("=" * 80)
    logger.info("SIMPLIFIED Kafka Consumer Starting")
    logger.info("=" * 80)

    spark = None
    events_query = None

    try:
        spark = create_spark_session()
        ensure_iceberg_tables(spark)

        avro_schema, source = load_avro_schema(
            Config.SCHEMA_REGISTRY_URL, Config.TOPIC, LOCAL_SCHEMA_PATH
        )
        logger.info(f"Loaded Avro schema from {source}")
        schema_broadcast = spark.sparkContext.broadcast(avro_schema)
        deserialize_udf = make_deserialize_udf(Config.SCHEMA_REGISTRY_URL, schema_broadcast)

        bootstrap_csv = ",".join(Config.KAFKA_BOOTSTRAP_SERVERS)
        logger.info(f"Connecting to Kafka: {bootstrap_csv}")

        kafka_df = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap_csv)
            .option("subscribe", Config.TOPIC)
            .option("startingOffsets", "earliest")
            .option("kafka.session.timeout.ms", "30000")
            .option("kafka.request.timeout.ms", "40000")
            .option("kafka.max.poll.interval.ms", "300000")
            .option("failOnDataLoss", "false")
            .load()
        )
        logger.info("Kafka stream connected")

        events_query = start_events_stream(kafka_df, deserialize_udf)

        logger.info("=" * 80)
        logger.info("PIPELINE ACTIVE:")
        logger.info("  1. On-time raw events -> Iceberg table 'ad_events'")
        logger.info("  2. Very late raw events -> Iceberg table 'ad_events_late'")
        logger.info("  3. 30-sec campaign conversion metrics recomputed from 'ad_events' -> Iceberg table 'campaign_conversion_metrics'")
        logger.info("=" * 80)

        events_query.awaitTermination()

    finally:
        stop_query("events", events_query)
        if spark is not None:
            try:
                spark.stop()
            except Exception as exc:
                logger.warning(f"Failed to stop Spark session cleanly: {exc}")


if __name__ == "__main__":
    run_consumer()
