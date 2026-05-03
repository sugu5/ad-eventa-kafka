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
    sum as sum_,
    to_date,
    to_timestamp,
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
METRICS_CHECKPOINT_PATH = str(PROJECT_ROOT / "output" / "checkpoints_campaign_conversion_watermarked")
ICEBERG_WAREHOUSE = str(PROJECT_ROOT / "output" / "iceberg_warehouse")
LOCAL_SCHEMA_PATH = PROJECT_ROOT / "schema" / Config.SCHEMA_PATH

WATERMARK_DELAY = "2 hours"
DASHBOARD_WINDOW = "30 seconds"
DASHBOARD_TRIGGER = "30 seconds"
SHUFFLE_PARTITIONS = "8"
MAX_OFFSETS_PER_TRIGGER = "2000"
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

RAW_KAFKA_EVENT_COLUMNS = [
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
    "kafka_timestamp_type",
    "key",
    "value",
    "deserialized_payload",
    "parse_status",
    "kafka_date",
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
        .config("spark.driver.memory", "4g")
        .config("spark.python.worker.reuse", "true")
        .config("spark.sql.execution.python.processor.window.size", "2000")
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
        CREATE TABLE IF NOT EXISTS raw_ad_events (
            kafka_topic STRING,
            kafka_partition INT,
            kafka_offset BIGINT,
            kafka_timestamp TIMESTAMP,
            kafka_timestamp_type INT,
            key BINARY,
            value BINARY,
            deserialized_payload STRING,
            parse_status STRING,
            kafka_date DATE,
            ingestion_batch_id BIGINT,
            ingested_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (kafka_date)
        TBLPROPERTIES ('format-version'='2')
        """
    )

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

    for table_name in ("raw_ad_events", "ad_events", "campaign_conversion_metrics"):
        spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES ('format-version'='2')")

    # Schema evolution: Ensure all columns required for v3 exist in the raw events tables.
    # This prevents AnalysisExceptions if the tables were created with older schema versions.
    column_definitions = {
        "engagement_score": "FLOAT",
        "user_segment": "STRING",
        "conversion_value": "DOUBLE",
        "geo_latitude": "DOUBLE",
        "event_timestamp": "TIMESTAMP",
        "ingestion_batch_id": "BIGINT",
        "ingested_at": "TIMESTAMP"
    }
    for table_name in ("ad_events",):
        spark.catalog.refreshTable(table_name)
        existing_cols = [f.name for f in spark.table(table_name).schema.fields]
        for col_name, col_type in column_definitions.items():
            if col_name not in existing_cols:
                logger.info(f"Evolving schema: adding {col_name} to {table_name}")
                spark.sql(f"ALTER TABLE {table_name} ADD COLUMNS ({col_name} {col_type})")
        spark.catalog.refreshTable(table_name)


def align_to_target_columns(spark, source_df: DataFrame, table_name, all_columns):
    """Ensure the merge source has every target column, filling missing optional columns with nulls."""
    spark.catalog.refreshTable(table_name)
    target_schema = {field.name: field.dataType for field in spark.table(table_name).schema.fields}

    aligned_df = source_df
    for column in all_columns:
        if column not in aligned_df.columns:
            aligned_df = aligned_df.withColumn(column, lit(None).cast(target_schema[column]))

    return aligned_df.select(*all_columns)


def merge_into_iceberg_table(spark, source_df, table_name, key_columns, all_columns, temp_view_name):
    """Merge a microbatch into an Iceberg table using primary business keys."""
    aligned_source_df = align_to_target_columns(spark, source_df, table_name, all_columns)

    aligned_source_df.createOrReplaceTempView(temp_view_name)

    # Force a cache clear for the target table to prevent AnalysisExceptions 
    # when processing high-volume batches after a schema change.
    spark.catalog.refreshTable(table_name)

    def quoted(column):
        return f"`{column}`"

    merge_condition = " AND ".join([f"target.{quoted(column)} = source.{quoted(column)}" for column in key_columns])
    update_clause = ", ".join([f"{quoted(column)} = source.{quoted(column)}" for column in all_columns])
    insert_columns = ", ".join([quoted(column) for column in all_columns])
    insert_values = ", ".join([f"source.{quoted(column)}" for column in all_columns])

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


def prepare_raw_kafka_records(parsed_df, epoch_id):
    """Store every Kafka message before any business filtering."""
    return (
        parsed_df
        .withColumn(
            "parse_status",
            when(col("json_str").isNull(), lit("deserialization_failed"))
            .when(col("data").isNull(), lit("json_parse_failed"))
            .when(col("data.event_id").isNull(), lit("missing_event_id"))
            .otherwise(lit("parsed"))
        )
        .withColumn("kafka_date", to_date(col("kafka_timestamp")))
        .withColumn("ingestion_batch_id", lit(int(epoch_id)))
        .withColumn("ingested_at", current_timestamp())
        .select(*RAW_KAFKA_EVENT_COLUMNS)
    )


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


def build_valid_events_stream(df, deserialize_udf):
    """Parse Kafka records into valid event-time rows for downstream streaming logic."""
    return (
        df.select(deserialize_udf(col("value")).alias("json_str"))
        .withColumn("data", from_json(col("json_str"), OUTPUT_SCHEMA))
        .where("data is not null")
        .select("data.*")
        .withColumn("event_timestamp", to_timestamp(col("event_time")))
        .filter(
            col("event_id").isNotNull()
            & col("campaign_id").isNotNull()
            & col("event_type").isNotNull()
            & col("event_timestamp").isNotNull()
        )
    )


def build_watermarked_campaign_metrics_stream(df, deserialize_udf):
    """Build real-time campaign metrics with an event-time watermark."""
    valid_events = build_valid_events_stream(df, deserialize_udf)

    metrics_source_df = (
        valid_events.withWatermark("event_timestamp", WATERMARK_DELAY)
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

    return (
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


def recompute_campaign_conversion_metrics_for_range(spark, min_event_timestamp, max_event_timestamp):
    """Recompute dashboard metrics from the curated events table for affected windows."""
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


def process_metrics_batch(batch_df, epoch_id):
    """Merge watermarked streaming metric updates into Iceberg."""
    spark = batch_df.sparkSession

    if batch_df.isEmpty():
        logger.info(f"[METRICS BATCH {epoch_id}] Empty batch")
        return

    try:
        merge_into_iceberg_table(
            spark,
            batch_df,
            "campaign_conversion_metrics",
            ["campaign_id", "window_start", "window_end"],
            CAMPAIGN_METRIC_COLUMNS,
            f"campaign_conversion_metrics_stream_{epoch_id}",
        )
        logger.info(
            f"[METRICS BATCH {epoch_id}] Upserted {batch_df.count()} watermarked campaign conversion windows"
        )
    except Exception as exc:
        logger.error(f"[METRICS BATCH {epoch_id}] Writing campaign metrics failed: {exc}")
        logger.error(traceback.format_exc())
        raise


def process_events_batch(batch_df, epoch_id, deserialize_udf):
    """Store source records and curate valid events into Iceberg."""
    batch_start = datetime.now()
    spark = batch_df.sparkSession

    if batch_df.isEmpty():
        logger.warning(f"[BATCH {epoch_id}] Empty batch")
        return

    total = batch_df.count()
    logger.info(f"[BATCH {epoch_id}] Processing {total} Kafka records")

    try:
        kafka_records = batch_df.select(
            col("topic").alias("kafka_topic"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp"),
            col("timestampType").alias("kafka_timestamp_type"),
            col("key"),
            col("value"),
            deserialize_udf(col("value")).alias("json_str"),
        )
        parsed = kafka_records.withColumn("data", from_json(col("json_str"), OUTPUT_SCHEMA))

        parsed.persist()

        raw_records = prepare_raw_kafka_records(
            parsed.withColumn("deserialized_payload", col("json_str")),
            epoch_id,
        )
        merge_into_iceberg_table(
            spark,
            raw_records,
            "raw_ad_events",
            ["kafka_topic", "kafka_partition", "kafka_offset"],
            RAW_KAFKA_EVENT_COLUMNS,
            f"raw_ad_events_batch_{epoch_id}",
        )
        logger.info(f"[BATCH {epoch_id}] Upserted {total} source records into Iceberg table 'raw_ad_events'")

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

        good_count = valid_records.count()
        bad_count = total - good_count

        logger.info(f"[BATCH {epoch_id}] Total: {total}, Valid: {good_count}, Corrupted: {bad_count}")

        if good_count > 0:
            try:
                prepared_records = prepare_event_records(valid_records, epoch_id)
                merge_into_iceberg_table(
                    spark,
                    prepared_records,
                    "ad_events",
                    ["event_id"],
                    RAW_EVENT_COLUMNS,
                    f"ad_events_batch_{epoch_id}",
                )
                logger.info(
                    f"[BATCH {epoch_id}] Upserted {prepared_records.count()} records into Iceberg table 'ad_events'"
                )
            except Exception as exc:
                logger.error(f"[BATCH {epoch_id}] Writing to Iceberg 'ad_events' failed: {exc}")
                logger.error(traceback.format_exc())
                raise

        # Free memory at the end of the batch
        parsed.unpersist()

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


def start_campaign_metrics_stream(df, deserialize_udf):
    metrics_df = build_watermarked_campaign_metrics_stream(df, deserialize_udf)
    query = (
        metrics_df.writeStream.queryName("campaign_conversion_metrics_watermarked")
        .foreachBatch(process_metrics_batch)
        .outputMode("update")
        .option("checkpointLocation", METRICS_CHECKPOINT_PATH)
        .trigger(processingTime=DASHBOARD_TRIGGER)
        .start()
    )

    logger.info(f"Campaign metrics stream started with watermark={WATERMARK_DELAY} (query_id={query.id})")
    return query


def run_consumer():
    logger.info("=" * 80)
    logger.info("SIMPLIFIED Kafka Consumer Starting")
    logger.info("=" * 80)

    spark = None
    events_query = None
    metrics_query = None

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
            .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
            .option("kafka.session.timeout.ms", "30000")
            .option("kafka.request.timeout.ms", "40000")
            .option("kafka.max.poll.interval.ms", "300000")
            .option("failOnDataLoss", "false")
            .load()
        )
        logger.info(
            f"Kafka stream connected (trigger={DASHBOARD_TRIGGER}, maxOffsetsPerTrigger={MAX_OFFSETS_PER_TRIGGER})"
        )

        events_query = start_events_stream(kafka_df, deserialize_udf)
        metrics_query = start_campaign_metrics_stream(kafka_df, deserialize_udf)

        logger.info("=" * 80)
        logger.info("PIPELINE ACTIVE:")
        logger.info("  1. All Kafka source records -> Iceberg table 'raw_ad_events'")
        logger.info("  2. Valid parsed events -> Iceberg table 'ad_events'")
        logger.info(f"  3. Watermarked ({WATERMARK_DELAY}) 30-sec campaign metrics -> Iceberg table 'campaign_conversion_metrics'")
        logger.info("=" * 80)

        spark.streams.awaitAnyTermination()

    finally:
        stop_query("events", events_query)
        stop_query("campaign metrics", metrics_query)
        if spark is not None:
            try:
                spark.stop()
            except Exception as exc:
                logger.warning(f"Failed to stop Spark session cleanly: {exc}")


if __name__ == "__main__":
    run_consumer()
