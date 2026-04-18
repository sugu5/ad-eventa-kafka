"""
SIMPLIFIED Kafka -> PySpark Structured Streaming -> PostgreSQL Consumer
Single file - Events + Windowed Aggregation (5-min windows)

Usage:
    python simple_consumer.py
"""
import sys
import traceback
from pathlib import Path
from datetime import datetime

# Path setup
PROJECT_ROOT = Path(__file__).resolve().parent.parent  # Go up from consumer/ to kafka-develop/
sys.path.insert(0, str(PROJECT_ROOT))

from ad_stream_producer.config import Config
from ad_stream_producer.logger import get_logger
from consumer.avro_deserializer import load_avro_schema, make_deserialize_udf

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, current_timestamp, unix_timestamp,
    window, count
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import psycopg2
import pyspark
import urllib.request
import logging

logger = get_logger("simple_consumer")

# Output paths
CHECKPOINT_PATH = str(PROJECT_ROOT / "output" / "checkpoints_final_robust")
CORRUPTED_RECORDS_PATH = str(PROJECT_ROOT / "output" / "corrupted_records")
OUTPUT_PARQUET_PATH = str(PROJECT_ROOT / "output" / "ads_events_parquet")
LATE_EVENTS_PATH = str(PROJECT_ROOT / "output" / "late_events_parquet")
LOCAL_SCHEMA_PATH = PROJECT_ROOT / "schema" / Config.SCHEMA_PATH
AGGREGATION_CHECKPOINT = str(PROJECT_ROOT / "output" / "checkpoints_aggregation")

# Event schema
OUTPUT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("campaign_id", IntegerType(), True),
    StructField("ad_id", IntegerType(), True),
    StructField("device", StringType(), True),
    StructField("country", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("ad_creative_id", StringType(), True),
])


def create_spark_session():
    """Create Spark session."""
    logging.getLogger("org.apache.kafka").setLevel(logging.ERROR)
    logging.getLogger("kafka").setLevel(logging.ERROR)
    
    jars_dir = PROJECT_ROOT / "jars"
    jars_dir.mkdir(exist_ok=True)
    
    # Download JARs if needed
    kafka_jar_path = jars_dir / "kafka-clients-3.4.1.jar"
    if not kafka_jar_path.exists():
        logger.info("Downloading kafka-clients JAR...")
        try:
            urllib.request.urlretrieve(
                "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar",
                kafka_jar_path
            )
        except Exception as e:
            logger.warning(f"Failed to download: {e}")
    
    pg_jar_path = jars_dir / "postgresql-42.6.0.jar"
    if not pg_jar_path.exists():
        logger.info("Downloading postgresql JAR...")
        try:
            urllib.request.urlretrieve(
                "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar",
                pg_jar_path
            )
        except Exception as e:
            logger.warning(f"Failed to download: {e}")
    
    jars = f"{kafka_jar_path},{pg_jar_path}"
    spark_version = pyspark.__version__
    
    spark = (
        SparkSession.builder
        .appName("SimpleKafkaConsumer")
        .config("spark.jars.packages", f"org.apache.spark:spark-sql-kafka-0-10_2.12:{spark_version}")
        .config("spark.jars", jars)
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.streaming.kafka.consumer.cache.enabled", "false")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .getOrCreate()
    )
    
    spark.sparkContext.setLogLevel("ERROR")
    logger.info(f"Spark {spark.version} session created")
    return spark


def process_events_batch(batch_df, epoch_id, deserialize_udf):
    """Process events batch: separate on-time vs late, write to PostgreSQL/Parquet."""
    batch_start = datetime.now()
    
    if batch_df.isEmpty():
        logger.warning(f"[BATCH {epoch_id}] Empty batch")
        return
    
    total = batch_df.count()
    logger.info(f"[BATCH {epoch_id}] Processing {total} records")
    
    try:
        # ...existing code...
        deserialized = batch_df.select(
            deserialize_udf(col("value")).alias("json_str")
        )
        
        # Parse JSON
        parsed = deserialized.withColumn(
            "data", from_json(col("json_str"), OUTPUT_SCHEMA)
        )
        
        good_records = parsed.where("data is not null").select("data.*")
        bad_records = parsed.where("data is null AND json_str is not null").select(
            col("json_str").alias("corrupted_payload")
        )
        
        good_count = good_records.count()
        bad_count = bad_records.count()
        
        # Separate on-time vs late (2 hour threshold)
        if good_count > 0:
            records_with_time = (
                good_records
                .withColumn("event_timestamp", to_timestamp(col("event_time")))
                .withColumn("current_time", current_timestamp())
                .withColumn(
                    "lateness_seconds",
                    unix_timestamp(col("current_time")) - unix_timestamp(col("event_timestamp"))
                )
            )
            
            on_time_records = (
                records_with_time
                .where(col("lateness_seconds") <= 7200)
                .select("event_id", "event_time", "user_id", "campaign_id", "ad_id",
                        "device", "country", "event_type", "ad_creative_id")
            )
            
            late_records = (
                records_with_time
                .where(col("lateness_seconds") > 7200)
                .select("event_id", "event_time", "user_id", "campaign_id", "ad_id",
                        "device", "country", "event_type", "ad_creative_id")
            )
            
            on_time_count = on_time_records.count()
            late_count = late_records.count()
            
            logger.info(f"[BATCH {epoch_id}] On-time: {on_time_count}, Late: {late_count}, Bad: {bad_count}")
            
            # Write on-time to PostgreSQL
            if on_time_count > 0:
                try:
                    converted = (
                        on_time_records
                        .withColumn("event_time", to_timestamp(col("event_time")))
                        .withColumn("campaign_id", col("campaign_id").cast(IntegerType()))
                        .withColumn("ad_id", col("ad_id").cast(IntegerType()))
                    )
                    
                    temp_table = f"temp_ad_events_{epoch_id}_{int(batch_start.timestamp())}"
                    
                    converted.write.jdbc(
                        url=Config.POSTGRES_URL,
                        table=temp_table,
                        mode="overwrite",
                        properties=Config.POSTGRES_PROPERTIES,
                    )
                    
                    # Upsert to main table
                    conn = psycopg2.connect(
                        host="localhost", port="5432", database="postgres",
                        user=Config.POSTGRES_PROPERTIES["user"],
                        password=Config.POSTGRES_PROPERTIES["password"]
                    )
                    conn.autocommit = True
                    
                    with conn.cursor() as cursor:
                        upsert_sql = f"""
                        INSERT INTO {Config.POSTGRES_TABLE} (
                            event_id, event_time, user_id, campaign_id, ad_id,
                            device, country, event_type, ad_creative_id
                        )
                        SELECT event_id, event_time, user_id, campaign_id, ad_id,
                               device, country, event_type, ad_creative_id
                        FROM {temp_table}
                        ON CONFLICT (event_id) DO UPDATE SET
                            event_time = EXCLUDED.event_time,
                            user_id = EXCLUDED.user_id,
                            campaign_id = EXCLUDED.campaign_id,
                            ad_id = EXCLUDED.ad_id,
                            device = EXCLUDED.device,
                            country = EXCLUDED.country,
                            event_type = EXCLUDED.event_type,
                            ad_creative_id = EXCLUDED.ad_creative_id
                        """
                        cursor.execute(upsert_sql)
                        rows = cursor.rowcount
                    
                    with conn.cursor() as cursor:
                        cursor.execute(f"DROP TABLE IF EXISTS {temp_table}")
                    conn.close()
                    
                    logger.info(f"[BATCH {epoch_id}] Wrote {rows} on-time records to PostgreSQL")
                
                except Exception as e:
                    logger.error(f"[BATCH {epoch_id}] PostgreSQL failed: {e}")
                    logger.error(traceback.format_exc())
            
            # Write late to Parquet
            if late_count > 0:
                try:
                    late_formatted = (
                        late_records
                        .withColumn("event_time", to_timestamp(col("event_time")))
                        .withColumn("campaign_id", col("campaign_id").cast(IntegerType()))
                        .withColumn("ad_id", col("ad_id").cast(IntegerType()))
                    )
                    late_formatted.repartition(1).write.mode("append").parquet(LATE_EVENTS_PATH)
                    logger.info(f"[BATCH {epoch_id}] Wrote {late_count} late records to Parquet")
                except Exception as e:
                    logger.error(f"[BATCH {epoch_id}] Late events failed: {e}")
        
        # Write bad records
        if bad_count > 0:
            try:
                bad_records.repartition(1).write.mode("append").format("json").save(
                    CORRUPTED_RECORDS_PATH
                )
                logger.info(f"[BATCH {epoch_id}] Wrote {bad_count} corrupted records")
            except Exception as e:
                logger.error(f"[BATCH {epoch_id}] Corrupted records failed: {e}")
        
        duration = (datetime.now() - batch_start).total_seconds()
        logger.info(f"[BATCH {epoch_id}] Done in {duration:.2f}s")
    
    except Exception as e:
        logger.error(f"[BATCH {epoch_id}] Batch processing failed: {e}")
        logger.error(traceback.format_exc())


def write_aggregation_batch(batch_df, batch_id):
    """Write aggregated statistics to PostgreSQL."""
    batch_start = datetime.now()
    
    try:
        # Collect rows first - NO isEmpty() call!
        rows = batch_df.collect()
        
        if not rows:
            logger.debug(f"[AGG BATCH {batch_id}] Empty batch")
            return

        record_count = len(rows)
        logger.info(f"[AGG BATCH {batch_id}] Processing {record_count} aggregation records")

        # ...existing code...

        conn = psycopg2.connect(
            host="localhost", port="5432", database="postgres",
            user=Config.POSTGRES_PROPERTIES["user"],
            password=Config.POSTGRES_PROPERTIES["password"]
        )
        conn.autocommit = True
        
        with conn.cursor() as cursor:
            # Create schema and table
            cursor.execute("CREATE SCHEMA IF NOT EXISTS event_schema")
            
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS event_schema.event_type_device_stats (
                id SERIAL PRIMARY KEY,
                event_type VARCHAR(50),
                device VARCHAR(50),
                event_count INT,
                window_start TIMESTAMP,
                window_end TIMESTAMP,
                aggregation_time TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(event_type, device, window_start, window_end)
            )
            """
            cursor.execute(create_table_sql)
            
            # Create indexes
            cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_window_time 
            ON event_schema.event_type_device_stats(window_end DESC)
            """)
            cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_event_device 
            ON event_schema.event_type_device_stats(event_type, device)
            """)
            
            # Upsert records
            written_count = 0
            for row in rows:
                event_type = row["event_type"]
                device = row["device"]
                event_count = row["event_count"]
                window_start = row["window_start"]
                window_end = row["window_end"]
                aggregation_time = datetime.now()
                
                upsert_sql = """
                INSERT INTO event_schema.event_type_device_stats 
                (event_type, device, event_count, aggregation_time, window_start, window_end)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_type, device, window_start, window_end) DO UPDATE SET
                    event_count = EXCLUDED.event_count,
                    aggregation_time = EXCLUDED.aggregation_time
                """
                cursor.execute(upsert_sql, (event_type, device, event_count, 
                                           aggregation_time, window_start, window_end))
                written_count += 1
        
        conn.close()
        
        duration = (datetime.now() - batch_start).total_seconds()
        logger.info(f"[AGG BATCH {batch_id}] Wrote {written_count} records in {duration:.2f}s")
    
    except Exception as e:
        logger.error(f"[AGG BATCH {batch_id}] Failed: {e}")
        logger.error(traceback.format_exc())


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
        df.writeStream
        .queryName("ad_events_ingestion")
        .foreachBatch(lambda batch_df, epoch_id: process_events_batch(batch_df, epoch_id, deserialize_udf))
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .start()
    )

    logger.info(f"Events stream started (query_id={query.id})")
    return query


def start_aggregation_stream(df, deserialize_udf):
    agg_df = (
        df.select(deserialize_udf(col("value")).alias("json_str"))
        .withColumn("data", from_json(col("json_str"), OUTPUT_SCHEMA))
        .select("data.*")
        .filter(col("event_time").isNotNull())
    )

    windowed_df = (
        agg_df
        .select(
            col("event_time"),
            col("event_type"),
            col("device"),
            to_timestamp(col("event_time")).alias("event_timestamp")
        )
        .withWatermark("event_timestamp", "2 minutes")
    )

    aggregated_df = (
        windowed_df
        .groupBy(
            window(col("event_timestamp"), "5 minutes"),
            col("event_type"),
            col("device")
        )
        .agg(count(col("event_timestamp")).alias("event_count"))
        .select(
            col("window").getField("start").alias("window_start"),
            col("window").getField("end").alias("window_end"),
            col("event_type"),
            col("device"),
            col("event_count")
        )
    )

    aggregation_query = (
        aggregated_df.writeStream
        .queryName("ad_event_type_device_aggregation")
        .foreachBatch(lambda batch_df, batch_id: write_aggregation_batch(batch_df, batch_id))
        .outputMode("update")
        .option("checkpointLocation", AGGREGATION_CHECKPOINT)
        .start()
    )

    logger.info(f"Aggregation stream started (query_id={aggregation_query.id})")
    return aggregation_query


def run_consumer():
    logger.info("=" * 80)
    logger.info("SIMPLIFIED Kafka Consumer Starting")
    logger.info("=" * 80)

    spark = None
    events_query = None
    aggregation_query = None

    try:
        spark = create_spark_session()

        avro_schema, source = load_avro_schema(
            Config.SCHEMA_REGISTRY_URL, Config.TOPIC, LOCAL_SCHEMA_PATH
        )
        logger.info(f"Loaded Avro schema from {source}")
        schema_broadcast = spark.sparkContext.broadcast(avro_schema)
        deserialize_udf = make_deserialize_udf(schema_broadcast)

        bootstrap_csv = ",".join(Config.KAFKA_BOOTSTRAP_SERVERS)
        logger.info(f"Connecting to Kafka: {bootstrap_csv}")

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
        logger.info("Kafka stream connected")

        events_query = start_events_stream(df, deserialize_udf)
        aggregation_query = start_aggregation_stream(df, deserialize_udf)

        logger.info("=" * 80)
        logger.info("DUAL STREAMS ACTIVE:")
        logger.info("  1. Events -> PostgreSQL")
        logger.info("  2. 5-min Aggregations -> PostgreSQL")
        logger.info("=" * 80)

        try:
            spark.streams.awaitAnyTermination()
        except Exception:
            logger.error("One of the streaming queries terminated with an error.")
            log_query_failure("Events", events_query)
            log_query_failure("Aggregation", aggregation_query)

            events_failed = query_exception(events_query) is not None
            aggregation_failed = query_exception(aggregation_query) is not None

            if aggregation_failed and not events_failed and events_query and events_query.isActive:
                logger.info("Aggregation stream failed; continuing with events-only stream...")
                spark.streams.resetTerminated()
                events_query.awaitTermination()
            else:
                raise

    finally:
        stop_query("aggregation", aggregation_query)
        stop_query("events", events_query)
        if spark is not None:
            try:
                spark.stop()
            except Exception as exc:
                logger.warning(f"Failed to stop Spark session cleanly: {exc}")



if __name__ == "__main__":
    run_consumer()
