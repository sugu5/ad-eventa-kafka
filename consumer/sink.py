"""
Sink logic — processes Spark Structured Streaming micro-batches.

LATENESS CHECK STRATEGY (No watermark filtering):
- ALL records from Kafka reach sink.py
- Lateness calculated: current_time - event_time
- Manual separation based on 2-hour threshold

OUTPUT LOGIC:
On-time records (≤ 2 hours)  → PostgreSQL
Late records (> 2 hours)      → Parquet file  
Bad records                   → JSON files

NOTE: Watermark is NOT used for filtering records.
Watermark is only needed if aggregations/windowing are added later.
Example: window().groupBy().agg() would then use withWatermark("event_time", "2 hours")
"""
import traceback
from datetime import datetime

from pyspark.sql.functions import col, from_json, to_timestamp, current_timestamp, unix_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


# Schema that the deserialized JSON is parsed into
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


def make_process_batch(
    deserialize_udf,
    postgres_url: str,
    postgres_properties: dict,
    postgres_table: str,
    output_parquet_path: str,
    corrupted_records_path: str,
    late_events_path: str,
    logger,
):
    """
    Returns a `foreachBatch` callback for processing micro-batches.

    Separates records by age (2-hour threshold):
    - On-time (≤ 2h)  → PostgreSQL
    - Late (> 2h)     → Parquet (late_events_path)
    - Bad             → JSON

    Args:
        deserialize_udf: UDF for deserializing Avro to JSON
        postgres_url: PostgreSQL JDBC URL
        postgres_properties: PostgreSQL connection properties
        postgres_table: Target PostgreSQL table
        output_parquet_path: Parquet fallback path
        corrupted_records_path: Path for corrupted records
        late_events_path: Path for late-arriving events (> 2 hours)
        logger: Logger instance
    """

    def process_batch(batch_df, epoch_id):
        batch_start = datetime.now()
        
        if batch_df.isEmpty():
            logger.warning(f"[BATCH {epoch_id}] Empty batch, skipping")
            return

        total = batch_df.count()
        logger.info(f"[BATCH {epoch_id}] Processing {total} records")

        # 1. Deserialize Avro → JSON
        deserialized_df = batch_df.select(
            deserialize_udf(col("value")).alias("json_str")
        ).cache()

        # 2. Parse JSON
        parsed_df = deserialized_df.withColumn(
            "data", from_json(col("json_str"), OUTPUT_SCHEMA)
        ).cache()

        # Separate good and bad records
        good_records = parsed_df.where("data is not null").select("data.*")
        bad_records = parsed_df.where("data is null AND json_str is not null").select(
            col("json_str").alias("corrupted_payload")
        )

        good_count = good_records.count()
        bad_count = bad_records.count()

        # 3. Separate on-time and late records (2-hour threshold)
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

            # 2 hours = 7200 seconds
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

            # 4. Write ON-TIME records to PostgreSQL
            if on_time_count > 0:
                converted = (
                    on_time_records
                    .withColumn("event_time", to_timestamp(col("event_time")))
                    .withColumn("campaign_id", col("campaign_id").cast(IntegerType()))
                    .withColumn("ad_id", col("ad_id").cast(IntegerType()))
                )

                temp_table = f"temp_ad_events_{epoch_id}_{int(batch_start.timestamp())}"

                try:
                    converted.write.jdbc(
                        url=postgres_url,
                        table=temp_table,
                        mode="overwrite",
                        properties=postgres_properties,
                    )

                    import psycopg2
                    conn = psycopg2.connect(
                        host="localhost",
                        port="5432",
                        database="postgres",
                        user=postgres_properties["user"],
                        password=postgres_properties["password"]
                    )
                    conn.autocommit = True

                    with conn.cursor() as cursor:
                        upsert_sql = f"""
                        INSERT INTO {postgres_table} (
                            event_id, event_time, user_id, campaign_id, ad_id,
                            device, country, event_type, ad_creative_id
                        )
                        SELECT
                            event_id, event_time, user_id, campaign_id, ad_id,
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

                    logger.info(f"[BATCH {epoch_id}] ✓ Wrote {rows} on-time records to PostgreSQL")

                except Exception as e:
                    logger.error(f"[BATCH {epoch_id}] ✗ PostgreSQL write failed: {e}")
                    logger.error(traceback.format_exc())
                    try:
                        on_time_records.write.mode("append").parquet(output_parquet_path)
                    except Exception as pe:
                        logger.error(f"[BATCH {epoch_id}] ✗ Parquet fallback failed: {pe}")

            # 5. Write LATE records to Parquet
            if late_count > 0:
                try:
                    late_formatted = (
                        late_records
                        .withColumn("event_time", to_timestamp(col("event_time")))
                        .withColumn("campaign_id", col("campaign_id").cast(IntegerType()))
                        .withColumn("ad_id", col("ad_id").cast(IntegerType()))
                    )

                    late_formatted.repartition(1).write.mode("append").parquet(late_events_path)
                    logger.info(f"[BATCH {epoch_id}] ✓ Wrote {late_count} late records to {late_events_path}")
                except Exception as e:
                    logger.error(f"[BATCH {epoch_id}] ✗ Late events write failed: {e}")
                    logger.error(traceback.format_exc())

        # 6. Write corrupted records
        if bad_count > 0:
            try:
                bad_records.repartition(1).write.mode("append").format("json").save(
                    corrupted_records_path
                )
                logger.info(f"[BATCH {epoch_id}] ✓ Wrote {bad_count} corrupted records")
            except Exception as e:
                logger.error(f"[BATCH {epoch_id}] ✗ Corrupted records write failed: {e}")

        duration = (datetime.now() - batch_start).total_seconds()
        logger.info(f"[BATCH {epoch_id}] ✓ Done in {duration:.2f}s")

    return process_batch

