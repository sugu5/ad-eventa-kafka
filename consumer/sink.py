"""
Sink logic — processes each Spark Structured-Streaming micro-batch.
Good records  → PostgreSQL (with Parquet fallback)
Bad records   → local JSON files
"""
import traceback
from datetime import datetime

from pyspark.sql.functions import col, from_json, to_timestamp
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
    logger,
):
    """
    Returns a `foreachBatch` callback closed over the supplied configuration.
    """

    def process_batch(batch_df, epoch_id):
        batch_start = datetime.now()
        logger.info(f"[BATCH {epoch_id}] Starting — {batch_start}")

        if batch_df.isEmpty():
            logger.warning(f"[BATCH {epoch_id}] Empty batch, skipping")
            return

        total = batch_df.count()
        logger.info(f"[BATCH {epoch_id}] Records in batch: {total}")

        # 1. Deserialize Avro → JSON string
        deserialized_df = batch_df.select(
            deserialize_udf(col("value")).alias("json_str")
        )
        deserialized_df.cache()

        # 2. Parse JSON → typed columns
        parsed_df = deserialized_df.withColumn(
            "data", from_json(col("json_str"), OUTPUT_SCHEMA)
        )
        parsed_df.cache()

        good_records = parsed_df.where("data is not null").select("data.*")
        bad_records = parsed_df.where(
            "data is null AND json_str is not null"
        ).select(col("json_str").alias("corrupted_payload"))

        good_count = good_records.count()
        bad_count = bad_records.count()
        logger.info(
            f"[BATCH {epoch_id}] Parsed: {good_count} good, {bad_count} bad"
        )

        # 3. Type-cast & write good records to PostgreSQL with UPSERT
        if good_count > 0:
            converted = (
                good_records
                .withColumn("event_time", to_timestamp(col("event_time")))
                .withColumn("campaign_id", col("campaign_id").cast(IntegerType()))
                .withColumn("ad_id", col("ad_id").cast(IntegerType()))
            )

            # Generate unique temp table name for this batch
            temp_table = f"temp_ad_events_{epoch_id}_{int(batch_start.timestamp())}"

            try:
                # Step 1: Create temp table and insert batch data
                logger.info(f"[BATCH {epoch_id}] Creating temp table: {temp_table}")
                converted.write.jdbc(
                    url=postgres_url,
                    table=temp_table,
                    mode="overwrite",  # Creates the temp table
                    properties=postgres_properties,
                )

                # Step 2: Execute UPSERT from temp table to main table
                logger.info(f"[BATCH {epoch_id}] Executing UPSERT from {temp_table} to {postgres_table}")

                # Get JDBC connection and execute MERGE
                from pyspark.sql import SparkSession
                spark = SparkSession.getActiveSession()

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

                # Execute the UPSERT
                spark.sql(f"""
                SELECT 1
                """).collect()  # Dummy query to establish connection

                # Use JDBC to execute the raw SQL
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
                    cursor.execute(upsert_sql)
                    rows_affected = cursor.rowcount
                conn.close()

                # Step 3: Clean up temp table
                cleanup_sql = f"DROP TABLE IF EXISTS {temp_table}"
                conn = psycopg2.connect(
                    host="localhost",
                    port="5432",
                    database="postgres",
                    user=postgres_properties["user"],
                    password=postgres_properties["password"]
                )
                conn.autocommit = True
                with conn.cursor() as cursor:
                    cursor.execute(cleanup_sql)
                conn.close()

                dur = (datetime.now() - batch_start).total_seconds()
                logger.info(
                    f"[BATCH {epoch_id}] ✓ UPSERT completed: {rows_affected} rows affected "
                    f"in {dur:.2f}s"
                )

            except Exception as e:
                logger.error(
                    f"[BATCH {epoch_id}] ✗ PostgreSQL UPSERT failed: {e}"
                )
                logger.error(traceback.format_exc())
                # Fallback to Parquet
                try:
                    good_records.write.mode("append").parquet(output_parquet_path)
                    logger.info(
                        f"[BATCH {epoch_id}] ✓ Parquet fallback succeeded"
                    )
                except Exception as pe:
                    logger.error(
                        f"[BATCH {epoch_id}] ✗ Parquet fallback failed: {pe}"
                    )

        # 4. Persist corrupted records
        if bad_count > 0:
            try:
                bad_records.repartition(1).write.mode("append").format(
                    "json"
                ).save(corrupted_records_path)
                logger.info(
                    f"[BATCH {epoch_id}] Wrote {bad_count} corrupted records"
                )
            except Exception as e:
                logger.error(
                    f"[BATCH {epoch_id}] ✗ Corrupted-records write failed: {e}"
                )

        duration = (datetime.now() - batch_start).total_seconds()
        logger.info(f"[BATCH {epoch_id}] Done in {duration:.2f}s")

    return process_batch

