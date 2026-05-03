"""
Daily batch reconciliation for campaign conversion metrics.

Usage:
    python -m consumer.reconciliation_job
    python -m consumer.reconciliation_job --date 2026-05-02
"""
import argparse
import sys
import traceback
from datetime import date, datetime, timedelta
from pathlib import Path

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
from pyspark.sql.types import IntegerType

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from ad_stream_producer.logger import get_logger
from consumer.simple_consumer import (
    CAMPAIGN_METRIC_COLUMNS,
    CLICK_EVENT_TYPES,
    CONVERSION_EVENT_TYPES,
    DASHBOARD_WINDOW,
    OUTPUT_SCHEMA,
    RAW_EVENT_COLUMNS,
    create_spark_session,
    ensure_iceberg_tables,
    merge_into_iceberg_table,
)

logger = get_logger("reconciliation_job")


def default_reconciliation_date():
    """Reconcile the last complete calendar day by default."""
    return date.today() - timedelta(days=1)


def parse_args():
    parser = argparse.ArgumentParser(description="Reconcile daily campaign conversion metrics from raw_ad_events.")
    parser.add_argument(
        "--date",
        dest="target_date",
        default=default_reconciliation_date().isoformat(),
        help="Event date to reconcile in YYYY-MM-DD format. Defaults to yesterday.",
    )
    return parser.parse_args()


def parse_target_date(target_date):
    try:
        return datetime.strptime(target_date, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"Invalid --date '{target_date}'. Expected YYYY-MM-DD.") from exc


def build_curated_events_from_raw(spark, target_date):
    """Rebuild valid curated events for one event_date from the raw source-of-truth table."""
    raw_df = (
        spark.table("raw_ad_events")
        .where(col("parse_status") == lit("parsed"))
        .select(
            "deserialized_payload",
            col("ingestion_batch_id").alias("source_ingestion_batch_id"),
            col("ingested_at").alias("source_ingested_at"),
        )
        .withColumn("data", from_json(col("deserialized_payload"), OUTPUT_SCHEMA))
        .where("data is not null")
        .select("data.*", "source_ingestion_batch_id", "source_ingested_at")
        .withColumn("event_timestamp", to_timestamp(col("event_time")))
        .withColumn("event_date", to_date(col("event_timestamp")))
        .where(col("event_date") == lit(target_date.isoformat()).cast("date"))
        .filter(
            col("event_id").isNotNull()
            & col("campaign_id").isNotNull()
            & col("event_type").isNotNull()
            & col("event_timestamp").isNotNull()
        )
    )

    return (
        raw_df.dropDuplicates(["event_id"])
        .withColumn("event_time", to_timestamp(col("event_time")))
        .withColumn("campaign_id", col("campaign_id").cast(IntegerType()))
        .withColumn("ad_id", col("ad_id").cast(IntegerType()))
        .withColumn("ingestion_batch_id", col("source_ingestion_batch_id"))
        .withColumn("ingested_at", col("source_ingested_at"))
        .select(*RAW_EVENT_COLUMNS)
    )


def build_campaign_metrics_for_day(events_df):
    metrics_source_df = (
        events_df.where(col("event_type").isin(*(CLICK_EVENT_TYPES + CONVERSION_EVENT_TYPES)))
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


def replace_campaign_metrics_for_day(spark, metrics_df, target_date):
    spark.sql(f"DELETE FROM campaign_conversion_metrics WHERE window_date = DATE '{target_date.isoformat()}'")
    metrics_count = metrics_df.count()

    if metrics_count == 0:
        return 0

    metrics_df.select(*CAMPAIGN_METRIC_COLUMNS).writeTo("campaign_conversion_metrics").append()
    spark.catalog.refreshTable("campaign_conversion_metrics")
    return metrics_count


def reconcile_day(spark, target_date):
    logger.info(f"Starting reconciliation for event_date={target_date.isoformat()}")
    ensure_iceberg_tables(spark)

    curated_events = build_curated_events_from_raw(spark, target_date).persist()
    event_count = curated_events.count()

    if event_count > 0:
        merge_into_iceberg_table(
            spark,
            curated_events,
            "ad_events",
            ["event_id"],
            RAW_EVENT_COLUMNS,
            f"ad_events_reconciliation_{target_date.strftime('%Y%m%d')}",
        )
        logger.info(f"Reconciled {event_count} curated events into 'ad_events'")
    else:
        logger.info(f"No valid source events found for event_date={target_date.isoformat()}")

    metrics_df = build_campaign_metrics_for_day(curated_events)
    metrics_count = replace_campaign_metrics_for_day(spark, metrics_df, target_date)
    logger.info(f"Replaced {metrics_count} campaign metric windows for event_date={target_date.isoformat()}")

    curated_events.unpersist()
    return event_count, metrics_count


def main():
    args = parse_args()
    target_date = parse_target_date(args.target_date)
    spark = None

    try:
        spark = create_spark_session()
        events, metrics = reconcile_day(spark, target_date)
        logger.info(
            f"Reconciliation complete for event_date={target_date.isoformat()}: "
            f"events={events}, metric_windows={metrics}"
        )
    except Exception as exc:
        logger.error(f"Reconciliation failed: {exc}")
        logger.error(traceback.format_exc())
        raise
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
