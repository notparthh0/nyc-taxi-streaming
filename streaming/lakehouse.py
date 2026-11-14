"""
The stream writes into the SAME Apache Iceberg lakehouse as the batch pipeline
(nyc-taxi-elt-pipeline, catalog `lake`, warehouse ../nyc-taxi-aws/data/lake/iceberg):

  lake.nyc.trips_cleaned      written by the batch job; the stream reads it (via DuckDB)
                              to build seasonal baselines
  lake.nyc.zone_windows_live  5-minute zone counts, written by this job
  lake.nyc.demand_spikes      detected spikes, written by this job

Writes use MERGE keyed on (window_start, zone). A micro-batch that Spark replays
after a crash MERGEs the same rows again and changes nothing, so the Iceberg
tables end up exactly-once too.
"""

import os
from pathlib import Path

from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parents[1]
ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1"
DEFAULT_WAREHOUSE = ROOT.parent / "nyc-taxi-aws" / "data" / "lake" / "iceberg"
WINDOWS_TABLE = "lake.nyc.zone_windows_live"
SPIKES_TABLE = "lake.nyc.demand_spikes"
TRIPS_TABLE_DIR = ("nyc", "trips_cleaned")


def warehouse_path() -> Path:
    return Path(os.environ.get("ICEBERG_WAREHOUSE", DEFAULT_WAREHOUSE)).resolve()


def with_iceberg(builder, warehouse: Path = None):
    return (builder
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.lake.type", "hadoop")
            .config("spark.sql.catalog.lake.warehouse", (warehouse or warehouse_path()).as_uri()))


def ensure_tables(spark: SparkSession) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.nyc")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {WINDOWS_TABLE} (
        window_start timestamp_ntz, zone int, trips int, avg_fare double,
        last_produced_ms bigint, processed_at_ms bigint, batch_id bigint)
        USING iceberg PARTITIONED BY (days(window_start))""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {SPIKES_TABLE} (
        window_start timestamp_ntz, zone int, trips int, baseline_mean double, baseline_std double,
        threshold double, detector string, explanation string, detected_at_ms bigint)
        USING iceberg PARTITIONED BY (days(window_start))""")


def merge_rows(spark: SparkSession, table: str, rows, schema: str) -> int:
    """Upsert rows into an Iceberg table keyed on (window_start, zone)."""
    if not rows:
        return 0
    spark.createDataFrame(rows, schema).createOrReplaceTempView("_incoming")
    spark.sql(f"""MERGE INTO {table} t
                  USING (SELECT * FROM _incoming) s
                  ON t.window_start = s.window_start AND t.zone = s.zone
                  WHEN MATCHED THEN UPDATE SET *
                  WHEN NOT MATCHED THEN INSERT *""")
    return len(rows)
