"""
Spark Structured Streaming: Kafka -> 5-minute event-time windows per zone -> spike detection.

    python streaming/spark_stream.py                     # run until Ctrl+C
    python streaming/spark_stream.py --until-idle 30     # stop after 30 s with no new input (experiments)

How it handles the hard parts of streaming:
  * Event time, not arrival time: windows use each trip's pickup time.
  * Late data: a 10-minute watermark. A trip arriving up to 10 minutes (event time)
    late still lands in its own window; later than that it is dropped, and Spark
    reports how many were dropped (logged to data/stream_progress.jsonl).
  * Duplicates: the producer may resend a message (at-least-once). Events are
    deduplicated on event_id within the watermark.
  * Each window is emitted once, when it is final (append mode).
  * Restarts: Kafka offsets live in the checkpoint; results are written with the
    micro-batch id in one SQLite transaction, so a replayed batch is never
    applied twice (see streaming/store.py), and MERGEd into the Iceberg
    lakehouse keyed on (window_start, zone), so a replay changes nothing there
    either. Net effect: exactly-once results in both sinks.
  * Detection: by default each zone is compared with its seasonal profile (same
    weekday and 5-minute slot over the previous 4 weeks), read from the batch
    pipeline's Iceberg table; --detector rolling uses the v1 previous-hour rule.
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import (DoubleType, IntegerType, LongType, StringType,  # noqa: E402
                               StructField, StructType)

from streaming.lakehouse import (ICEBERG_PACKAGE, SPIKES_TABLE, WINDOWS_TABLE,  # noqa: E402
                                  ensure_tables, merge_rows, warehouse_path, with_iceberg)
from streaming.processor import process_batch, write_dashboard_snapshots  # noqa: E402
from streaming.seasonal import RollingDetector, SeasonalDetector, iceberg_source  # noqa: E402
from streaming.store import TS_FORMAT, Store  # noqa: E402

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
DATA_DIR = ROOT / "data"

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("tpep_pickup_datetime", StringType()),
    StructField("PULocationID", IntegerType()),
    StructField("fare_amount", DoubleType()),
    StructField("trip_distance", DoubleType()),
    StructField("passenger_count", IntegerType()),
    StructField("produced_at_ms", LongType()),
])


def build_spark(app_name: str = "nyc-taxi-stream", with_kafka: bool = True, with_lakehouse: bool = True,
                warehouse: Path = None) -> SparkSession:
    builder = (SparkSession.builder.appName(app_name).master("local[*]")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.shuffle.partitions", "4"))
    packages = ([KAFKA_PACKAGE] if with_kafka else []) + ([ICEBERG_PACKAGE] if with_lakehouse else [])
    if packages:
        builder = builder.config("spark.jars.packages", ",".join(packages))
    if with_lakehouse:
        builder = with_iceberg(builder, warehouse)
    return builder.getOrCreate()


def sync_to_iceberg(spark: SparkSession, store: Store, window_starts) -> dict:
    """MERGE this batch's windows and spikes from SQLite into the Iceberg lakehouse.
    Runs even when SQLite skipped a replayed batch, so a crash between the two sinks heals itself."""
    if not window_starts:
        return {"iceberg_windows": 0, "iceberg_spikes": 0}
    marks = ",".join("?" * len(window_starts))
    as_ts = lambda s: datetime.strptime(s, TS_FORMAT)  # noqa: E731
    windows = [(as_ts(w), z, t, f, lp, pa, b) for w, z, t, f, lp, pa, b in store.conn.execute(
        f"SELECT window_start, zone, trips, avg_fare, last_produced_ms, processed_at_ms, batch_id "
        f"FROM zone_windows WHERE window_start IN ({marks})", list(window_starts))]
    spikes = [(as_ts(w), z, t, m, s, th, d, e, at) for w, z, t, m, s, th, d, e, at in store.conn.execute(
        f"SELECT window_start, zone, trips, baseline_mean, baseline_std, threshold, detector, explanation, "
        f"detected_at_ms FROM anomalies WHERE window_start IN ({marks})", list(window_starts))]
    n_windows = merge_rows(spark, WINDOWS_TABLE, windows,
                           "window_start timestamp_ntz, zone int, trips int, avg_fare double, "
                           "last_produced_ms bigint, processed_at_ms bigint, batch_id bigint")
    n_spikes = merge_rows(spark, SPIKES_TABLE, spikes,
                          "window_start timestamp_ntz, zone int, trips int, baseline_mean double, "
                          "baseline_std double, threshold double, detector string, explanation string, "
                          "detected_at_ms bigint")
    return {"iceberg_windows": n_windows, "iceberg_spikes": n_spikes}


def windowed_counts(kafka_df: DataFrame, watermark: str = "10 minutes") -> DataFrame:
    """Kafka rows (binary `value`) -> one row per (5-minute window, zone). Works on streams and static tables."""
    events = (kafka_df
              .select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"))
              .select("e.*")
              .withColumn("event_time", F.to_timestamp("tpep_pickup_datetime"))
              .where("event_time IS NOT NULL AND PULocationID IS NOT NULL AND event_id IS NOT NULL"))
    watermarked = events.withWatermark("event_time", watermark)
    # Streams: keep dedup state only as long as the watermark needs it.
    # Static tables (unit tests): Spark only allows the plain version.
    deduped = (watermarked.dropDuplicatesWithinWatermark(["event_id"]) if kafka_df.isStreaming
               else watermarked.dropDuplicates(["event_id"]))
    return (deduped
            .groupBy(F.window("event_time", "5 minutes"), F.col("PULocationID").alias("zone"))
            .agg(F.count(F.lit(1)).alias("trips"),
                 F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
                 F.max("produced_at_ms").alias("last_produced_ms"))
            .select(F.date_format("window.start", "yyyy-MM-dd HH:mm:ss").alias("window_start"),
                    "zone", "trips", "avg_fare", "last_produced_ms"))


def make_batch_handler(store: Store, data_dir: Path, k: float, min_trips: int, max_llm_calls: int,
                       detector=None, lakehouse: bool = True):
    def handle(batch_df: DataFrame, batch_id: int) -> None:
        rows = [row.asDict() for row in batch_df.collect()]
        result = process_batch(store, rows, batch_id, k=k, min_trips=min_trips, max_llm_calls=max_llm_calls,
                               detector=detector)
        if lakehouse:
            result.update(sync_to_iceberg(batch_df.sparkSession, store, sorted({r["window_start"] for r in rows})))
        write_dashboard_snapshots(store, data_dir)
        if rows or result.get("skipped"):
            print(f"[batch {batch_id}] {json.dumps(result)}", flush=True)
    return handle


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "nyc-taxi-stream"))
    parser.add_argument("--data-dir", type=Path, default=DATA_DIR)
    parser.add_argument("--watermark", default="10 minutes")
    parser.add_argument("--trigger-seconds", type=int, default=2)
    parser.add_argument("--detector", choices=["seasonal", "rolling"], default="seasonal")
    parser.add_argument("--k", type=float, default=None,
                        help="spike = mean + k * spread (default 3 seasonal, 2 rolling)")
    parser.add_argument("--min-trips", type=int, default=5)
    parser.add_argument("--no-lakehouse", action="store_true", help="skip the Iceberg sink (SQLite only)")
    parser.add_argument("--max-llm-calls", type=int, default=3, help="Gemini calls per micro-batch")
    parser.add_argument("--no-llm", action="store_true", help="never call Gemini")
    parser.add_argument("--until-idle", type=int, default=0,
                        help="stop after this many seconds without new input (0 = run forever)")
    args = parser.parse_args(argv)

    try:
        from dotenv import load_dotenv
        load_dotenv(ROOT / ".env")
    except ImportError:
        pass
    if args.no_llm:
        os.environ["GEMINI_API_KEY"] = ""
        args.max_llm_calls = 0

    data_dir = args.data_dir
    store = Store(data_dir / "stream.sqlite")
    lakehouse = not args.no_lakehouse
    spark = build_spark(with_lakehouse=lakehouse)
    spark.sparkContext.setLogLevel("ERROR")
    if lakehouse:
        ensure_tables(spark)
    if args.detector == "seasonal":
        k = 3.0 if args.k is None else args.k
        detector = SeasonalDetector(iceberg_source(warehouse_path()), k=k, min_trips=args.min_trips)
    else:
        k = 2.0 if args.k is None else args.k
        detector = RollingDetector(k=k, min_trips=args.min_trips)

    kafka_df = (spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", args.bootstrap)
                .option("subscribe", args.topic)
                .option("startingOffsets", "earliest")
                .option("failOnDataLoss", "false")
                .load())
    query = (windowed_counts(kafka_df, args.watermark).writeStream
             .outputMode("append")
             .foreachBatch(make_batch_handler(store, data_dir, k, args.min_trips, args.max_llm_calls,
                                              detector=detector, lakehouse=lakehouse))
             .option("checkpointLocation", str(data_dir / "checkpoints" / "zone_windows"))
             .trigger(processingTime=f"{args.trigger_seconds} seconds")
             .start())
    (data_dir / "stream_ready").write_text(str(time.time()))
    print(f"streaming from {args.topic} @ {args.bootstrap} (watermark {args.watermark}, "
          f"detector {detector.name}, k={k}, iceberg={'on' if lakehouse else 'off'})", flush=True)

    progress_log = data_dir / "stream_progress.jsonl"
    seen, idle_since = set(), time.time()
    try:
        while query.isActive:
            time.sleep(2)
            for progress in query.recentProgress:
                if progress["batchId"] in seen:
                    continue
                seen.add(progress["batchId"])
                if progress.get("numInputRows", 0) > 0:
                    idle_since = time.time()
                with progress_log.open("a") as f:
                    f.write(json.dumps({
                        "batch_id": progress["batchId"],
                        "input_rows": progress.get("numInputRows"),
                        "input_rows_per_s": progress.get("inputRowsPerSecond"),
                        "processed_rows_per_s": progress.get("processedRowsPerSecond"),
                        "trigger_ms": progress.get("durationMs", {}).get("triggerExecution"),
                        "watermark": progress.get("eventTime", {}).get("watermark"),
                        "rows_dropped_by_watermark": sum(op.get("numRowsDroppedByWatermark", 0)
                                                         for op in progress.get("stateOperators", [])),
                    }) + "\n")
            if args.until_idle and time.time() - idle_since > args.until_idle:
                print(f"no new input for {args.until_idle}s, stopping", flush=True)
                query.stop()
    except KeyboardInterrupt:
        query.stop()
    finally:
        (data_dir / "stream_ready").unlink(missing_ok=True)
        store.close()
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
