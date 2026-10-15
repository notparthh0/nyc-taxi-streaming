import json
import os
import statistics
from collections import defaultdict, deque
from datetime import datetime, timedelta
from dotenv import load_dotenv
from kafka import KafkaConsumer
from google import genai

load_dotenv()

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "nyc-taxi-stream")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
ANOMALY_FEED_CAP = 100  # Maximum anomalies kept in memory and written to JSON

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
os.makedirs(DATA_DIR, exist_ok=True)
ANOMALIES_FILE = os.path.join(DATA_DIR, "anomalies.json")
METRICS_FILE = os.path.join(DATA_DIR, "metrics.json")

# Initialize google-genai client (new SDK)
gemini_client = None
if GEMINI_API_KEY and GEMINI_API_KEY not in ("", "your_key_here"):
    gemini_client = genai.Client(api_key=GEMINI_API_KEY)


def floor_to_5min(dt: datetime) -> datetime:
    """Floors a datetime object to the nearest 5-minute boundary."""
    minute = (dt.minute // 5) * 5
    return dt.replace(minute=minute, second=0, microsecond=0)


def explain_anomaly(zone_id: int, window_str: str, count: int, mean: float, std: float) -> str:
    """
    Generates a context note for the anomaly.
    Calls Gemini if a client is configured; otherwise returns a factual fallback.
    Described as 'generated context', not a verified root cause.
    """
    if not gemini_client:
        return (
            f"Zone {zone_id} recorded {count} trips in {window_str}, "
            f"against a trailing mean of {mean:.1f} (std {std:.1f}). "
            f"Set GEMINI_API_KEY for AI-generated context."
        )

    prompt = (
        f"NYC Yellow Taxi telemetry — anomalous demand detected.\n"
        f"Zone ID (PULocationID): {zone_id}\n"
        f"5-minute window: {window_str}\n"
        f"Observed pickups: {count}\n"
        f"Trailing 12-window mean: {mean:.2f}, std: {std:.2f}\n\n"
        f"In 1-2 sentences, describe the observed magnitude relative to baseline and suggest "
        f"plausible urban traffic factors for this time window. "
        f"Distinguish hypotheses from confirmed facts."
    )
    try:
        response = gemini_client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt,
        )
        return response.text.strip()
    except Exception as exc:
        return (
            f"Zone {zone_id}: {count} trips vs {mean:.1f} mean. "
            f"(Gemini unavailable: {exc})"
        )


def atomic_write_json(filepath: str, data: object) -> None:
    """Atomically writes JSON by writing to a temp file first."""
    temp_path = f"{filepath}.tmp"
    with open(temp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    os.replace(temp_path, filepath)


def flush_window(
    window_start: datetime,
    window_counts: dict,
    zone_history: dict,
    anomalies_list: list,
    total_records: int,
) -> None:
    """Evaluates a completed window: detects anomalies, updates baselines, writes JSON."""
    window_end = window_start + timedelta(minutes=5)
    window_label = (
        f"{window_start.strftime('%Y-%m-%d %H:%M')} - {window_end.strftime('%H:%M')}"
    )
    print(f"\n[WINDOW] {window_label} | Records so far: {total_records:,}")

    all_known_zones = set(window_counts.keys()) | set(zone_history.keys())

    for z_id in all_known_zones:
        count = window_counts.get(z_id, 0)
        history = zone_history[z_id]

        if len(history) >= 3:
            mean = statistics.mean(history)
            # statistics.stdev requires at least 2 values; history has >= 3 here
            std = statistics.stdev(history)
            threshold = mean + (2.0 * std)

            if count > threshold and count > (mean + 1):
                print(f"  [ANOMALY] Zone {z_id}: count={count}, mean={mean:.2f}, std={std:.2f}")
                explanation = explain_anomaly(z_id, window_label, count, mean, std)
                anomalies_list.insert(0, {
                    "zone_id": z_id,
                    "window": window_label,
                    "observed_count": count,
                    "baseline_mean": round(mean, 2),
                    "baseline_std": round(std, 2),
                    "explanation": explanation,
                    "detected_at": datetime.now().strftime("%H:%M:%S"),
                })
                # Cap feed to avoid unbounded growth
                del anomalies_list[ANOMALY_FEED_CAP:]
                atomic_write_json(ANOMALIES_FILE, anomalies_list)

        history.append(count)

    top_zones = sorted(
        [{"zone": str(k), "trips": v} for k, v in window_counts.items()],
        key=lambda x: x["trips"],
        reverse=True,
    )[:10]

    atomic_write_json(METRICS_FILE, {
        "total_records_processed": total_records,
        "anomalies_detected": len(anomalies_list),
        "last_window_time": window_label,
        "top_zones": top_zones,
    })


def run_consumer() -> None:
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="nyc-taxi-anomaly-detector",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    current_window_start = None
    window_counts: dict[int, int] = defaultdict(int)
    zone_history: dict[int, deque] = defaultdict(lambda: deque(maxlen=12))
    anomalies_list: list = []
    total_records = 0

    # Initialize empty snapshots so the dashboard starts cleanly
    atomic_write_json(ANOMALIES_FILE, [])
    atomic_write_json(METRICS_FILE, {
        "total_records_processed": 0,
        "anomalies_detected": 0,
        "last_window_time": "Waiting for first window...",
        "top_zones": [],
    })

    print(f"Consumer listening on topic '{KAFKA_TOPIC}' ({BOOTSTRAP_SERVERS})...")
    print(f"Gemini context: {'enabled' if gemini_client else 'disabled (set GEMINI_API_KEY)'}")
    print("Note: all metrics are session-scoped and reset on restart.\n")

    try:
        for msg in consumer:
            # Validate message structure before processing
            try:
                record = msg.value
                pickup_dt = datetime.fromisoformat(record["tpep_pickup_datetime"])
                zone_id = int(record["PULocationID"])
            except (KeyError, TypeError, ValueError) as exc:
                print(f"  [SKIP] Malformed message: {exc}")
                continue

            total_records += 1
            msg_window = floor_to_5min(pickup_dt)

            if current_window_start is None:
                current_window_start = msg_window

            if msg_window > current_window_start:
                flush_window(
                    current_window_start,
                    dict(window_counts),
                    zone_history,
                    anomalies_list,
                    total_records,
                )
                current_window_start = msg_window
                window_counts.clear()

            window_counts[zone_id] += 1

    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        # Flush the final in-progress window before shutting down
        if current_window_start is not None and window_counts:
            print("Flushing final window before shutdown...")
            flush_window(
                current_window_start,
                dict(window_counts),
                zone_history,
                anomalies_list,
                total_records,
            )
        consumer.close()
        print("Consumer closed cleanly.")


if __name__ == "__main__":
    run_consumer()
