import json
import os
import statistics
from collections import defaultdict, deque
from datetime import datetime, timedelta
# pyrefly: ignore [missing-import]
from dotenv import load_dotenv
from kafka import KafkaConsumer
# pyrefly: ignore [missing-import]
import google.generativeai as genai

load_dotenv()

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "nyc-taxi-stream")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
os.makedirs(DATA_DIR, exist_ok=True)
ANOMALIES_FILE = os.path.join(DATA_DIR, "anomalies.json")
METRICS_FILE = os.path.join(DATA_DIR, "metrics.json")

# Configure Gemini API
model = None
if GEMINI_API_KEY and GEMINI_API_KEY != "your_key_here":
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel("gemini-2.5-flash")


def floor_to_5min(dt: datetime) -> datetime:
    """Floors a datetime object to the nearest 5-minute boundary."""
    minute = (dt.minute // 5) * 5
    return dt.replace(minute=minute, second=0, microsecond=0)


def explain_anomaly_with_gemini(
    zone_id: int, window_str: str, count: int, mean: float, std: float
) -> str:
    """Calls Gemini API to generate a 1-2 sentence explanation for the spike."""
    if not model:
        return (
            f"Zone {zone_id} saw {count} trips during {window_str}, significantly above "
            f"its recent baseline average of {mean:.1f} (±{std:.1f}). (Set GEMINI_API_KEY for AI analysis.)"
        )

    prompt = (
        f"You are an NYC urban mobility analyst. In NYC Taxi LocationID {zone_id} "
        f"during the 5-minute window starting at {window_str}, observed taxi pickup count was {count}, "
        f"compared to a rolling 1-hour baseline mean of {mean:.2f} and standard deviation of {std:.2f}. "
        f"Explain this demand anomaly in 1 to 2 concise sentences in plain English, mentioning likely real-world "
        f"factors (e.g., rush hour, event release, weather, or transit hub surge) appropriate for that time of day."
    )
    try:
        response = model.generate_content(prompt)
        return response.text.strip()
    except Exception as e:
        return f"Spike of {count} trips vs {mean:.1f} average (Gemini API error: {e})"


def atomic_write_json(filepath: str, data: object):
    """Writes JSON to a temporary file first, then atomically replaces the target file."""
    temp_path = f"{filepath}.tmp"
    with open(temp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    os.replace(temp_path, filepath)


def run_consumer():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="nyc-taxi-anomaly-detector",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    # State tracking
    current_window_start = None
    window_counts = defaultdict(int)
    zone_history = defaultdict(lambda: deque(maxlen=12))  # Rolling 12 windows per zone
    anomalies_list = []
    total_records = 0

    # Initialize empty JSON files so dashboard can start cleanly
    atomic_write_json(ANOMALIES_FILE, anomalies_list)
    atomic_write_json(
        METRICS_FILE,
        {
            "total_records_processed": 0,
            "anomalies_detected": 0,
            "last_window_time": "Waiting for first window...",
            "top_zones": [],
        },
    )

    print(f"Listening for messages on '{KAFKA_TOPIC}'...")

    try:
        for msg in consumer:
            record = msg.value
            total_records += 1

            pickup_dt = datetime.fromisoformat(record["tpep_pickup_datetime"])
            zone_id = int(record["PULocationID"])
            msg_window = floor_to_5min(pickup_dt)

            if current_window_start is None:
                current_window_start = msg_window

            # If message moves into a new 5-minute window, evaluate the completed window
            if msg_window > current_window_start:
                window_end = current_window_start + timedelta(minutes=5)
                window_label = f"{current_window_start.strftime('%Y-%m-%d %H:%M')} - {window_end.strftime('%H:%M')}"
                print(f"\n--- Completing Window: {window_label} | Total Records: {total_records:,} ---")

                # Evaluate all zones seen either in this window or in historical baseline
                all_known_zones = set(window_counts.keys()) | set(zone_history.keys())

                for z_id in all_known_zones:
                    count = window_counts.get(z_id, 0)
                    history = zone_history[z_id]

                    # Anomaly check requires at least 3 windows of prior history
                    if len(history) >= 3:
                        mean = statistics.mean(history)
                        std = statistics.stdev(history)
                        threshold = mean + (2 * std)

                        # Flag anomaly if count > mean + 2*std (and ensure meaningful jump if std == 0)
                        if count > threshold and count > (mean + 1):
                            print(
                                f"ANOMALY in Zone {z_id}: count={count} (mean={mean:.2f}, std={std:.2f})"
                            )
                            explanation = explain_anomaly_with_gemini(
                                z_id, window_label, count, mean, std
                            )
                            anomaly_event = {
                                "zone_id": z_id,
                                "window": window_label,
                                "observed_count": count,
                                "baseline_mean": round(mean, 2),
                                "baseline_std": round(std, 2),
                                "explanation": explanation,
                                "detected_at": datetime.now().strftime("%H:%M:%S"),
                            }
                            # Prepend so newest anomalies are first
                            anomalies_list.insert(0, anomaly_event)
                            atomic_write_json(ANOMALIES_FILE, anomalies_list)

                    # Append current window count to rolling history
                    history.append(count)

                # Compute top 10 zones for the completed window
                top_zones = sorted(
                    [{"zone": str(k), "trips": v} for k, v in window_counts.items()],
                    key=lambda x: x["trips"],
                    reverse=True,
                )[:10]

                metrics_payload = {
                    "total_records_processed": total_records,
                    "anomalies_detected": len(anomalies_list),
                    "last_window_time": window_label,
                    "top_zones": top_zones,
                }
                atomic_write_json(METRICS_FILE, metrics_payload)

                # Reset state for the new window
                current_window_start = msg_window
                window_counts.clear()

            # Count the current trip
            window_counts[zone_id] += 1

    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        consumer.close()


if __name__ == "__main__":
    run_consumer()
