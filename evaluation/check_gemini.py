"""
Check that the Gemini explanations work.

    python evaluation/check_gemini.py          # sends the 3 biggest stored spikes to Gemini
    python evaluation/check_gemini.py --n 5

Uses GEMINI_API_KEY from .env and the spikes saved by the last stream run
(data/stream.sqlite). Prints each answer and saves everything to
data/gemini_check.json as evidence. Each spike is one API call.
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from streaming.detector import Spike  # noqa: E402
from streaming.explain import MODEL, explain, zone_name  # noqa: E402
from streaming.processor import window_label  # noqa: E402
from streaming.store import Store  # noqa: E402

KEY_URL = "https://aistudio.google.com/apikey"


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--n", type=int, default=3, help="how many spikes to explain (= API calls)")
    args = parser.parse_args(argv)

    try:
        from dotenv import load_dotenv
        load_dotenv(ROOT / ".env")
    except ImportError:
        pass
    if not os.getenv("GEMINI_API_KEY"):
        print(f"No GEMINI_API_KEY found. Get a free key at {KEY_URL}, then add this line to {ROOT / '.env'}:\n"
              f"  GEMINI_API_KEY=your-key-here")
        return 1

    db = ROOT / "data" / "stream.sqlite"
    if not db.exists():
        print("No stored spikes yet: run `make experiment` first.")
        return 1
    store = Store(db)
    rows = store.conn.execute(
        "SELECT window_start, zone, trips, baseline_mean, baseline_std, threshold FROM anomalies "
        "ORDER BY trips - baseline_mean DESC LIMIT ?", (args.n,)).fetchall()

    results, ok = [], 0
    for window_start, zone, trips, mean, std, threshold in rows:
        spike = Spike(zone, trips, mean, std, threshold)
        label = window_label(window_start)
        started = time.time()
        try:
            text, error = explain(spike, label, strict=True), None
            ok += 1
        except Exception as exc:
            text, error = None, f"{type(exc).__name__}: {exc}"
        seconds = round(time.time() - started, 2)
        results.append({"zone": zone, "zone_name": zone_name(zone), "window": label, "trips": trips,
                        "usual_level": mean, "gemini_text": text, "error": error, "seconds": seconds})
        print(f"\n{zone_name(zone)}, {label}: {trips} pickups (usual {mean:.1f})")
        print(f"  {'Gemini' if text else 'FAILED'} ({seconds}s): {text or error}")

    report = {"model": MODEL, "checked_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
              "calls": len(rows), "succeeded": ok, "results": results}
    (ROOT / "data" / "gemini_check.json").write_text(json.dumps(report, indent=2))
    print(f"\n{ok}/{len(rows)} Gemini calls succeeded (model {MODEL}). Saved to data/gemini_check.json")
    store.close()
    return 0 if ok == len(rows) and rows else 1


if __name__ == "__main__":
    sys.exit(main())
