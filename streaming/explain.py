"""
Plain-language context for a detected spike, from Gemini when GEMINI_API_KEY is set.

The text is labelled as generated context, not a verified cause. Calls are capped
per micro-batch so a burst of spikes cannot run up the API bill, and every
failure falls back to a factual sentence built from the numbers.
"""

import csv
import os
from functools import lru_cache
from pathlib import Path

from streaming.detector import Spike

ZONE_LOOKUP = Path(__file__).resolve().parents[1] / "reference" / "taxi_zone_lookup.csv"

# Lite model: answers in ~2 s. The larger flash models timed out (HTTP 504) for this key in Nov 2026.
MODEL = os.getenv("GEMINI_MODEL", "gemini-3.5-flash-lite")
# A slow or stuck API call must never hold up a micro-batch.
TIMEOUT_SECONDS = float(os.getenv("GEMINI_TIMEOUT_SECONDS", "30"))
_client = None
_client_ready = False


def _get_client():
    global _client, _client_ready
    if not _client_ready:
        _client_ready = True
        key = os.getenv("GEMINI_API_KEY", "")
        if key and key != "your_key_here":
            try:
                from google import genai
                from google.genai import types
                _client = genai.Client(api_key=key, http_options=types.HttpOptions(
                    timeout=int(TIMEOUT_SECONDS * 1000),
                    retry_options=types.HttpRetryOptions(attempts=1)))
            except Exception as exc:  # library missing or bad key: stay on the fallback
                print(f"[explain] Gemini disabled: {exc}")
    return _client


@lru_cache(maxsize=1)
def _zones():
    """TLC zone id -> 'Zone (Borough)', e.g. 132 -> 'JFK Airport (Queens)'."""
    if not ZONE_LOOKUP.exists():
        return {}
    with ZONE_LOOKUP.open() as f:
        return {int(r["LocationID"]): f"{r['Zone']} ({r['Borough']})" for r in csv.DictReader(f)}


def zone_name(zone: int) -> str:
    return _zones().get(zone, f"zone {zone}")


def fallback(spike: Spike, window_label: str) -> str:
    return (f"{zone_name(spike.zone)}: {spike.trips} pickups in {window_label}, against a usual level of "
            f"{spike.baseline_mean:.1f} (spread {spike.baseline_std:.1f}).")


def build_prompt(spike: Spike, window_label: str) -> str:
    return (
        "NYC Yellow Taxi demand spike.\n"
        f"Pickup zone: {zone_name(spike.zone)} (TLC zone {spike.zone})\n"
        f"5-minute window: {window_label}\n"
        f"Observed pickups: {spike.trips}; usual level for this zone at this time: "
        f"{spike.baseline_mean:.2f} (spread {spike.baseline_std:.2f})\n\n"
        "In 1-2 sentences, describe the size of the spike relative to the usual level and suggest "
        "plausible causes for this time and place. Label causes as hypotheses, not facts."
    )


def explain(spike: Spike, window_label: str, strict: bool = False) -> str:
    """Gemini's explanation, or the factual fallback if Gemini is off or fails.
    strict=True raises instead of falling back (used by evaluation/check_gemini.py)."""
    client = _get_client()
    if client is None:
        if strict:
            raise RuntimeError("Gemini is not configured: set GEMINI_API_KEY in .env")
        return fallback(spike, window_label)
    try:
        from google.genai import types
        response = client.models.generate_content(
            model=MODEL, contents=build_prompt(spike, window_label),
            config=types.GenerateContentConfig(max_output_tokens=200))
        return (response.text or "").strip()
    except Exception as exc:
        if strict:
            raise
        return fallback(spike, window_label) + f" (Gemini unavailable: {type(exc).__name__})"
