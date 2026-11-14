"""
Plain-language context for a detected spike, from Gemini when GEMINI_API_KEY is set.

The text is labelled as generated context, not a verified cause. Calls are capped
per micro-batch so a burst of spikes cannot run up the API bill, and every
failure falls back to a factual sentence built from the numbers.
"""

import os

from streaming.detector import Spike

MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
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
                _client = genai.Client(api_key=key)
            except Exception as exc:  # library missing or bad key: stay on the fallback
                print(f"[explain] Gemini disabled: {exc}")
    return _client


def fallback(spike: Spike, window_label: str) -> str:
    return (f"Zone {spike.zone} recorded {spike.trips} trips in {window_label}, against a trailing "
            f"mean of {spike.baseline_mean:.1f} (std {spike.baseline_std:.1f}).")


def explain(spike: Spike, window_label: str) -> str:
    client = _get_client()
    if client is None:
        return fallback(spike, window_label)
    prompt = (
        "NYC Yellow Taxi demand spike.\n"
        f"Pickup zone (TLC PULocationID): {spike.zone}\n"
        f"5-minute window: {window_label}\n"
        f"Observed pickups: {spike.trips}; previous-hour mean {spike.baseline_mean:.2f}, "
        f"std {spike.baseline_std:.2f}\n\n"
        "In 1-2 sentences, describe the size of the spike relative to the baseline and suggest "
        "plausible causes for this time and place. Label causes as hypotheses, not facts."
    )
    try:
        return client.models.generate_content(model=MODEL, contents=prompt).text.strip()
    except Exception as exc:
        return fallback(spike, window_label) + f" (Gemini unavailable: {type(exc).__name__})"
