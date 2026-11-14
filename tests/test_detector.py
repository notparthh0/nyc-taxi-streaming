from streaming.detector import Spike, baseline, detect_spikes


def test_flags_a_clear_spike():
    history = {132: [10, 12, 11, 9, 10, 12, 11, 10, 9, 11, 12, 10]}
    spikes = detect_spikes({132: 40}, history)
    assert spikes == [Spike(zone=132, trips=40, baseline_mean=10.58, baseline_std=1.08, threshold=12.75)]


def test_normal_window_is_not_flagged():
    history = {132: [10, 12, 11, 9, 10, 12, 11, 10, 9, 11, 12, 10]}
    assert detect_spikes({132: 12}, history) == []


def test_needs_three_windows_of_history():
    assert detect_spikes({132: 40}, {132: [10, 10]}) == []
    assert len(detect_spikes({132: 40}, {132: [10, 10, 11]})) == 1


def test_sparse_zone_noise_is_ignored_by_min_trips():
    history = {5: [0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0]}
    assert detect_spikes({5: 3}, history, min_trips=5) == []
    assert len(detect_spikes({5: 3}, history, min_trips=1)) == 1


def test_higher_k_is_stricter():
    history = {1: [10, 14, 8, 12, 10, 9, 13, 11, 10, 12, 8, 13]}
    # mean 10.83, std 1.99 -> threshold 14.82 at k=2, 16.81 at k=3
    assert len(detect_spikes({1: 16}, history, k=2.0)) == 1
    assert detect_spikes({1: 16}, history, k=3.0) == []


def test_zone_without_history_is_skipped():
    assert detect_spikes({7: 50}, {}) == []


def test_baseline_of_short_history():
    assert baseline([]) == (0.0, 0.0)
    assert baseline([4]) == (4.0, 0.0)


def test_gemini_prompt_names_the_zone_and_strict_mode_reports_missing_key(monkeypatch):
    import pytest
    from streaming import explain as ex

    spike = Spike(zone=132, trips=40, baseline_mean=10.0, baseline_std=3.0, threshold=19.0)
    prompt = ex.build_prompt(spike, "2023-03-14 08:00 - 08:05")
    assert "JFK Airport (Queens)" in prompt and "TLC zone 132" in prompt
    assert ex.fallback(spike, "x").startswith("JFK Airport (Queens): 40 pickups")
    monkeypatch.setattr(ex, "_client", None)
    monkeypatch.setattr(ex, "_client_ready", True)
    with pytest.raises(RuntimeError, match="GEMINI_API_KEY"):
        ex.explain(spike, "x", strict=True)

    class FakeModels:
        def generate_content(self, model, contents, **kwargs):
            return type("R", (), {"text": " Likely an arriving flight bank (hypothesis). "})()

    monkeypatch.setattr(ex, "_client", type("C", (), {"models": FakeModels()})())
    assert ex.explain(spike, "x", strict=True) == "Likely an arriving flight bank (hypothesis)."
