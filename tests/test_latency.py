from src.monitoring.stats import LatencyTracker


def test_latency_tracker_p99():
    tracker = LatencyTracker(window=5)
    for val in [1, 2, 3, 4, 100]:
        tracker.record("q", val)
    assert tracker.p99("q") == 100
