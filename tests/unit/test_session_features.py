from datetime import datetime, timedelta
from feature_defs.session_features import compute_session_features
from feature_defs.feature_spec import feature_names

def _events(rows, spark):
    return spark.createDataFrame(rows)

def test_one_row_per_session_with_all_features(spark):
    t0 = datetime(2026, 6, 2, 18, 0, 0)
    rows = [
        dict(event_id="1", session_id="s1", user_id="u1", content_id="c1",
             content_length_sec=1000.0, event_time=t0, event_type="play",
             position_sec=0.0, playback_rate=1.0, bitrate_kbps=2000,
             buffer_health_sec=5.0, cdn_pop="ord1", device="web", network_type="wifi"),
        dict(event_id="2", session_id="s1", user_id="u1", content_id="c1",
             content_length_sec=1000.0, event_time=t0 + timedelta(seconds=10),
             event_type="rebuffer", position_sec=10.0, playback_rate=0.0,
             bitrate_kbps=800, buffer_health_sec=0.0, cdn_pop="ord1",
             device="web", network_type="wifi"),
    ]
    as_of = t0 + timedelta(seconds=15)
    out = compute_session_features(_events(rows, spark), as_of).collect()
    assert len(out) == 1
    r = out[0].asDict()
    for name in feature_names():
        assert name in r
    assert r["session_id"] == "s1"
    assert r["feature_ts"] == as_of
    assert r["rebuffer_count_5m"] == 1
    assert r["bitrate_switch_count_5m"] == 1          # 2000 -> 800
    assert r["min_buffer_health_30s"] == 0.0          # within 30s of as_of
    assert abs(r["completion_pct"] - 0.01) < 1e-6     # 10/1000
    assert r["device"] == "web"

def test_window_excludes_events_outside_5m(spark):
    t0 = datetime(2026, 6, 2, 18, 0, 0)
    rows = [
        dict(event_id="1", session_id="s2", user_id="u", content_id="c",
             content_length_sec=600.0, event_time=t0, event_type="rebuffer",
             position_sec=1.0, playback_rate=0.0, bitrate_kbps=1000,
             buffer_health_sec=0.0, cdn_pop="x", device="tv", network_type="wifi"),
    ]
    as_of = t0 + timedelta(seconds=400)               # > 5 min after the rebuffer
    r = compute_session_features(_events(rows, spark), as_of).collect()[0].asDict()
    assert r["rebuffer_count_5m"] == 0                 # outside 5m window
    assert r["session_duration_sec"] == 400.0         # session-to-date still counts
