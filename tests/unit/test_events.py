from datetime import datetime
from generator.events import simulate_session, EVENT_TYPES

def test_simulate_session_yields_valid_ordered_events():
    evs = simulate_session("s1", start=datetime(2026, 6, 2, 18, 0, 0),
                           seed=42, bad_network=True)
    assert len(evs) >= 5
    assert evs[0]["event_type"] == "play"
    assert evs[-1]["event_type"] == "ended"
    # monotonic event_time
    ts = [e["event_time"] for e in evs]
    assert ts == sorted(ts)
    # required keys present on every event
    keys = {"event_id","session_id","user_id","content_id","content_length_sec",
            "event_time","event_type","position_sec","playback_rate","bitrate_kbps",
            "buffer_health_sec","cdn_pop","device","network_type"}
    for e in evs:
        assert keys <= set(e)
        assert e["event_type"] in EVENT_TYPES

def test_bad_network_sessions_produce_more_rebuffers():
    good = simulate_session("g", datetime(2026,6,2,18,0,0), seed=1, bad_network=False)
    bad  = simulate_session("b", datetime(2026,6,2,18,0,0), seed=1, bad_network=True)
    n = lambda evs: sum(e["event_type"] == "rebuffer" for e in evs)
    assert n(bad) >= n(good)
