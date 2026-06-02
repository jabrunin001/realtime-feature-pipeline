import pytest
from datetime import datetime, timedelta
pytestmark = pytest.mark.integration

def test_append_and_read_raw_events_roundtrip():
    from common.spark import build_spark
    from feature_store import offline
    spark = build_spark("test-offline")
    t0 = datetime(2026, 6, 2, 18, 0, 0)
    df = spark.createDataFrame([
        dict(event_id="1", session_id="s1", user_id="u", content_id="c",
             content_length_sec=600.0, event_time=t0, event_type="play",
             position_sec=0.0, playback_rate=1.0, bitrate_kbps=2000,
             buffer_health_sec=5.0, cdn_pop="ord1", device="web", network_type="wifi"),
    ])
    offline.append_raw_events(df)
    recent = offline.read_recent_events(spark, t0 + timedelta(seconds=30), 600)
    assert recent.filter("session_id = 's1'").count() >= 1
    spark.stop()
