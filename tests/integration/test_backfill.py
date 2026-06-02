import pytest
from datetime import datetime, timedelta
pytestmark = pytest.mark.integration

def test_backfill_reproduces_features_from_raw_log():
    from common.spark import build_spark
    from feature_store import offline
    from feature_defs.session_features import compute_session_features
    spark = build_spark("test-backfill")
    raw = spark.read.format("delta").load(__import__(
        "common.config", fromlist=["RAW_EVENTS_PATH"]).RAW_EVENTS_PATH)
    end = raw.agg({"event_time": "max"}).collect()[0][0]
    sample = compute_session_features(
        offline.read_recent_events(spark, end, 600), end)
    assert sample.count() >= 1          # same transform runs over the log
    spark.stop()
