from datetime import datetime, timedelta
from feature_store.pit_join import point_in_time_join

def test_picks_latest_feature_at_or_before_label(spark):
    t0 = datetime(2026, 6, 2, 18, 0, 0)
    features = spark.createDataFrame([
        dict(session_id="s1", feature_ts=t0,                 rebuffer_count_5m=0),
        dict(session_id="s1", feature_ts=t0+timedelta(seconds=30), rebuffer_count_5m=1),
        dict(session_id="s1", feature_ts=t0+timedelta(seconds=90), rebuffer_count_5m=5),
    ])
    labels = spark.createDataFrame([
        dict(session_id="s1", label_ts=t0+timedelta(seconds=60),
             will_rebuffer_next_60s=1),
    ])
    out = point_in_time_join(labels, features, max_staleness_sec=300).collect()
    assert len(out) == 1
    assert out[0]["rebuffer_count_5m"] == 1      # the 30s row, NOT the future 90s row

def test_drops_rows_when_feature_too_stale(spark):
    t0 = datetime(2026, 6, 2, 18, 0, 0)
    features = spark.createDataFrame([
        dict(session_id="s1", feature_ts=t0, rebuffer_count_5m=0)])
    labels = spark.createDataFrame([
        dict(session_id="s1", label_ts=t0+timedelta(seconds=600),
             will_rebuffer_next_60s=0)])
    out = point_in_time_join(labels, features, max_staleness_sec=300).collect()
    assert out == []                             # 600s gap > 300s staleness cap
