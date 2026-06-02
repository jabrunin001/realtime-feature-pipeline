from datetime import datetime, timedelta
from training.make_labels import make_labels

def test_label_is_1_when_rebuffer_within_lookahead(spark):
    t0 = datetime(2026, 6, 2, 18, 0, 0)
    rows = [
        dict(session_id="s1", event_time=t0, event_type="heartbeat"),
        dict(session_id="s1", event_time=t0 + timedelta(seconds=40),
             event_type="rebuffer"),
    ]
    df = spark.createDataFrame(rows)
    out = {r["label_ts"]: r["will_rebuffer_next_60s"]
           for r in make_labels(df, 60).filter("session_id='s1'").collect()}
    assert out[t0] == 1                      # rebuffer 40s later, within 60s

def test_label_is_0_when_no_rebuffer_in_window(spark):
    t0 = datetime(2026, 6, 2, 18, 0, 0)
    rows = [
        dict(session_id="s2", event_time=t0, event_type="heartbeat"),
        dict(session_id="s2", event_time=t0 + timedelta(seconds=120),
             event_type="rebuffer"),
    ]
    out = {r["label_ts"]: r["will_rebuffer_next_60s"]
           for r in make_labels(spark.createDataFrame(rows), 60).collect()}
    assert out[t0] == 0                      # rebuffer is 120s out, beyond 60s
