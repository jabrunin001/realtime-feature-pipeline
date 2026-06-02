import argparse
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from common import config as cfg
from common.spark import build_spark
from feature_store import offline
from feature_defs.session_features import compute_session_features

def run(start: datetime, end: datetime, step_sec: int = 30):
    spark = build_spark("backfill")
    all_features = []
    t = start
    while t <= end:
        events = offline.read_recent_events(spark, t, cfg.STREAM_LOOKBACK_SEC)
        if not events.rdd.isEmpty():
            all_features.append(compute_session_features(events, t))
        t += timedelta(seconds=step_sec)
    if all_features:
        out = all_features[0]
        for f in all_features[1:]:
            out = out.unionByName(f)
        offline.append_features(out)
        print(f"backfilled {out.count()} feature rows")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="frm", required=True)  # ISO timestamp
    ap.add_argument("--to", dest="to", required=True)
    ap.add_argument("--step", type=int, default=30)
    a = ap.parse_args()
    run(datetime.fromisoformat(a.frm), datetime.fromisoformat(a.to), a.step)

if __name__ == "__main__":
    main()
