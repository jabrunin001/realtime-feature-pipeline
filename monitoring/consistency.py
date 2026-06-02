import redis
from pyspark.sql import functions as F
from common import config as cfg
from common.spark import build_spark
from feature_store.online import read_online
from feature_defs.feature_spec import numeric_features

TOL = 1e-6

def main():
    spark = build_spark("consistency")
    r = redis.Redis(host=cfg.REDIS_HOST, port=cfg.REDIS_PORT)
    feats = spark.read.format("delta").load(cfg.FEATURES_PATH)
    keys = [k.decode().split(":")[-1] for k in r.keys("feat:session:*")][:50]
    checked = matched = 0
    from datetime import datetime, timezone
    for sid in keys:
        online = read_online(r, sid)
        if not online:
            continue
        ts = datetime.fromisoformat(online["feature_ts"])
        if ts.tzinfo is not None:
            ts = ts.astimezone(timezone.utc).replace(tzinfo=None)
        latest = (feats.filter((F.col("session_id") == sid) & (F.col("feature_ts") == F.lit(ts)))
                       .collect())
        if not latest:
            continue
        checked += 1
        off = latest[0].asDict()
        ok = all(abs(float(off[n]) - float(online[n])) <= TOL
                 for n in numeric_features())
        matched += int(ok)
    rate = matched / checked if checked else 0.0
    print(f"consistency_checked={checked} match_rate={rate:.4f}")

if __name__ == "__main__":
    main()
