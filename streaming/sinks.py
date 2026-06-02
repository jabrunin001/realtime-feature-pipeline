import redis
from pyspark.sql import functions as F
from common import config as cfg
from common.spark import build_spark
from feature_store import offline, online
from feature_defs.session_features import compute_session_features
from feature_defs.feature_spec import feature_names

EVENT_COLS = ["event_id","session_id","user_id","content_id","content_length_sec",
              "event_time","event_type","position_sec","playback_rate","bitrate_kbps",
              "buffer_health_sec","cdn_pop","device","network_type"]

def process_batch(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return
    # 1. durable raw log
    offline.append_raw_events(batch_df.select(*EVENT_COLS))
    spark = batch_df.sparkSession
    # 2. as_of = max event_time in this batch; recompute affected sessions from recent log
    as_of = batch_df.agg(F.max("event_time")).collect()[0][0]
    touched = [r["session_id"] for r in
               batch_df.select("session_id").distinct().collect()]
    recent = offline.read_recent_events(spark, as_of, cfg.STREAM_LOOKBACK_SEC) \
                    .filter(F.col("session_id").isin(touched))
    feats = compute_session_features(recent, as_of)
    # 3a. offline append
    offline.append_features(feats)
    # 3b. online write
    rows = [r.asDict() for r in feats.collect()]
    for row in rows:
        row["feature_ts"] = row["feature_ts"].isoformat()
    r = redis.Redis(host=cfg.REDIS_HOST, port=cfg.REDIS_PORT)
    online.write_online(r, [{**row} for row in rows])
