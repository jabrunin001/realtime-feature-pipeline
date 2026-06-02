from datetime import datetime
from pyspark.sql import DataFrame, functions as F, Window
from common import config as cfg

def compute_session_features(events: DataFrame, as_of: datetime) -> DataFrame:
    as_of_c = F.lit(as_of).cast("timestamp")
    e = events.withColumn("_as_of", as_of_c)
    e = e.filter(F.col("event_time") <= F.col("_as_of"))

    in_5m = F.col("event_time") >= F.expr(f"_as_of - INTERVAL {cfg.WINDOW_5M} SECONDS")
    in_30s = F.col("event_time") >= F.expr(f"_as_of - INTERVAL {cfg.WINDOW_30S} SECONDS")

    def c(cond):  # count helper
        return F.sum(F.when(cond, 1).otherwise(0))

    # bitrate switches in 5m: compare to previous bitrate within session by time
    w = Window.partitionBy("session_id").orderBy("event_time")
    e = e.withColumn("_prev_bitrate", F.lag("bitrate_kbps").over(w))
    e = e.withColumn("_switch",
                     ((F.col("_prev_bitrate").isNotNull()) &
                      (F.col("_prev_bitrate") != F.col("bitrate_kbps"))).cast("int"))
    # heartbeat gaps in 30s
    e = e.withColumn("_prev_ht",
                     F.lag(F.when(F.col("event_type") == "heartbeat",
                                  F.col("event_time"))).over(w))
    e = e.withColumn("_ht_gap",
                     F.when((F.col("event_type") == "heartbeat") &
                            F.col("_prev_ht").isNotNull(),
                            F.col("event_time").cast("double") -
                            F.col("_prev_ht").cast("double")))

    agg = e.groupBy("session_id").agg(
        F.first("_as_of").alias("feature_ts"),
        c((F.col("event_type") == "rebuffer") & in_5m).alias("rebuffer_count_5m"),
        F.coalesce(F.sum(F.when((F.col("event_type") == "rebuffer") & in_5m &
                                (F.col("playback_rate") == 0.0), 1.0)), F.lit(0.0))
            .alias("rebuffer_secs_5m"),
        F.coalesce(F.avg(F.when(in_30s, F.col("buffer_health_sec"))), F.lit(0.0))
            .alias("avg_buffer_health_30s"),
        F.coalesce(F.min(F.when(in_30s, F.col("buffer_health_sec"))), F.lit(0.0))
            .alias("min_buffer_health_30s"),
        F.coalesce(F.sum(F.when(in_5m, F.col("_switch"))), F.lit(0)).cast("int")
            .alias("bitrate_switch_count_5m"),
        F.coalesce(F.avg(F.when(in_5m, F.col("bitrate_kbps"))), F.lit(0.0))
            .alias("avg_bitrate_5m"),
        c((F.col("event_type") == "seek") & in_5m).cast("int").alias("seek_count_5m"),
        c((F.col("event_type") == "pause") & in_5m).cast("int").alias("pause_count_5m"),
        (F.first("_as_of").cast("double") - F.min("event_time").cast("double"))
            .alias("session_duration_sec"),
        (F.max("position_sec") / F.first("content_length_sec")).alias("completion_pct"),
        F.coalesce(F.max(F.when(in_30s, F.col("_ht_gap"))), F.lit(0.0))
            .alias("heartbeat_gap_max_30s"),
        F.last("device").alias("device"),
        F.last("network_type").alias("network_type"),
        F.last("cdn_pop").alias("cdn_pop"),
    )
    # cast count cols that came from sum() to int
    agg = (agg.withColumn("rebuffer_count_5m", F.col("rebuffer_count_5m").cast("int")))
    return agg
