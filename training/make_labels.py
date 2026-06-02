from pyspark.sql import DataFrame, functions as F

def make_labels(events: DataFrame, lookahead_sec: int) -> DataFrame:
    # candidate label timestamps: every non-rebuffer event is a prediction point
    points = (events.filter(F.col("event_type") != "rebuffer")
                    .select("session_id", F.col("event_time").alias("label_ts")))
    rebuffers = (events.filter(F.col("event_type") == "rebuffer")
                       .select("session_id",
                               F.col("event_time").alias("reb_ts")))
    joined = points.join(rebuffers, "session_id", "left")
    has = (F.col("reb_ts").isNotNull() &
           (F.col("reb_ts") > F.col("label_ts")) &
           (F.col("reb_ts").cast("double") - F.col("label_ts").cast("double")
            <= lookahead_sec))
    return (joined.groupBy("session_id", "label_ts")
            .agg(F.max(has.cast("int")).alias("will_rebuffer_next_60s"))
            .fillna(0, subset=["will_rebuffer_next_60s"]))
