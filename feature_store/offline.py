from datetime import datetime
from pyspark.sql import DataFrame, functions as F
from common import config as cfg

def _with_partition(df, col):
    return df.withColumn("dt", F.to_date(F.col(col)))

def append_raw_events(df: DataFrame) -> None:
    out = df.withColumn("_ingest_ts", F.current_timestamp())
    out = _with_partition(out, "event_time")
    (out.write.format("delta").mode("append").partitionBy("dt")
        .save(cfg.RAW_EVENTS_PATH))

def read_raw_events(spark, start: datetime, end: datetime) -> DataFrame:
    return (spark.read.format("delta").load(cfg.RAW_EVENTS_PATH)
            .filter((F.col("event_time") >= F.lit(start)) &
                    (F.col("event_time") <= F.lit(end))))

def read_recent_events(spark, end: datetime, lookback_sec: int) -> DataFrame:
    df = spark.read.format("delta").load(cfg.RAW_EVENTS_PATH)
    return df.filter(
        (F.col("event_time") <= F.lit(end)) &
        (F.col("event_time") >= F.expr(
            f"timestamp'{end.isoformat()}' - INTERVAL {lookback_sec} SECONDS")))

def append_features(df: DataFrame) -> None:
    out = (df.withColumn("_ingest_ts", F.current_timestamp())
             .withColumn("feature_def_version", F.lit(__import__(
                 "feature_defs.feature_spec", fromlist=["FEATURE_VERSION"]).FEATURE_VERSION)))
    out = _with_partition(out, "feature_ts")
    (out.write.format("delta").mode("append").partitionBy("dt").save(cfg.FEATURES_PATH))

def read_features(spark) -> DataFrame:
    return spark.read.format("delta").load(cfg.FEATURES_PATH)
