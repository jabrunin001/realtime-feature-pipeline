from pyspark.sql import DataFrame, functions as F, Window

def point_in_time_join(labels: DataFrame, features: DataFrame,
                       max_staleness_sec: int) -> DataFrame:
    j = labels.join(features, on="session_id", how="inner")
    j = j.filter(
        (F.col("feature_ts") <= F.col("label_ts")) &
        (F.col("label_ts").cast("double") - F.col("feature_ts").cast("double")
         <= max_staleness_sec))
    w = Window.partitionBy("session_id", "label_ts").orderBy(F.col("feature_ts").desc())
    return (j.withColumn("_rn", F.row_number().over(w))
             .filter(F.col("_rn") == 1).drop("_rn"))
