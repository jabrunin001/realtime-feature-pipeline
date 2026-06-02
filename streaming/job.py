from pyspark.sql import functions as F, types as T
from common import config as cfg
from common.spark import build_spark
from streaming.sinks import process_batch

SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("session_id", T.StringType()),
    T.StructField("user_id", T.StringType()),
    T.StructField("content_id", T.StringType()),
    T.StructField("content_length_sec", T.DoubleType()),
    T.StructField("event_time", T.StringType()),
    T.StructField("event_type", T.StringType()),
    T.StructField("position_sec", T.DoubleType()),
    T.StructField("playback_rate", T.DoubleType()),
    T.StructField("bitrate_kbps", T.LongType()),
    T.StructField("buffer_health_sec", T.DoubleType()),
    T.StructField("cdn_pop", T.StringType()),
    T.StructField("device", T.StringType()),
    T.StructField("network_type", T.StringType()),
])

def main():
    spark = build_spark("streaming-features")
    spark.sparkContext.setLogLevel("WARN")
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", cfg.KAFKA_BOOTSTRAP)
           .option("subscribe", cfg.TOPIC)
           .option("startingOffsets", "latest").load())
    parsed = (raw.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("e"))
                 .select("e.*")
                 .withColumn("event_time", F.to_timestamp("event_time"))
                 .withWatermark("event_time", cfg.WATERMARK))
    (parsed.writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", f"s3a://{cfg.S3_BUCKET}/_chk/streaming")
        .trigger(processingTime="10 seconds").start().awaitTermination())

if __name__ == "__main__":
    main()
