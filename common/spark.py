from pyspark.sql import SparkSession
from common import config as cfg

def build_spark(app="rfp"):
    return (SparkSession.builder.appName(app)
        .config("spark.jars.packages",
                "io.delta:delta-spark_2.12:3.2.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", cfg.MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", cfg.MINIO_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", cfg.MINIO_SECRET)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate())
