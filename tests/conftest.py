import pytest

@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession
    builder = (
        SparkSession.builder.master("local[2]").appName("tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
    )
    s = builder.getOrCreate()
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()
