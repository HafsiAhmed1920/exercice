import pytest
from pyspark.sql import SparkSession
from reporting_tool.Common.saver import save_parquet


def test_save_parquet(tmpdir):
    spark = SparkSession.builder.appName("test").getOrCreate()
    data = [("Alice", 1), ("Bob", 2)]
    df = spark.createDataFrame(data, ["name", "age"])
    path = str(tmpdir.join("test.parquet"))
    save_parquet(df, path)
    read_df = spark.read.parquet(path)
    assert read_df.collect() == df.collect()
