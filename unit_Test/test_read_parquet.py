import pytest
from pyspark.sql import SparkSession
from reporting_tool.Common.reader import parquet_reader 


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("pytest") \
        .getOrCreate()
    yield spark
    spark.stop()


def test_parquet_reader(spark, tmpdir):
    # Create a temporary Parquet file for testing
    data = [("Alice", 1), ("Bob", 2)]
    df = spark.createDataFrame(data, ["name", "age"])
    filepath = str(tmpdir.join("test.parquet"))
    df.write.parquet(filepath)

    # Test the parquet_reader function
    result = parquet_reader(filepath)
    assert result.collect() == df.collect()
