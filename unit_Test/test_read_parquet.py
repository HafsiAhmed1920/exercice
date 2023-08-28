import pytest
from pyspark.sql import SparkSession
from reporting_tool.Common.reader import parquet_reader
import tempfile
import shutil


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("pytest") \
        .getOrCreate()
    yield spark
    spark.stop()


def test_parquet_reader(spark):
    # Create a temporary directory to store the test Parquet file
    temp_dir = tempfile.mkdtemp()
    try:
        # Create a test DataFrame
        data = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
        df = spark.createDataFrame(data, ["name", "age"])
        
        # Write the test DataFrame to a Parquet file
        filepath = f"{temp_dir}/test.parquet"
        df.write.parquet(filepath)
        
        # Read the Parquet file using the parquet_reader function
        result_df = parquet_reader(filepath)
        
        # Verify that the resulting DataFrame is equal to the original DF
        assert result_df.count() == df.count()
        assert result_df.select("name").collect() == df.select("name").collect()
        assert result_df.select("age").collect() == df.select("age").collect()
    finally:
        # Clean up the temporary directory
        shutil.rmtree(temp_dir)
