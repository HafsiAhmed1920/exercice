from reporting_tool.Common.reader import parquet_reader
import pytest
# Create a new SparkSessionManager instance


@pytest.mark.usefixtures("spark")
def test_parquet_reader(tmpdir, spark):
    # Create a temporary Parquet file for testing
    data = [("Alice", 1), ("Bob", 2)]
    df = spark.createDataFrame(data, ["name", "age"])
    filepath = str(tmpdir.join("test.parquet"))
    df.write.parquet(filepath)

    # Test the parquet_reader function
    result = parquet_reader(filepath)
    assert result.collect() == df.collect()
