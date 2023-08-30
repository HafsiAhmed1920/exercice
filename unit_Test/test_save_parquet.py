from reporting_tool.Common.saver import save_parquet
import pytest 


@pytest.mark.usefixtures("spark")
def test_save_parquet(tmpdir, spark):
    data = [("Alice", 1), ("Bob", 2)]
    df = spark.createDataFrame(data, ["name", "age"])
    path = str(tmpdir.join("test.parquet"))
    save_parquet(df, path)
    read_df = spark.read.parquet(path)
    assert read_df.collect() == df.collect()
