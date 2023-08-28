"""
"""
from reporting_tool.context.conftest import SparkSessionHandler

# Create a new SparkSessionManager instance
spark_manager = SparkSessionHandler(app_name="MyApp")

# Start the Spark session
spark = spark_manager.start()


def parquet_reader(filepath): 
    df = spark.read.parquet(filepath)
    return df
