import pytest
from _pytest.fixtures import FixtureRequest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark(request: FixtureRequest) -> SparkSession:
    # Create a new SparkSession instance
    spark = SparkSession.builder \
        .appName("MyApp") \
        .master("local[*]") \
        .getOrCreate()

    # Add a finalizer to stop the SparkSession when the test session is finished
    def stop_spark():
        spark.stop()

    request.addfinalizer(stop_spark)

    # Return the SparkSession instance
    return spark
