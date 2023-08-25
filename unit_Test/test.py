import pytest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType


def create_dataset_join3(
    spark: SparkSession,
    leftjoin2: DataFrame,
    maag_raty: DataFrame,
    join_column: str,
    schema: StructType,
) -> DataFrame:
    """
    Creates a dataset by performing a left join operation.
    """
    result_df = (
        leftjoin2.join(maag_raty, on=join_column, how="left")
        .distinct()
        .select(*schema.fieldNames())
    )
    return result_df


@pytest.fixture(scope="module")
def spark():
    # Create a SparkSession object
    spark = SparkSession.builder.appName("TestCreateDatasetJoin3").master("local[*]").getOrCreate()
    yield spark
    # Stop the SparkSession object to release resources
    spark.stop()


@pytest.fixture(scope="module")
def test_data(spark):
    # Create a DataFrame to represent the leftjoin2 input data
    leftjoin2 = spark.createDataFrame([("A", 1), ("B", 2), ("C", 3)], ["id", "value"])
    # Create a DataFrame to represent the maag_raty input data
    maag_raty = spark.createDataFrame([("A", "x"), ("B", "y")], ["id", "name"])
    # Define the join column for the join operation
    join_column = "id"
    # Define the schema for the resulting DataFrame
    schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("value", StringType(), True),
            StructField("name", StringType(), True),
        ]
    )
    return leftjoin2, maag_raty, join_column, schema


def test_create_dataset_join3(spark, test_data):
    leftjoin2, maag_raty, join_column, schema = test_data
    # Call the create_dataset_join3 function with the test data and store the result
    result_df = create_dataset_join3(spark, leftjoin2, maag_raty, join_column, schema)
    # Create an expected DataFrame that represents the expected result of the join operation
    expected_df = spark.createDataFrame(
        [("A", 1, "x"), ("B", 2, "y"), ("C", 3, None)], ["id", "value", "name"]
    )
    # Check if the resulting DataFrame is equal to the expected DataFrame
    assert result_df.subtract(expected_df).count() == 0
    assert expected_df.subtract(result_df).count() == 0
