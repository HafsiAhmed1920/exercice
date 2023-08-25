import unittest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType


def create_dataset_join3(
    spark : SparkSession, 
    leftjoin2: DataFrame,
    maag_raty: DataFrame,
    join_column: str,
    schema: StructType
) -> DataFrame:
    """
    Creates a dataset by performing a left join operation.
    """
    result_df = (
        leftjoin2.join(maag_raty, on=join_column, how='left')
        .distinct()
        .select(*schema.fieldNames())
    )
    return result_df


class TestCreateDatasetJoin3(unittest.TestCase):
    # Define a class method that is run once before all tests in the test case
    @classmethod
    def setUpClass(cls):
        # Create a SparkSession object
        cls.spark = SparkSession.builder.appName("TestCreateDatasetJoin3").getOrCreate()
        # Create a DataFrame to represent the leftjoin2 input data
        cls.leftjoin2 = cls.spark.createDataFrame(
            [("A", 1), ("B", 2), ("C", 3)], ["id", "value"]
        )
        # Create a DataFrame to represent the maag_raty input data
        cls.maag_raty = cls.spark.createDataFrame(
            [("A", "x"), ("B", "y")], ["id", "name"]
        )
        # Define the join column for the join operation
        cls.join_column = "id"
        # Define the schema for the resulting DataFrame
        cls.schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("value", StringType(), True),
                StructField("name", StringType(), True),
            ]
        )

    # Define a test method that tests the create_dataset_join3 function
    def test_create_dataset_join3(self):
        # Call the create_dataset_join3 function with the test data and store the result
        result_df = create_dataset_join3(
            self.spark, self.leftjoin2, self.maag_raty, self.join_column, self.schema
        )
        # Create an expected DataFrame that represents the expected result of the join operation
        expected_df = self.spark.createDataFrame(
            [("A", 1, "x"), ("B", 2, "y"), ("C", 3, None)],
            ["id", "value", "name"],
        )
        # Check if the resulting DataFrame is equal to the expected DataFrame
        self.assertTrue(result_df.subtract(expected_df).count() == 0)
        self.assertTrue(expected_df.subtract(result_df).count() == 0)

    # Define a class method that is run once after all tests in the test case have completed
    @classmethod
    def tearDownClass(cls):
        # Stop the SparkSession object to release resources
        cls.spark.stop()
