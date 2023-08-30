import pytest
from reporting_tool.process.process import create_dataset_join


@pytest.mark.usefixtures("spark")
def test_create_dataset_join3(spark):
    # Create a DataFrame to represent the leftjoin2 input data
    table1 = spark.createDataFrame([("A", 1), ("B", 2),
                                    ("C", 3)], ["id", "value"])
                                     
    # Create a DataFrame to represent the maag_raty input data
    table2 = spark.createDataFrame([("A", "x"), ("B", "y")], ["id", "name"])
    join_column = "id"
    
    # Call the create_dataset_join3 function with the test data and store 
    result_df = create_dataset_join(table1, table2, join_column)
    
    # Create an expected DataFrame that represents the expected result 
    expected_df = spark.createDataFrame(
        [("A", 1, "x"), ("B", 2, "y"), ("C", 3, None)], ["id", "value", "name"]
    )
    
    # Check if the resulting DataFrame is equal to the expected DataFrame
    assert result_df.subtract(expected_df).count() == 0
    assert expected_df.subtract(result_df).count() == 0
