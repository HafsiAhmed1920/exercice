from pyspark.sql import DataFrame

# Process
def left_join(df1: DataFrame, df2: DataFrame, column_name: str):
    result = df1.join(df2, on=column_name, how='left')
    return result

def create_dataset_join1(
    maag_master: DataFrame,
    reac_ref: DataFrame,
    join_columns: list
) -> DataFrame:
    """
    This function creates a dataset by performing a left join operation between two dataframes using the specified join columns.
   
    """
    result_df = maag_master.join(reac_ref, on=join_columns, how='left')
    return result_df


def create_dataset_join2(
    leftjoin1: DataFrame,
    maag_repa: DataFrame,
    join_columns: list
) -> DataFrame:
    """
    This function creates a dataset by performing a left join operation between two dataframes using the specified join columns.
    
    """
    result_df = leftjoin1.join(maag_repa, on=join_columns, how='left')
    return result_df


def create_dataset_join3(
    leftjoin2: DataFrame,
    maag_raty: DataFrame,
    join_column: str
) -> DataFrame:
    """
    This function creates a dataset by performing a left join operation between two dataframes using the specified join column.
    
    """
    result_df = leftjoin2.join(maag_raty, on=join_column, how='left')
    return result_df

