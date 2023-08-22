from pyspark.sql import DataFrame

# Process
def left_join(df1: DataFrame, df2: DataFrame, column_name: str):
    result = df1.join(df2, on=column_name, how='left')
    return result

def right_join(df1: DataFrame, df2: DataFrame, column_name: str):
    result = df1.join(df2, on=column_name, how='right')
    return result

def inner_join(df1: DataFrame, df2: DataFrame, column_name: str):
    result = df1.join(df2, on=column_name, how='inner')
    return result

def outer_join(df1: DataFrame, df2: DataFrame, column_name: str):
    result = df1.join(df2, on=column_name, how='outer')
    return result
