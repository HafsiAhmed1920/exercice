"""
"""
from reporting_tool.context.context import spark 


def parquet_reader(filepath): 
    df = spark.read.parquet(filepath)
    return df   


