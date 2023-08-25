<<<<<<< HEAD
"""
"""
from context.context import spark 


def parquet_reader(filepath): 
    df = spark.read.parquet(filepath)
    return df   


=======
"""
"""

from context.context import spark 


def parquet_reader(filepath) : 
    df = spark.read.parquet(filepath)
    return df   

>>>>>>> 3b82bc29147ae04bb8ae3be6c84f134bf5c80c1d
