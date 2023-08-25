<<<<<<< HEAD
from pyspark.sql import DataFrame

def save_parquet(df : DataFrame , path) : 
=======
def save_parquet(df : DataFrame , path) : 
>>>>>>> 3b82bc29147ae04bb8ae3be6c84f134bf5c80c1d
    df.write.parquet(path)