def save_parquet(df : DataFrame , path) : 
    df.write.parquet(path)