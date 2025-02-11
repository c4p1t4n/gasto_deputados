import logging
import awswrangler as wr
import pandas as pd

def upload_s3_parquet_file(path:str,df:pd.DataFrame):
    """
    Uploads a Pandas DataFrame to an S3 bucket as a Parquet file.

    Args:
        path (str): The full S3 path where the Parquet file will be stored.
        df (pd.DataFrame): The DataFrame to be uploaded.
    
    Returns:
        None
    """
    wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True
    )
    logging.info("Files uploaded")
