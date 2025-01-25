import pytest
import pandas as pd
import awswrangler as wr
import logging
import boto3
from src.utils.main import upload_s3_parquet_file  # Ajuste o caminho para o módulo correto

@pytest.fixture
def s3_bucket():
    return "gastos-deputados-9723-dev"


def test_upload_s3_parquet_file_integration(s3_bucket):
    df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})

    path = f"s3://{s3_bucket}/test-folder/data.parquet"
    
    upload_s3_parquet_file(path, df)
    s3_client= boto3.client('s3')
    try:
        s3_client.head_object(Bucket=s3_bucket,Key="test-folder/data.parquet")
    except s3_client.exceptions.ClientError:
        assert False, f"File not found at {path}"

    s3_client.delete_object(Bucket=s3_bucket, Key="test-folder/data.parquet")
    try:
        s3_client.head_object(Bucket=s3_bucket, Key="test-folder/data.parquet")
        assert False, f"File not deleted at {path}"
    except s3_client.exceptions.ClientError:
        pass
