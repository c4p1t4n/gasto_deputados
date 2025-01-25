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

    path = f"s3://{s3_bucket}/test-folder/"
    
    upload_s3_parquet_file(path, df)
    s3_client= boto3.client('s3')
    try:
        response = s3_client.list_objects_v2(Bucket=s3_bucket,Prefix="test-folder/")
        assert 'Contents' in response and len(response['Contents']) > 0, f"No files found at {path}"
    except s3_client.exceptions.ClientError:
        assert False, f"File not found at {path}"


    for obj in response.get('Contents', []):
        s3_client.delete_object(Bucket=s3_bucket, Key=obj['Key'])
    
    response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix="test-folder/")
    assert 'Contents' not in response or len(response['Contents']) == 0, f"Files not deleted at {path}"
