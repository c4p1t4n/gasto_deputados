import os
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType, TimestampType

spark = SparkSession.builder \
    .appName("ReadS3Data") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .getOrCreate()

def read_s3_data(s3_path: str) -> DataFrame:
    """
    Read data from S3 bucket and return as a PySpark DataFrame.

    Args:
        s3_path (str): The full S3 path where the data is stored.
        schema (StructType): The schema of the data.

    Returns:
        DataFrame: A PySpark DataFrame with the data read from the S3 bucket.
    """
    options = {"mergeSchema": "true"}
    df = spark.read.format("parquet").options(**options).load(s3_path)
    return df

def write_s3_data(df: DataFrame, s3_path: str) -> None:
    """
    Write data to S3 bucket.

    Args:
        df (DataFrame): The PySpark DataFrame to be written.
        s3_path (str): The full S3 path where the data will be stored.

    Returns:
        None
    """
    df.write.mode("overwrite").parquet(s3_path)



s3_path = os.getenv("source_s3_path",default="s3a://gastos-deputados-9723-dev/raw/deputados/")


df = read_s3_data(s3_path)

df = df.select(
    F.col('id').cast(IntegerType()).alias('id'),
    F.col('uri').cast(StringType()).alias('uri'),
    F.col('nome').cast(StringType()).alias('nome'),
    F.col('siglaPartido').cast(StringType()).alias('siglaPartido'),
    F.col('uriPartido').cast(StringType()).alias('uriPartido'),
    F.col('siglaUf').cast(StringType()).alias('siglaUf'),
    F.col('idLegislatura').cast(IntegerType()).alias('idLegislatura'),
    F.col('urlFoto').cast(StringType()).alias('urlFoto')
)
df = df.dropDuplicates(['id'])
s3_destiny_path = os.getenv("s3_destiny_path",default="s3a://gastos-deputados-9723-dev/processed/deputados/") 
write_s3_data(df, s3_destiny_path)




