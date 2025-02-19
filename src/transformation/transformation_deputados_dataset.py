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



s3_path = os.getenv("source_s3_path",default="s3a://gastos-deputados-9723-dev/gastos/")


df = read_s3_data(s3_path)

df = df.select(
    F.col('id').cast(IntegerType()).alias('id'),
    F.col('ano').cast(IntegerType()).alias('ano'),
    F.col('mes').cast(IntegerType()).alias('mes'),
    F.col('tipoDespesa').cast(StringType()).alias('tipoDespesa'),
    F.col('codDocumento').cast(IntegerType()).alias('codDocumento'),
    F.col('tipoDocumento').cast(StringType()).alias('tipoDocumento'),
    F.col('codTipoDocumento').cast(IntegerType()).alias('codTipoDocumento'),
    F.col('dataDocumento').cast(TimestampType()).alias('dataDocumento'),
    F.col('numDocumento').cast(StringType()).alias('numDocumento'),
    F.col('valorDocumento').cast(FloatType()).alias('valorDocumento'),
    F.col('urlDocumento').cast(StringType()).alias('urlDocumento'),
    F.col('nomeFornecedor').cast(StringType()).alias('nomeFornecedor'),
    F.col('cnpjCpfFornecedor').cast(StringType()).alias('cnpjCpfFornecedor'),
    F.col('valorLiquido').cast(FloatType()).alias('valorLiquido'),
    F.col('valorGlosa').cast(FloatType()).alias('valorGlosa'),
    F.col('numRessarcimento').cast(StringType()).alias('numRessarcimento'),
    F.col('codLote').cast(IntegerType()).alias('codLote'),
    F.col('parcela').cast(IntegerType()).alias('parcela')
)
df = df.dropDuplicates(['id'])
s3_destiny_path = os.getenv("s3_destiny_path",default="s3a://gastos-deputados-9723-dev/processed/gastos/") 
write_s3_data(df, s3_destiny_path)




