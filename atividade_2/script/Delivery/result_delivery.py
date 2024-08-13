import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from io import BytesIO

#Variáveis
bucket_name = 'eedb-011-data-lake'
raw_file_path = 'Delivery/tb_delivery.parquet'

#Cria os DataFrames a partir dos parquet
df_banks = pd.read_parquet('s3://eedb-011-data-lake/Truested/dw_banks.parquet', engine='pyarrow')
df_complaints = pd.read_parquet('s3://eedb-011-data-lake/Truested/dw_complaints.parquet', engine='pyarrow')
df_employees = pd.read_parquet('s3://eedb-011-data-lake/Truested/dw_employees.parquet', engine='pyarrow')

df_inner_join = pd.merge(df_banks, df_complaints, on='cnpj', how='inner')
df_result = pd.merge(df_inner_join, df_employees, on='cnpj', how='right')

# Convertendo DataFrame para Parquet em um buffer de memória
buffer = BytesIO()
table = pa.Table.from_pandas(df_result)
pq.write_table(table, buffer)

# Conectando ao S3 e fazendo o upload
s3_client = boto3.client('s3')
buffer.seek(0)  # Voltar ao início do buffer
s3_client.upload_fileobj(buffer, bucket_name, raw_file_path)

print(f'Arquivo {raw_file_path} salvo com sucesso no bucket {bucket_name}.')

#---------------------------------------------------------------------------------------------
import sys
from awsglue.transforms import *
from awsglue.context import GlueContext
from pyspark.context import SparkContext

# Inicialize o GlueContext e o SparkContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

spark_df = spark.createDataFrame(df_employees)

# Configurações do PostgreSQL
postgres_url = 'jdbc:postgresql://eedb.cn6s8gc84h78.us-east-2.rds.amazonaws.com:5432/postgres'
postgres_properties = {
    'user': 'postgres',
    'password': 'postgres',
    'driver': 'org.postgresql.Driver'
}

# Salvar o DataFrame no PostgreSQL
spark_df.write.jdbc(url=postgres_url, table='tb_delivery', mode='overwrite', properties=postgres_properties)

print('Dados carregados com sucesso no PostgreSQL.')


