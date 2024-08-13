import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from io import BytesIO

#Variáveis
bucket_name = 'eedb-011-data-lake'
raw_file_path = 'Truested/dw_banks.parquet'

#Path onde os arquivos origens estão salvos
file_path= 's3://eedb-011-data-lake/RAW/raw_bancos.parquet' 

#Cria os DataFrames a partir dos parquet
df = pd.read_parquet(file_path, engine='pyarrow')

#Definide os novos tipo das colunas
df['segmento'] = df['segmento'].astype(str)
df['cnpj'] = df['cnpj'].astype(str)
df['nome'] = df['nome'].astype(str)

#Padroniza nome de colunas
df.rename(columns={'segmento': 'segment'}, inplace=True)
df.rename(columns={'nome': 'name'}, inplace=True)

# Convertendo DataFrame para Parquet em um buffer de memória
buffer = BytesIO()
table = pa.Table.from_pandas(df)
pq.write_table(table, buffer)

# Conectando ao S3 e fazendo o upload
s3_client = boto3.client('s3')
buffer.seek(0)  # Voltar ao início do buffer
s3_client.upload_fileobj(buffer, bucket_name, raw_file_path)

print(f'Arquivo {raw_file_path} salvo com sucesso no bucket {bucket_name}.')

#---------------------------------------------------------------------------------------------
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# Inicialize o GlueContext e o SparkContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

spark_df = spark.createDataFrame(df)

# Configurações do PostgreSQL
postgres_url = 'jdbc:postgresql://eedb.cn6s8gc84h78.us-east-2.rds.amazonaws.com:5432/postgres'
postgres_properties = {
    'user': 'postgres',
    'password': 'postgres',
    'driver': 'org.postgresql.Driver'
}

# Salvar o DataFrame no PostgreSQL
spark_df.write.jdbc(url=postgres_url, table='dw_banks', mode='overwrite', properties=postgres_properties)

print('Dados carregados com sucesso no PostgreSQL.')


