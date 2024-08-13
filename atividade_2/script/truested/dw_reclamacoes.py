import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from io import BytesIO

#Variáveis
bucket_name = 'eedb-011-data-lake'
raw_file_path = 'Truested/dw_complaints.parquet'

#Path onde os arquivos origens estão salvos
file_path= 's3://eedb-011-data-lake/RAW/raw_reclamacoes.parquet' 

#Cria os DataFrames a partir dos parquet
df = pd.read_parquet(file_path, engine='pyarrow')

#substituindo NA por 0
df['ano'] = df['ano'].fillna(0)
df['quantidade_de_reclamacoes_reguladas_procedentes'] = df['quantidade_de_reclamacoes_reguladas_procedentes'].fillna(0)
df['quantidade_de_reclamacoes_reguladas_outras'] = df['quantidade_de_reclamacoes_reguladas_outras'].fillna(0)

#Definide os novos tipo das colunas
df['ano'] = df['ano'].astype(str)
df['trimestre'] = df['trimestre'].astype(str)
df['categoria'] = df['categoria'].astype(str)
df['tipo'] = df['tipo'].astype(str)
df['cnpj'] = df['cnpj'].astype(str)
df['instituicao_financeira'] = df['instituicao_financeira'].astype(str)
df['indice'] = df['indice'].astype(str)
df['quantidade_de_reclamacoes_reguladas_procedentes'] = df['quantidade_de_reclamacoes_reguladas_procedentes'].astype(str)
df['quantidade_de_reclamacoes_reguladas_outras'] = df['quantidade_de_reclamacoes_reguladas_outras'].astype(str)
df['quantidade_de_reclamacoes_nao_reguladas'] = df['quantidade_de_reclamacoes_nao_reguladas'].astype(str)
df['quantidade_total_de_reclamacoes'] = df['quantidade_total_de_reclamacoes'].astype(str)
df['quantidade_total_de_clientes_ccs_e_scr'] = df['quantidade_total_de_clientes_ccs_e_scr'].astype(str)
df['quantidade_de_clientes_ccs'] = df['quantidade_de_clientes_ccs'].astype(str)
df['quantidade_de_clientes_scr'] = df['quantidade_de_clientes_scr'].astype(str)

#Padroniza nome de colunas
df.rename(columns={'ano': 'year'}, inplace=True)
df.rename(columns={'trimestre': 'quarter'}, inplace=True)
df.rename(columns={'categoria': 'category'}, inplace=True)
df.rename(columns={'tipo': 'type_category'}, inplace=True)
df.rename(columns={'cnpj': 'cnpj'}, inplace=True)
df.rename(columns={'instituicao_financeira': 'financial_institution'}, inplace=True)
df.rename(columns={'indice': 'index'}, inplace=True)
df.rename(columns={'quantidade_de_reclamacoes_reguladas_procedentes': 'quantity_of_regulated_complaints_found'}, inplace=True)
df.rename(columns={'quantidade_de_reclamacoes_reguladas_outras': 'quantity_of_regulated_complaints_others'}, inplace=True)
df.rename(columns={'quantidade_de_reclamacoes_nao_reguladas': 'quantity_of_non_regulated_complaints'}, inplace=True)
df.rename(columns={'quantidade_total_de_reclamacoes': 'total_quantity_of_complaints'}, inplace=True)
df.rename(columns={'quantidade_total_de_clientes_ccs_e_scr': 'total_quantity_of_clients_ccs_e_scr'}, inplace=True)
df.rename(columns={'quantidade_de_clientes_ccs': 'quantity_of_ccs_clients'}, inplace=True)
df.rename(columns={'quantidade_de_clientes_scr': 'quantity_of_customers_scr'}, inplace=True)


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
spark_df.write.jdbc(url=postgres_url, table='dw_complaints', mode='overwrite', properties=postgres_properties)

print('Dados carregados com sucesso no PostgreSQL.')


