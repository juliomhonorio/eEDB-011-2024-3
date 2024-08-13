import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from io import BytesIO

#Variáveis
bucket_name = 'eedb-011-data-lake'
raw_file_path = 'Truested/dw_employees.parquet'

#Path onde os arquivos origens estão salvos
file_path= 's3://eedb-011-data-lake/RAW/raw_empregados.parquet' 

#Cria os DataFrames a partir dos parquet
df = pd.read_parquet(file_path, engine='pyarrow')

#Definide os novos tipo das colunas
df['employer_name'] = df['employer_name'].astype(str)
df['reviews_count'] = df['reviews_count'].astype(int)
df['culture_count'] = df['culture_count'].astype(int)
df['salaries_count'] = df['salaries_count'].astype(float)
df['benefits_count'] = df['benefits_count'].astype(int)
df['employer_website'] = df['employer_website'].astype(str)
df['employer_headquarters'] = df['employer_headquarters'].astype(str)
df['employer_founded'] = df['employer_founded'].astype(float)
df['employer_industry'] = df['employer_industry'].astype(str)
df['employer_revenue'] = df['employer_revenue'].astype(str)
df['url'] = df['url'].astype(str)
df['geral'] = df['geral'].astype(float)
df['cultura_e_valores'] = df['cultura_e_valores'].astype(float)
df['diversidade_e_inclusao'] = df['diversidade_e_inclusao'].astype(float)
df['qualidade_de_vida'] = df['qualidade_de_vida'].astype(float)
df['alta_lideranca'] = df['alta_lideranca'].astype(float)
df['remuneracao_e_beneficios'] = df['remuneracao_e_beneficios'].astype(float)
df['oportunidades_de_carreira'] = df['oportunidades_de_carreira'].astype(float)
df['percentual_recomendam_para_outras_pessoas'] = df['percentual_recomendam_para_outras_pessoas'].astype(float)
df['percentual_perspectiva_positiva_da_empresa'] = df['percentual_perspectiva_positiva_da_empresa'].astype(float)
df['cnpj'] = df['cnpj'].fillna(0)
df['cnpj'] = df['cnpj'].astype(int)
df['cnpj'] = df['cnpj'].astype(str)
df['nome'] = df['nome'].astype(str)
df['match_percent'] = df['match_percent'].astype(str)
df['segmento'] = df['segmento'].fillna(0)
df['segmento'] = df['segmento'].astype(int)
df['segmento'] = df['segmento'].astype(str)

#Padroniza nome de colunas
df.rename(columns={'geral': 'general'}, inplace=True)
df.rename(columns={'cultura_e_valores': 'culture_and_values'}, inplace=True)
df.rename(columns={'diversidade_e_inclusao': 'diversity_and_inclusion'}, inplace=True)
df.rename(columns={'qualidade_de_vida': 'quality_of_life'}, inplace=True)
df.rename(columns={'alta_lideranca': 'high_leadership'}, inplace=True)
df.rename(columns={'remuneracao_e_beneficios': 'remuneration_and_benefits'}, inplace=True)
df.rename(columns={'oportunidades_de_carreira': 'career_opportunities'}, inplace=True)
df.rename(columns={'percentual_recomendam_para_outras_pessoas': 'percentage_recommend_to_other_people'}, inplace=True)
df.rename(columns={'percentual_perspectiva_positiva_da_empresa': 'percentage_positive_perspective_of_the_company'}, inplace=True)
df.rename(columns={'nome': 'name'}, inplace=True)
df.rename(columns={'segmento': 'segment'}, inplace=True)

# Trata campo cnpj
#df['cnpj'] = df['cnpj'].replace('.0', '')

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
spark_df.write.jdbc(url=postgres_url, table='dw_employees', mode='overwrite', properties=postgres_properties)

print('Dados carregados com sucesso no PostgreSQL.')


