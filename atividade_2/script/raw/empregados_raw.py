import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from io import BytesIO

#Variáveis
bucket_name = 'eedb-011-data-lake'
raw_file_path = 'RAW/raw_empregados.parquet'

#Path onde os arquivos origens estão salvos
file_path_less = 's3://eedb-011-data-lake/Empregados/glassdoor_consolidado_join_match_less_v2.csv' 
file_path_match = 's3://eedb-011-data-lake/Empregados/glassdoor_consolidado_join_match_v2.csv' 

#Padrão da nomeclatura das colunas 
column_names_less = [
    'employer_name',
	'reviews_count',
	'culture_count',
	'salaries_count',
	'benefits_count',
	'employer_website',
	'employer_headquarters',
	'employer_founded',
	'employer_industry',
	'employer_revenue',
	'url',
	'geral',
	'cultura_e_valores',
	'diversidade_e_inclusao',
	'qualidade_de_vida',
	'alta_lideranca',
	'remuneracao_e_beneficios',
	'oportunidades_de_carreira',
	'percentual_recomendam_para_outras_pessoas',
	'percentual_perspectiva_positiva_da_empresa',
	'cnpj',
	'nome',
	'match_percent'
]

column_names_match = [
    'employer_name',
	'reviews_count',
	'culture_count',
	'salaries_count',
	'benefits_count',
	'employer_website',
	'employer_headquarters',
	'employer_founded',
	'employer_industry',
	'employer_revenue',
	'url',
	'geral',
	'cultura_e_valores',
	'diversidade_e_inclusao',
	'qualidade_de_vida',
	'alta_lideranca',
	'remuneracao_e_beneficios',
	'oportunidades_de_carreira',
	'percentual_recomendam_para_outras_pessoas',
	'percentual_perspectiva_positiva_da_empresa',
	'nome',
	'match_percent',
	'segmento'
]

#Cria os DataFrames a partir dos Csv
df_less = pd.read_csv(file_path_less, names=column_names_less, header=0, sep='|')
df_match = pd.read_csv(file_path_match, names=column_names_match, header=0, sep='|')

# Adicionar as colunas ausentes ao df1 e df2
df_match['cnpj'] = df_match.get('cnpj', pd.Series([None] * len(df_match)))
df_less['segmento'] = df_less.get('segmento', pd.Series([None] * len(df_less)))

# Garantir que as colunas estejam na mesma ordem
df_match = df_match[['employer_name','reviews_count','culture_count','salaries_count','benefits_count','employer_website','employer_headquarters','employer_founded','employer_industry','employer_revenue','url','geral','cultura_e_valores','diversidade_e_inclusao','qualidade_de_vida','alta_lideranca','remuneracao_e_beneficios','oportunidades_de_carreira','percentual_recomendam_para_outras_pessoas','percentual_perspectiva_positiva_da_empresa','nome', 'match_percent', 'cnpj', 'segmento']]
df_less = df_less[['employer_name','reviews_count','culture_count','salaries_count','benefits_count','employer_website','employer_headquarters','employer_founded','employer_industry','employer_revenue','url','geral','cultura_e_valores','diversidade_e_inclusao','qualidade_de_vida','alta_lideranca','remuneracao_e_beneficios','oportunidades_de_carreira','percentual_recomendam_para_outras_pessoas','percentual_perspectiva_positiva_da_empresa','nome', 'match_percent', 'cnpj', 'segmento']]

df_raw = pd.concat([df_match, df_less], ignore_index=True)

# Convertendo DataFrame para Parquet em um buffer de memória
buffer = BytesIO()
table = pa.Table.from_pandas(df_raw)
pq.write_table(table, buffer)

# Conectando ao S3 e fazendo o upload
s3_client = boto3.client('s3')
buffer.seek(0)  # Voltar ao início do buffer
s3_client.upload_fileobj(buffer, bucket_name, raw_file_path)

print(f'Arquivo {raw_file_path} salvo com sucesso no bucket {bucket_name}.')

