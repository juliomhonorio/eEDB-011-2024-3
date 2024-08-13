import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from io import BytesIO

#Variáveis
bucket_name = 'eedb-011-data-lake'
raw_file_path = 'RAW/raw_bancos.parquet'

#Path onde os arquivos origens estão salvos
file_path_bancos = 's3://eedb-011-data-lake/Bancos/EnquadramentoInicia_v2.tsv' 

#Padrão da nomeclatura das colunas 
columns_name = [
    'segmento',
	'cnpj',
	'nome'
]

#Cria os DataFrames a partir dos Csv
df = pd.read_csv(file_path_bancos, names=columns_name, header=0, sep='\t', encoding='ISO-8859-1')

# Convertendo DataFrame para Parquet em um buffer de memória
buffer = BytesIO()
table = pa.Table.from_pandas(df)
pq.write_table(table, buffer)

# Conectando ao S3 e fazendo o upload
s3_client = boto3.client('s3')
buffer.seek(0)  # Voltar ao início do buffer
s3_client.upload_fileobj(buffer, bucket_name, raw_file_path)

print(f'Arquivo {raw_file_path} salvo com sucesso no bucket {bucket_name}.')



