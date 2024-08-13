import io
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from io import BytesIO

# Configurações do bucket e prefixo
bucket_name = 'eedb-011-data-lake'  # Substitua pelo nome do seu bucket
prefix = 'Reclamacoes/'  # Substitua pelo prefixo do caminho dos arquivos
raw_file_path = 'RAW/raw_reclamacoes.parquet'

# Inicializar o cliente S3
s3_client = boto3.client('s3')

# Listar todos os arquivos CSV no bucket com o prefixo especificado
response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

# Criar uma lista para armazenar DataFrames
dataframes = []

#Padroniza o nome das colunas
column_names_reclamacoes = ['ano',
                            'trimestre',
                            'categoria',
                            'tipo',
                            'cnpj',
                            'instituicao_financeira',
                            'indice',
                            'quantidade_de_reclamacoes_reguladas_procedentes',
                            'quantidade_de_reclamacoes_reguladas_outras',
                            'quantidade_de_reclamacoes_nao_reguladas',
                            'quantidade_total_de_reclamacoes',
                            'quantidade_total_de_clientes_ccs_e_scr',
                            'quantidade_de_clientes_ccs',
                            'quantidade_de_clientes_scr']

# Ler cada arquivo CSV
for obj in response['Contents']:
    file_key = obj['Key']
    if file_key.endswith('.csv'):
        try:
            # Obter o objeto S3
            s3_object = s3_client.get_object(Bucket=bucket_name, Key=file_key)
            
            # Ler o conteúdo do arquivo CSV para um DataFrame
            #df = pd.read_csv(io.BytesIO(s3_object['Body'].read()), names=column_names_reclamacoes, header=0, sep=';', encoding='ISO-8859-1')
            df = pd.read_csv(io.BytesIO(s3_object['Body'].read()), names=column_names_reclamacoes, sep=';', encoding='ISO-8859-1', usecols=range(14))
            
            #corrigindo coluna com o type divergente
            df['quantidade_total_de_reclamacoes'] = df['quantidade_total_de_reclamacoes'].astype(str)
            df['quantidade_total_de_reclamacoes'] = df['quantidade_total_de_reclamacoes'].str.replace(r'[^\d]', '', regex=True)
            df['quantidade_total_de_reclamacoes'] = pd.to_numeric(df['quantidade_total_de_reclamacoes'], errors='coerce').astype('Int64')
    
            dataframes.append(df)
        except Exception as e:
            print(f'Erro ao ler {file_key}: {e}')

# Concatenar todos os DataFrames
df_raw = pd.concat(dataframes, ignore_index=True)
    
# Convertendo DataFrame para Parquet em um buffer de memória
buffer = BytesIO()
table = pa.Table.from_pandas(df_raw)
pq.write_table(table, buffer)

# Conectando ao S3 e fazendo o upload
s3_client = boto3.client('s3')
buffer.seek(0)  # Voltar ao início do buffer
s3_client.upload_fileobj(buffer, bucket_name, raw_file_path)

print(f'Arquivo {raw_file_path} salvo com sucesso no bucket {bucket_name}.')
