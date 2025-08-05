import json
import pandas as pd
import numpy as np
from io import StringIO
from confluent_kafka import Consumer
import boto3
from datetime import datetime

BUCKET_ORIGEM = 'lake-csv'
BUCKET_DESTINO = 'bucket-transform-csv'

# Config kafka
KAFKA_CONFIG = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'transform_csv_group',
    'auto.offset.reset': 'earliest'
}

# cliente minio
def conectar_minio():
    return boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='minio',
        aws_secret_access_key='minio123'
    )
def garantir_bucket_existe(s3, nome_bucket):
    try:
        s3.head_bucket(Bucket=nome_bucket)
    except s3.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            print(f"Bucket '{nome_bucket}' não encontrado. Criando...")
            s3.create_bucket(Bucket=nome_bucket)
        else:
            raise

def conectar_kafka():
    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe(['transform_csv'])
    return consumer

# lendo os daditos
def listar_dados(s3):
    fontes = ['api', 'database', 'csv']
    todos_dados = pd.DataFrame()

    for fonte in fontes:
        try:
            arquivos = s3.list_objects(Bucket=BUCKET_ORIGEM, Prefix=fonte).get('Contents', [])
            for arquivo in arquivos:
                obj = s3.get_object(Bucket=BUCKET_ORIGEM, Key=arquivo['Key'])
                df = pd.read_json(obj['Body'])
                df['fonte'] = fonte
                todos_dados = pd.concat([todos_dados, df])
        except Exception as e:
            print(f"Problema na leitura da fonte '{fonte}': {str(e)}")

    return todos_dados

# pre-processamento
def preprocessar_dados(df):
    print("Pre-processamento iniciado...")

    # Remove colunas
    df = df.dropna(axis=1, how='all')
    df = df.dropna(axis=0, how='all')
   # Substitui
    for coluna in df.columns:
        if df[coluna].dtype == 'object':
            df[coluna] = df[coluna].replace(
                ['N/A', 'n/a', 'NA', 'na', 'null', 'NULL', 'NaN', 'nan'], pd.NA
            )
    # Substitui
    df.replace([np.inf, -np.inf], np.nan, inplace=True)

    # imputacao por media
    for coluna in df.columns:
        if df[coluna].isnull().any():
            if pd.api.types.is_numeric_dtype(df[coluna]):
                media = df[coluna].mean()
                df[coluna].fillna(media, inplace=True)
            else:
                moda = df[coluna].mode().iloc[0] if not df[coluna].mode().empty else ''
                df[coluna].fillna(moda, inplace=True)
    #padronizacao
    for coluna in df.select_dtypes(include=['float64', 'int64']).columns:
        min_val = df[coluna].min()
        max_val = df[coluna].max()
        if min_val != max_val:
            df[coluna] = (df[coluna] - min_val) / (max_val - min_val)

    print("Pre-processamento finalizado.")
    return df

def salvar_csv_mensal(s3, df):
    try:
        df['data'] = pd.to_datetime(df['date'])
        df['mes_ano'] = df['data'].dt.strftime('%Y-%m')

        for mes, grupo in df.groupby('mes_ano'):
            nome_arquivo = f"csv/{mes}.csv"
            conteudo = grupo.to_csv(index=False)

            s3.put_object(
                Bucket=BUCKET_DESTINO,
                Key=nome_arquivo,
                Body=conteudo,
                ContentType='text/csv'
            )
            print(f"Arquivo salvo: {nome_arquivo}")
    except Exception as e:
        print(f"Falha ao salvar CSVs: {str(e)}")

def main():
    print("Iniciando transformador CSV")
    s3 = conectar_minio()
    garantir_bucket_existe(s3, BUCKET_DESTINO)
    consumer = conectar_kafka()

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                print("Aguardando mensagens do Kafka")
                continue

            if msg.error():
                print(f"Erro na mensagem: {msg.error()}")
                continue

            print("\nNova mensagem recebida!")
            inicio = datetime.now()

            dados = listar_dados(s3)

            if dados.empty:
                print("Nenhum dado para processar.")
                continue

            dados = preprocessar_dados(dados) 

            salvar_csv_mensal(s3, dados)
            consumer.commit()
            print(f"Transformacao relizada em: {datetime.now() - inicio}")

    except KeyboardInterrupt:
        print("Parando transformador")
    finally:
        consumer.close()
        print("Conexoes encerradas.")

if __name__ == "__main__":
    main()
