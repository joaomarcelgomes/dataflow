import pandas as pd
import numpy as np
from confluent_kafka import Consumer
import boto3
from io import BytesIO
from datetime import datetime

# Nome do bucket de origem
BUCKET_ORIGEM = 'lake-csv'
# Nome do bucket de destino
BUCKET_DESTINO = 'bucket-transform-parquet'

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
            print(f"Bucket '{nome_bucket}' nao existe. Criando agora.")
            s3.create_bucket(Bucket=nome_bucket)
        else:
            raise

def carregar_dados(s3):
    fontes = ['api', 'database', 'csv']
    tudo_junto = pd.DataFrame()

    for fonte in fontes:
        try:
            objetos = s3.list_objects(Bucket=BUCKET_ORIGEM, Prefix=fonte).get('Contents', [])
            for obj in objetos:
                conteudo = s3.get_object(Bucket=BUCKET_ORIGEM, Key=obj['Key'])
                df = pd.read_json(conteudo['Body'])
                df['fonte'] = fonte
                tudo_junto = pd.concat([tudo_junto, df])
        except Exception as e:
            print(f"Erro ao carregar dados da fonte {fonte}: {str(e)}")

    return tudo_junto

def preprocessar_dados(df):
    print("Pre-processamento iniciado")

    # Remove colunas e linhas
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

    # Imputacao
    for coluna in df.columns:
        if df[coluna].isnull().any():
            if pd.api.types.is_numeric_dtype(df[coluna]):
                media = df[coluna].mean()
                df[coluna].fillna(media, inplace=True)
            else:
                moda = df[coluna].mode().iloc[0] if not df[coluna].mode().empty else ''
                df[coluna].fillna(moda, inplace=True)

    # Padronizacao
    for coluna in df.select_dtypes(include=['float64', 'int64']).columns:
        min_val = df[coluna].min()
        max_val = df[coluna].max()
        if min_val != max_val:
            df[coluna] = (df[coluna] - min_val) / (max_val - min_val)

    print("Pre-processamento finalizado.")
    return df

def salvar_em_parquet(s3, df):
    try:
        df['ano'] = pd.to_datetime(df['date']).dt.year

        for ano, grupo in df.groupby('ano'):
            buffer = BytesIO()
            grupo.to_parquet(buffer, index=False)

            caminho = f"parquet/ano={ano}/dados.parquet"
            s3.put_object(
                Bucket=BUCKET_DESTINO,
                Key=caminho,
                Body=buffer.getvalue()
            )
            print(f"Dados do ano {ano} salvos no caminho: {caminho}")
    except Exception as e:
        print(f"Erro ao salvar dados em Parquet: {str(e)}")

def main():
    print("Iniciando transformador de Parquet")
    s3 = conectar_minio()
    garantir_bucket_existe(s3, BUCKET_DESTINO)

    # Configurando Kafka para escutar o topico de transformação
    consumer = Consumer({
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'parquet_group',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe(['transform_parquet'])
    print("Aguardando mensagens no topico 'transform_parquet'")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                print("sem mensagem meu parcero")
                continue

            print("\nMensagem recebida! Iniciando transformacao")
            inicio = datetime.now()

            dados = carregar_dados(s3)

            if dados.empty:
                print("Nenhum dado encontrado nas fontes.")
                continue

            dados = preprocessar_dados(dados) 

            salvar_em_parquet(s3, dados)
            consumer.commit()
            print(f"Transformacao realizada em {datetime.now() - inicio}\n")

    except KeyboardInterrupt:
        print("Encerrando transformador")
    finally:
        consumer.close()
        print("Conexao com Kafka encerrada.")

if __name__ == "__main__":
    main()
