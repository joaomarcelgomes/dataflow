import pandas as pd
from confluent_kafka import Consumer
import boto3
from sqlalchemy import create_engine
from datetime import datetime
from io import BytesIO
import numpy as np

#config
BUCKET_ORIGEM = 'lake-database'
BUCKET_DESTINO = 'bucket-transform-bd'
DB_CONFIG = 'postgresql://postgres:postgres@db:5432/dataflow'

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
            print(f"Bucket '{nome_bucket}' nao encontrado. Criando now")
            s3.create_bucket(Bucket=nome_bucket)
        else:
            raise

def conectar_banco():
    return create_engine(DB_CONFIG)

def carregar_dados(s3):
    fontes = ['api', 'database', 'csv']
    dados = pd.DataFrame()

    for fonte in fontes:
        try:
            objetos = s3.list_objects(Bucket=BUCKET_ORIGEM, Prefix=fonte).get('Contents', [])
            for obj in objetos:
                conteudo = s3.get_object(Bucket=BUCKET_ORIGEM, Key=obj['Key'])
                df = pd.read_json(conteudo['Body'])
                df['fonte'] = fonte
                dados = pd.concat([dados, df])
        except Exception as e:
            print(f"Erro ao carregar dados da fonte {fonte}: {str(e)}")

    return dados

def preprocessar_dados(df):
    print("Iniciando pre-processamento...")

    df.dropna(axis=1, how='all', inplace=True)
    df.dropna(axis=0, how='all', inplace=True)

    # Substituir
    df.replace(['N/A', 'n/a', 'NA', 'na', 'null', 'NULL', 'NaN', 'nan', np.inf, -np.inf], np.nan, inplace=True)

    # Imputacao
    for coluna in df.columns:
        if df[coluna].isnull().any():
            if pd.api.types.is_numeric_dtype(df[coluna]):
                df[coluna].fillna(df[coluna].mean(), inplace=True)
            else:
                moda = df[coluna].mode().iloc[0] if not df[coluna].mode().empty else ''
                df[coluna].fillna(moda, inplace=True)

    # Padronizacao
    for coluna in df.select_dtypes(include=['float64', 'int64']).columns:
        min_val = df[coluna].min()
        max_val = df[coluna].max()
        if min_val != max_val:
            df[coluna] = (df[coluna] - min_val) / (max_val - min_val)

    print("pre-processamento finalizado.")
    return df

def inserir_linha_a_linha(engine, df):
    print("Inserindo dados no banco de dados")
    df['date'] = pd.to_datetime(df['date'])
    df = df.drop_duplicates()

    for _, linha in df.iterrows():
        try:
            linha.to_frame().T.to_sql('dados_processados', engine, if_exists='append', index=False)
        except Exception as e:
            print(f"Erro ao inserir linha no banco: {e}")

    print("Insercao realizada.")

def salvar_no_bucket(s3, df):
    print("Salvando dados no bucket da tranfromacao")
    df['data'] = pd.to_datetime(df['date'])
    df['ano'] = df['data'].dt.year
    df['mes'] = df['data'].dt.month

    for (ano, mes), grupo in df.groupby(['ano', 'mes']):
        buffer = BytesIO()
        grupo.to_json(buffer, orient='records', force_ascii=False)
        caminho = f"bd_processed/{ano}/{mes:02d}/dados_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

        s3.put_object(
            Bucket=BUCKET_DESTINO,
            Key=caminho,
            Body=buffer.getvalue(),
            ContentType='application/json'
        )
        print(f"Arquivo salvo no bucket: {caminho}")

def main():
    print("Iniciando transformador do bd")
    s3 = conectar_minio()
    garantir_bucket_existe(s3, BUCKET_DESTINO)
    engine = conectar_banco()

    consumer = Consumer({
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'bd_datalake_group',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe(['transform_bd'])

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                print("No aguardo de mensagens")
                continue

            if msg.error():
                print(f"Erro na mensagem Kafka: {msg.error()}")
                continue

            print("\nMensagem recebida! Processando")
            inicio = datetime.now()

            dados = carregar_dados(s3)
            if dados.empty:
                print("Nenhum dado encontrado.")
                continue

            dados = preprocessar_dados(dados)
            inserir_linha_a_linha(engine, dados)
            salvar_no_bucket(s3, dados)

            consumer.commit()
            print(f"Tranformacao relaizada em: {datetime.now() - inicio}")

    except KeyboardInterrupt:
        print("Interrompido pelo usuario.")
    finally:
        consumer.close()
        engine.dispose()
        print("Conexoes encerradas.")

if __name__ == "__main__":
    main()
