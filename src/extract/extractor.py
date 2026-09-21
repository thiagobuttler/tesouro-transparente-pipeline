import pandas as pd
from datetime import datetime
import boto3
import os

def lambda_handler(event, context): 

    try:

        # Captura o momentoe exato de extração do arquivo csv
        timestamp_extraction = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Define o modelo de nome do arquivo csv
        file_name = f"taxapreco_raw_{timestamp_extraction}.parquet"

        # Caminho local temp
        temp_path = f'/tmp/{file_name}'

        # Configuracoes bucket
        bucket_name = 'tesouro-transparente-pipeline'
        caminho_s3 = f'bronze/{file_name}'

        # Extrai os dados do tesouro transparencia, considerando o separador do arquivo csv como ";" e valores deciamis como ","
        print("Extraindo os dados do tesouro transparente...")
        raw_data = pd.read_csv('https://www.tesourotransparente.gov.br/ckan/dataset/df56aa42-484a-4a59-8184-7676580c81e3/resource/796d2059-14e9-44e3-80c9-2d9e30b405c1/download/precotaxatesourodireto.csv',
                                sep=';',
                                decimal=','
                            )

        # Captura o arquivo salvo na variável raw_data, converte para parquet e salva na pasta bronze
        raw_data.to_parquet(temp_path)
        print(f"Arquivo temporário salvo em: {temp_path}")

        # Envia o arquivo de temp para o S3
        print(f"Enviando para s3://{bucket_name}/{caminho_s3}")
        s3_client = boto3.client('s3')
        s3_client.upload_file(
            Filename=temp_path,
            Bucket=bucket_name,
            Key=caminho_s3
        )
        print("Upload para o S3 concluído.")

        # Limpeza
        if os.path.exists(temp_path):
            os.remove(temp_path)

        return {
            "statusCode": 200,
            "body": f"Arquivo {file_name} ingerido com sucesso na camada Bronze."
        }

    except Exception as e:
        print("Erro durante a execução:", str(e))
        raise e