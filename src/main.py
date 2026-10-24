import os
import sys
import logging

# Garante que o diretório raiz do projeto esteja no sys.path para permitir a importação dos módulos internos.
current_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in locals() else os.getcwd()
if current_dir not in sys.path:
    sys.path.append(current_dir)

# Imports dos módulos do projeto: validação de ambiente, configuração, manipulação de dados e transformações.
# from session.spark_session      import SparkSessionManager
from extract.validator          import EnvironmentValidator
from config.settings            import carregar_config, configurar_logging
from io_utils.data_handler      import DataHandler
from processing.transformations import Transformation
from pipeline.pipeline          import Pipeline

def main():
    # A sessão Spark é gerenciada e injetada automaticamente pelo runtime do Databricks Serverless.
    # spark = SparkSessionManager.get_spark_session()

    # Carrega as configurações externalizadas a partir do arquivo de settings (caminhos, parâmetros, etc.).
    config = carregar_config()
    
    # Ativa o log instantaneamente.
    configurar_logging(config['logging'])

    # Inicia a aplicação (logger).
    logger = logging.getLogger(__name__)
    logger.info(f'Iniciando o job: {config['spark']['app_name']}')

    # Instancia o DataHandler, responsável pelas operações de leitura e escrita de dados no Data Lake.
    dh = DataHandler(spark)

    # Instancia o Transformation, responsável pelas regras de transformação entre as camadas.
    transformer = Transformation()

    # Instancia o Pipeline, responsável pela orquestração a execução das transformações do pipeline (Injeção de dependências).
    pipeline = Pipeline(spark = spark, data_handler = dh, transformer = transformer)
    pipeline.run(config = config)

    # A sessão Spark NÃO é encerrada manualmente, pois sua ciclo de vida é gerenciado pelo runtime do Databricks.
    # spark.stop()
    logger.info('Pipeline finalizado com sucesso!')

if __name__ == '__main__':
    main()
