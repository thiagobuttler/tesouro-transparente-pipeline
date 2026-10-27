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
from io_utils.exceptions        import LoadTesouroTransparenteException, LoadSilverGoldException, DataHandlerException
from pyspark.errors             import PySparkException

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

    try:
        # Instancia o DataHandler, responsável pelas operações de leitura e escrita de dados no Data Lake.
        dh = DataHandler(spark)

        # Instancia o Transformation, responsável pelas regras de transformação entre as camadas.
        transformer = Transformation()

        # Instancia o Pipeline, responsável pela orquestração a execução das transformações do pipeline (Injeção de dependências).
        pipeline = Pipeline(spark = spark, data_handler = dh, transformer = transformer)
        pipeline.run(config = config)
    
    except LoadTesouroTransparenteException as e:
        # 1. Tratamento específico para o dataset crítico do tesouro transparente.
        logger.exception(f'Falha no carregamento dos dados do tesouro direto: {e}')
        sys.exit(1)

    except LoadSilverGoldException as e:
        # 2. Tratamento específico para os datasets das camadas silver e gold.
        logger.exception(f'Falha no carregamento de tabelas da camada silver e gold: {e}')
        sys.exit(1)

    except DataHandlerException as e:
        # 3. Tratamento genérico para qualquer outra falha de I/O.
        logger.exception(f'Erro na camada de leitura/escrita de dados: {e}')
        sys.exit(1)

    except PySparkException as e:
        # 4. Falhas do Spark ocorridas fora da leitura (ex: ações nas transformações).
        logger.exception(f'Erro originado no PySpark [Classe: {e.getErrorClass()}]: {e}')
        sys.exit(1)

    except Exception as e:
        # 5. Última linha de defesa para erros inesperados.
        logger.exception(f'Erro inesperado durante a execução do job: {e}')
        sys.exit(1)

    finally:
        # A sessão Spark NÃO é encerrada manualmente, pois sua ciclo de vida é gerenciado pelo runtime do Databricks.
        # spark.stop()
        logger.info('Pipeline finalizado com sucesso!')    

if __name__ == '__main__':
    main()
