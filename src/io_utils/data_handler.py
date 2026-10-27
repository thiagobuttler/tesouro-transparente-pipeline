# scr/io_utils/data_handler

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (StructType, StructField, StringType, LongType, ArrayType, DateType, FloatType, TimestampType, DoubleType)
from pyspark.errors import AnalysisException, PySparkException
from io_utils.exceptions import LoadTesouroTransparenteException, LoadSilverGoldException

logger = logging.getLogger(__name__)

class DataHandler:
    """
    Classe responsável pela leitura (input) e escrita (output) de dados.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def _get_schema_tesouro_transparente(self) -> StructType:
        """Define e retorna o schema para o dataframe dos dados do tesouro transparente."""
        return StructType([
            StructField('Tipo Titulo',          StringType(), True),
            StructField('Data Vencimento',      StringType(), True),
            StructField('Data Base',            StringType(), True),
            StructField('Taxa Compra Manha',    DoubleType(), True),
            StructField('Taxa Venda Manha',     DoubleType(), True),
            StructField('PU Compra Manha',      DoubleType(), True),
            StructField('PU Venda Manha',       DoubleType(), True),
            StructField('PU Base Manha',        DoubleType(), True)
        ])
    
    def load_tesouro_transparente(self, path: str) -> DataFrame:
        """Carrega os dataframes da camada bronze no formato parquet."""
        try:
            schema = self._get_schema_tesouro_transparente()
            df = (
                self.spark.read
                .format('parquet')
                .schema(schema)
                .load(path)
            )

            if df.isEmpty():
                logger.warning(f"ATENÇÃO: O arquivo em '{path}' foi lido mas não contém registros.")

            return df

        except AnalysisException as e:
            logger.error(f'Erro de análise/metadados no Spark [Classe: {e.getErrorClass()}]: {e}')
            raise LoadTesouroTransparenteException(f"Falha ao carregar os dados do tesouro direto a partir de '{path}'") from e

        except PySparkException as e:
            logger.error(f'Erro no PySpark [Classe: {e.getErrorClass()} | SQLSTATE: {e.getSqlState()}]: {e}')
            raise LoadTesouroTransparenteException(f"Erro no motor Spark ao carregar os dados do tesouro direto em '{path}'") from e

    def load_silver_or_gold(self, path: str, table_name: str) -> DataFrame:
        """Carrega tabelas da camada silver ou gold."""
        try:
            df = self.spark.table(path + f".`{table_name}`")

            if df.isEmpty():
                logger.warning(f"ATENÇÃO: A tabela '{path}.{table_name}' foi lida mas não contém registros.")

            return df

        except AnalysisException as e:
            logger.error(f'Erro de análise/metadados no Spark [Classe: {e.getErrorClass()}]: {e}')
            raise LoadSilverGoldException(f"Falha ao carregar a tabela '{path}.{table_name}'") from e

        except PySparkException as e:
            logger.error(f'Erro no PySpark [Classe: {e.getErrorClass()} | SQLSTATE: {e.getSqlState()}]: {e}')
            raise LoadSilverGoldException(f"Erro no motor Spark ao carregar a tabela '{path}.{table_name}'") from e

    def write_delta(self, df: DataFrame, path: str, table_name: str):
        """Salva o DataFrame no formato delta, sobreescrevendo se já existir.
        
        :param df: DataFrame a ser salvo.
        :param path: Caminho de destino.
        :param table_name: Nome da tabela.
        """
        df.write \
            .mode('overwrite') \
            .format('delta') \
            .saveAsTable(path + f'.`{table_name}`')