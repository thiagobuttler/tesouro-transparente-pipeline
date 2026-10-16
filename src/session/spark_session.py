# src/session/spark_session.py

from pyspark.sql import SparkSession
from config.settings import carregar_config

config = carregar_config()
SPARK_APP_NAME = config['spark']['app_name']

class SparkSessionManager:
    """
    Gerencia a criação e o acesso à sessão Spark.
    """
    @staticmethod
    def get_spark_session(app_name: str = SPARK_APP_NAME) -> SparkSession:
        """
        Cria e retorna uma sessão Spark.

        :param app_name: Nome da aplicação Spark.
        :return: Instância da SparkSession
        """
        return SparkSession.builder \
            .appName(app_name) \
            .getOrCreate()