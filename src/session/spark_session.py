# src/session/spark_session.py

# OBSERVAÇÃO:
# No Databricks (incluindo a Free Edition), a sessão Spark é gerenciada
# nativamente pelo runtime do cluster. O Databricks cria automaticamente
# uma SparkSession pré-configurada e a injeta no namespace global como
# a variável `spark`, disponível em notebooks e jobs. Por isso, a criação
# manual de sessão com SparkSession.builder não é necessária neste ambiente.
#
# Este módulo é mantido no projeto para fins educacionais e para permitir
# a execução do pipeline em ambientes locais (fora do Databricks), onde a
# sessão Spark precisaria ser criada manualmente.

# from pyspark.sql import SparkSession
# from config.settings import carregar_config
#
# config = carregar_config()
# SPARK_APP_NAME = config["spark"]["app_name"]
#
#
# class SparkSessionManager:
#     """
#     Gerencia a criação e o acesso à sessão Spark.
#     """
#
#     @staticmethod
#     def get_spark_session(app_name: str = SPARK_APP_NAME) -> SparkSession:
#         """
#         Cria e retorna uma sessão Spark.
#
#         :param app_name: Nome da aplicação Spark.
#         :return: Instância da SparkSession
#         """
#         return SparkSession.builder.appName(app_name).getOrCreate()
