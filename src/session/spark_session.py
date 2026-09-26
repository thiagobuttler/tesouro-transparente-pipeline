from pyspark.sql import SparkSession

class SparkSessionManager:

    @staticmethod
    def get_spark_session(app_name: str = "tesouro-transparente-pipeline") -> SparkSession:

        return SparkSession.builder.appName(app_name).getOrCreate()