from session.spark_session import SparkSessionManager
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSessionManager.get_spark_session()

tesouro_transparente_data = spark.read.option("header", "true") \
                                    .option("inferSchema", "true") \
                                    .option("sep", ";") \
                                    .csv("s3://tesouro-transparente-pipeline/bronze/")

tesouro_transparente_data.printSchema()