from session.spark_session import SparkSessionManager
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSessionManager.get_spark_session()

bronze_path = "/Volumes/workspace/tesouro-transparente-pipeline/bronze"

tesouro_transparente_bronze = (
    spark.read.format("parquet")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("sep", "true")
    .load(bronze_path)
)

display(tesouro_transparente_bronze)