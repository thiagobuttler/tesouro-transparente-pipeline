from session.spark_session import SparkSessionManager
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSessionManager.get_spark_session()

