from pyspark.sql import SparkSession, Row
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType
)
from pyspark.sql.functions import lit, concat, col, split
from pyspark.streaming import StreamingContext

scala_version = '2.12.20'
spark_version = '3.5.1'

packages = [
    
f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.4.0'
]

spark = SparkSession.builder \
    .master("local") \
    .appName("KafkaSparkIntegration") \
    .config("spark.jars.packages", ",".join(packages)) \
    .getOrCreate()

print("Spark Session created with the following configurations:")
for key, value in spark.sparkContext.getConf().getAll():
    print(f"{key} = {value}")
