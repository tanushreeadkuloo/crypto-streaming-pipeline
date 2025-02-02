from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Crypto Stream Processing") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

# Kafka Configuration
kafka_brokers = "localhost:9092"
kafka_topic = "cryptos"

# InfluxDB Configuration
token = "i2aSCCXdqLLnVHlFiGsMwWsQ9nK9jVr16Yk0J7HuZ7dCqQ4ynmVR6njIPzlE5_pIyor6L1pmnrspUCZbg6l8Yw=="
org = "Streaming Data"
bucket = "crypto"
influx_client = InfluxDBClient(url="http://localhost:8086", token=token, org=org)
write_api = influx_client.write_api(write_options=SYNCHRONOUS)

# Define schema for parsing
crypto_schema = StructType([
    StructField("currency", StringType(), True),
    StructField("price", DoubleType(), True)
])

# Create streaming DataFrame from Kafka
kafka_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", kafka_brokers) \
    .option("subscribe", kafka_topic) \
    .load()

# Parse the 'value' column, split it by commas, and create a DataFrame with 'currency' and 'price'
parsed_df = kafka_df.select(
    split(col("value").cast("string"), ",").alias("data")  # Split the CSV string
).select(
    col("data").getItem(0).alias("currency"),  # First element is currency
    col("data").getItem(1).cast(DoubleType()).alias("price")  # Second element is price, cast to Double
)

# Define a function to write to InfluxDB
def write_to_influxdb(df, epoch_id):
    """
    Function to process each batch of data and write to InfluxDB.
    """
    def save_row(row):
        try:
            if row.currency is None or row.price is None:
                print(f"Skipping invalid row: {row}")
                return
            point = Point("cryptos") \
                .tag("currency", row.currency) \
                .field("close", row.price)
            write_api.write(bucket=bucket, org=org, record=point)
            print(f"Processed: {row.currency} = {row.price}")
        except Exception as e:
            print(f"Error processing row {row}: {e}")
    
    # Process each row in the batch
    df.foreach(save_row)

# Write stream to InfluxDB
query = parsed_df.writeStream \
    .foreachBatch(write_to_influxdb) \
    .trigger(processingTime="60 seconds") \
    .start()

# Await termination of the streaming query
query.awaitTermination()