from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import os

class InfluxClient:
    def __init__(self, url, token, org, bucket):
        self._org = org
        self._bucket = bucket
        self._client = InfluxDBClient(url=url, token=token)

    def write_data(self, data, write_option=SYNCHRONOUS):
        try:
            write_api = self._client.write_api(write_options=write_option)
            write_api.write(self._bucket, self._org, data)
            print("Data written successfully!")
        except Exception as e:
            print(f"Error writing data: {e}")

# Credentials for InfluxDB (Use environment variables for security)
token = os.getenv("INFLUXDB_TOKEN", "i2aSCCXdqLLnVHlFiGsMwWsQ9nK9jVr16Yk0J7HuZ7dCqQ4ynmVR6njIPzlE5_pIyor6L1pmnrspUCZbg6l8Yw==")
url = os.getenv("INFLUXDB_URL", "http://localhost:8086")
org = "Streaming Data"
bucket = "crypto"

IC = InfluxClient(url, token, org, bucket)

# Example save_row function
def save_row(r):
    # Assume r is a dictionary; transform it to a Point
    point = Point("crypto_data").field("value", r.get("value")).time(r.get("time"))
    IC.write_data(point)

print("InfluxClient script executed successfully!")
