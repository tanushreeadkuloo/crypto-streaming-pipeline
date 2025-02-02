# Crypto Streaming Data Pipeline
This project implements a real-time data pipeline for cryptocurrency data using Kafka, Spark Structured Streaming, and InfluxDB. The pipeline ingests, processes, and stores crypto data, and real-time trends are visualized using Grafana.

## Technologies Used
- Apache Kafka
- Apache Spark (Structured Streaming)
- InfluxDB
- Grafana

## Setup Instructions
1. Clone this repository.
2. Install dependencies: `pip install -r requirements.txt`.
3. Start Zookeeper and Kafka services.
4. Run the pipeline: 'crypto_to_kafka.py', 'kafka_spark_integration.py', 'influx_client.py' and 'crypto_stream.py'
5. Add a new data source in Grafana
6. Create a dashboard and visualize the data in Grafana 
