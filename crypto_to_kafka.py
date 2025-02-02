import os
import time
import yfinance as yf
import numpy as np

while True:
    for crypt in ['BTC-USD', 'ETH-USD', 'XRP-USD', 'USDT-USD', 'BNB-USD', 'USDC-USD', 
'SOL-USD', 'DOGE-USD', 'ADA-USD', 'STETH-USD']:
        data = yf.download(tickers=crypt, period='1d', interval='1m')
        data = data.tail(1)
        data["Type"] = crypt
        data = data[["Type", "Close"]]
        print(data.info())
        csv_file = f"./data/current_{crypt.replace('-', '_')}.csv"
        data.to_csv(csv_file, index=False, header=True)
        print(type(data))
        os.system(f"/Users/tanushreeadkuloo/Downloads/kafka_2.12-3.9.0/bin/kafka-console-producer.sh "
                  f"--broker-list localhost:9092 --topic cryptos < {csv_file}")
    time.sleep(30)
