import json
import time
from kafka import KafkaProducer
from datetime import datetime
from urls import URLs

class Producer:
    def __init__(self, topic='btc-stream', bootstrap_servers='localhost:9092'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.topic = topic
        self.urls = URLs()

    def run(self):
        print("Productor iniciado. Enviando datos a Kafka...")
        try:
            while True:
                price = self.urls.get_bitcoin_price()
                hash_rate = self.urls.get_hash_rate()
                timestamp = datetime.utcnow().isoformat()

                if price is not None and hash_rate is not None:
                    data = {
                        'timestamp': timestamp,
                        'price_usd': price,
                        'hash_rate_ths': hash_rate
                    }
                    self.producer.send(self.topic, value=data)
                    print(f"Enviado: {data}")
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nInterrupción detectada. Cerrando productor...")
            self.producer.close()
