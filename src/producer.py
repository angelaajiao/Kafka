import json
import time
import requests
from kafka import KafkaProducer
from datetime import datetime

def get_bitcoin_price():
    try:
        response = requests.get('https://api.coinbase.com/v2/prices/spot?currency=')
        return float(response.json()['data']['amount'])
    except Exception as e:
        print("Error al obtener precio:", e)
        return None

def get_hash_rate():
    try:
        response = requests.get('https://api.blockchain.info/q/hashrate')
        # Devuelve en GH/s — lo convertimos a TH/s
        return float(response.text) / 1000
    except Exception as e:
        print("Error al obtener hash rate:", e)
        return None

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = 'btc-stream'

print("Productor iniciado. Enviando datos a Kafka...")

while True:
    price = get_bitcoin_price()
    hash_rate = get_hash_rate()
    timestamp = datetime.utcnow().isoformat()

    if price is not None and hash_rate is not None:
        data = {
            'timestamp': timestamp,
            'price_usd': price,
            'hash_rate_ths': hash_rate
        }

        producer.send(topic, value=data)
        print(f"📤 Enviado: {data}")

    time.sleep(1)  # Espera 1 segundo
