import json
import requests
from kafka import KafkaProducer
from datetime import datetime

def get_bitcoin_price():
    try:
        response = requests.get('https://api.coinbase.com/v2/prices/spot?currency=USD')
        return float(response.json()['data']['amount'])
    except requests.RequestException as e:
        print("Error de conexión o solicitud al obtener precio:", e)
    except json.JSONDecodeError as e:
        print("Error al decodificar la respuesta JSON al obtener precio:", e)
    except ValueError as e:
        print("Error al convertir el precio a float:", e)
    except Exception as e:
        print("Error al obtener precio:", e)
        return None

def get_hash_rate():
    try:
        response = requests.get('https://api.blockchain.info/q/hashrate')
        # Devuelve en GH/s — lo convertimos a TH/s
        return float(response.text) / 1000
    except requests.RequestException as e:
        print("Error de conexión o solicitud al obtener hash rate:", e)
    except ValueError as e:
        print("Error al convertir hash rate a float:", e)
    except Exception as e:
        print("Error al obtener hash rate:", e)
        return None

def create_kafka_producer():
    return KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def build_data(price, hash_rate):
    return {
        'timestamp': datetime.utcnow().isoformat(),
        'price_usd': price,
        'hash_rate_ths': hash_rate
    }
