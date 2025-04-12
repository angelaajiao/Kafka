import time
from sqlite3 import DataError
from bitcoin_data import get_bitcoin_price, get_hash_rate, create_kafka_producer, build_data

producer = create_kafka_producer()
topic = 'btc-stream'

print("Productor iniciado. Enviando datos a Kafka...")

while True:
    price = get_bitcoin_price()
    hash_rate = get_hash_rate()

    if price is not None and hash_rate is not None:
        data = build_data(price, hash_rate)

        try:
            # Enviar datos a Kafka
            producer.send(topic, value=data)
            print(f"Enviado: {data}")
        except DataError as e:
            print(f"Error al enviar datos a Kafka: {e}")
        except Exception as e:
            print(f"Error inesperado al enviar datos a Kafka: {e}")
        except KeyboardInterrupt:
            print("\nInterrupción del teclado recibida. Cerrando el productor de Kafka...")
            producer.close()
            print("Productor de Kafka cerrado. Programa terminado.")

    time.sleep(1)  # Espera 1 segundo
