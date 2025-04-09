import json
from kafka import KafkaConsumer

class Consumer:
    def __init__(self, topic='btc-stream', bootstrap_servers='localhost:9092'):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id='btc-visualizer',
            auto_offset_reset='latest',
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )

    def get_data(self):
        try:
            return next(self.consumer).value
        except Exception as e:
            print(f"Error al consumir datos: {e}")
            return None
