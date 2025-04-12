import json
from kafka import KafkaConsumer
from collections import deque

def create_consumer(topic, group_id='btc-visualizer'):
    return KafkaConsumer(
        topic,
        bootstrap_servers='localhost:9092',
        group_id=group_id,
        auto_offset_reset='latest',
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )

def init_data_structures(max_points):
    timestamps = deque(maxlen=max_points)
    prices = deque(maxlen=max_points)
    hash_rates = deque(maxlen=max_points)
    return timestamps, prices, hash_rates
