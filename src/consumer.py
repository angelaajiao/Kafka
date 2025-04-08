from kafka import KafkaConsumer
import sys

if len(sys.argv) < 2:
    print("Enter topic name")
    sys.exit(1)

topic_name = sys.argv[1]

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers='localhost:9092',
    group_id='test',
    enable_auto_commit=True,
    auto_offset_reset='earliest',
    key_deserializer=lambda k: k.decode('utf-8') if k else None,
    value_deserializer=lambda v: v.decode('utf-8')
)

print(f"Subscribed to topic {topic_name}")

for message in consumer:
    print(f"offset = {message.offset}, key = {message.key}, value = {message.value}")
