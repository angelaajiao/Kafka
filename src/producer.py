from kafka import KafkaProducer
import sys

if len(sys.argv) < 2:
    print("Enter topic name")
    sys.exit(1)

topic_name = sys.argv[1]

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    acks='all',
    retries=0,
    batch_size=16384,
    linger_ms=1,
    buffer_memory=33554432,
    key_serializer=lambda k: k.encode('utf-8'),
    value_serializer=lambda v: v.encode('utf-8')
)

for i in range(10):
    key = str(i)
    value = str(i)
    producer.send(topic_name, key=key, value=value)
    print("Message sent successfully")

producer.close()
