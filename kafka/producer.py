import os
from confluent_kafka import Producer

producer = Producer({
    'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
})

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Message delivered to {msg.topic()} [{msg.partition()}]")

def send_message(topic: str, message: str):
    producer.produce(topic, message.encode("utf-8"), callback=delivery_report)
    producer.flush()
