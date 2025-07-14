import os
import time
from confluent_kafka import Consumer, KafkaException

consumer = None  # Will be set by init_consumer()

# if multiple consumers share the same group.id
# only 1 of them will get the message
def init_consumer(group_id: str):
    global consumer
    for i in range(10):
        try:
            consumer = Consumer({
                'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
                'group.id': group_id,
                'auto.offset.reset': 'latest'
            })
            print(f"✅ Kafka connected (group.id = {group_id})")
            break
        except KafkaException as e:
            print(f"❌ Kafka connection failed: {e}. Retrying ({i+1}/10)...")
            time.sleep(3)

def subscribe_to_topic(topic: str):
    consumer.subscribe([topic])
    print(f"🔁 Subscribed to Kafka topic: {topic}")

def poll_message():
    return consumer.poll(1.0)

def close_consumer():
    consumer.close()
