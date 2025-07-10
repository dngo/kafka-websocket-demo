import asyncio
import websockets
import os
from aiohttp import web
from confluent_kafka import Consumer, KafkaException
from urllib.parse import urlparse, parse_qs
from collections import defaultdict

TOPIC = "test-topic"
websocket_connections = defaultdict(set)

# Retry Kafka consumer connection
for i in range(10):
    try:
        kafka_consumer = Consumer({
            'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
            'group.id': 'web-group',
            'auto.offset.reset': 'latest'
        })
        print("✅ Kafka connected")
        break
    except KafkaException as e:
        print(f"❌ Kafka connection failed: {e}. Retrying ({i+1}/10)...")
        time.sleep(3)

def update_kafka_subscription(topic):
    kafka_consumer.subscribe([topic])
    print(f"🔁 Subscribed to Kafka topics: {topic}")

def get_topic(websocket):
    query_params = parse_qs(urlparse(websocket.request.path).query)
    return f"{query_params['topic'][0]}_{query_params['id'][0]}"

async def kafka_listener():
    while True:
        msg = kafka_consumer.poll(1.0)
        if msg is None:
            await asyncio.sleep(0.1)
            continue
        if msg.error():
            continue

        topic = msg.topic()
        message = msg.value().decode('utf-8')
        print("📨 Kafka:", message)

        to_remove = set()
        for client in websocket_connections[topic]:
            if client.close_code is None:
                try:
                    await client.send(message)
                except Exception as e:
                    print(f" Failed to send to client: {e}")
                    to_remove.add(client)
            else:
                print("removing client due to close code")
                to_remove.add(client)
        websocket_connections[topic].difference_update(to_remove)

async def websocket_handler(websocket):
    topic = get_topic(websocket)
    if topic not in websocket_connections: update_kafka_subscription(topic)
    websocket_connections[topic].add(websocket)

    print("🔌 WebSocket connected ", websocket.id, topic)
    try:
        await websocket.send('websocket connected' + websocket.request.path)
        async for _ in websocket:
            pass  # Handle messages here if needed
    except Exception as e:
        print(f"WebSocket error: {e}", websocket.id, topic)
    finally:
        websocket_connections[topic].remove(websocket)
        print("🔌 WebSocket disconnected ", websocket.id, topic)

async def index(request):
    return web.FileResponse('./index.html')

async def start_servers():
    ws_server = websockets.serve(websocket_handler, '0.0.0.0', 8765)

    app = web.Application()
    app.router.add_get('/', index)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 8080)

    await asyncio.gather(ws_server, site.start(), kafka_listener())

if __name__ == "__main__":
    asyncio.run(start_servers())
