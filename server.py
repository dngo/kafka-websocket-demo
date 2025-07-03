import asyncio
import websockets
import os
from aiohttp import web
from confluent_kafka import Consumer, KafkaException

clients = set()

# Retry Kafka consumer connection
for i in range(10):
    try:
        kafka_consumer = Consumer({
            'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"),
            'group.id': 'web-group',
            'auto.offset.reset': 'latest'
        })
        kafka_consumer.subscribe(['test-topic'])
        print("✅ Kafka connected")
        break
    except KafkaException as e:
        print(f"❌ Kafka connection failed: {e}. Retrying ({i+1}/10)...")
        time.sleep(3)

async def kafka_listener():
    while True:
        msg = kafka_consumer.poll(1.0)
        if msg is None:
            await asyncio.sleep(0.1)
            continue
        if msg.error():
            continue

        message = msg.value().decode('utf-8')
        print("📨 Kafka:", message)

        to_remove = set()
        for client in clients:
            if client.close_code is None:
                try:
                    await client.send(message)
                except Exception as e:
                    print(f" Failed to send to client: {e}")
                    to_remove.add(client)
            else:
                to_remove.add(client)
        clients.difference_update(to_remove)

async def websocket_handler(websocket):
    clients.add(websocket)
    print("🔌 WebSocket connected")
    try:
        async for _ in websocket:
            pass
    finally:
        clients.remove(websocket)
        print("🔌 WebSocket disconnected")

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
