import asyncio
import websockets
from aiohttp import web
from urllib.parse import urlparse, parse_qs
from collections import defaultdict

from kafka.consumer import init_consumer, poll_message, subscribe_to_topic
from kafka.producer import send_message

websocket_connections = defaultdict(set)

init_consumer(group_id="web-group-1")

def get_topic(websocket):
    query_params = parse_qs(urlparse(websocket.request.path).query)
    return f"{query_params['topic'][0]}_{query_params['id'][0]}"

async def kafka_listener():
    while True:
        msg = poll_message()
        if msg is None:
            await asyncio.sleep(0.1)
            continue
        if msg.error():
            continue

        topic = msg.topic()
        message = msg.value().decode("utf-8")
        print("📨 Kafka:", message)

        to_remove = set()
        for client in websocket_connections[topic]:
            if client.close_code is None:
                try:
                    await client.send(message)
                except Exception as e:
                    print(f"❌ Failed to send to client: {e}")
                    to_remove.add(client)
            else:
                to_remove.add(client)
        websocket_connections[topic].difference_update(to_remove)

async def websocket_handler(websocket):
    topic = get_topic(websocket)
    if topic not in websocket_connections:
        subscribe_to_topic(topic)
    websocket_connections[topic].add(websocket)

    print("🔌 WebSocket connected", topic)
    try:
        await websocket.send('websocket connected ' + websocket.request.path)
        async for message in websocket:
            send_message(topic, message)
    except Exception as e:
        print(f"WebSocket error: {e}")
    finally:
        websocket_connections[topic].remove(websocket)
        print("🔌 WebSocket disconnected", topic)

async def index(request):
    return web.FileResponse('./index1.html')

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