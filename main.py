import asyncio
import websockets
import logging
import traceback
import os

logging.basicConfig(level=logging.DEBUG)

MAX_CONNECTIONS = 10000  # 最大接続数
CLIENTS = set()
OWNER = None
MESSAGE_QUEUE = asyncio.Queue()
PING_INTERVAL = 30  # pingを送る間隔（秒）

async def process_messages():
    while True:
        message = await MESSAGE_QUEUE.get()
        if OWNER:
            await broadcast_message(message)
        MESSAGE_QUEUE.task_done()

async def broadcast_message(message):
    for client in CLIENTS:
        try:
            await client.send(message)
        except websockets.exceptions.ConnectionClosed:
            pass

async def send_ping(websocket):
    while True:
        try:
            await asyncio.sleep(PING_INTERVAL)
            await websocket.ping()
        except websockets.exceptions.ConnectionClosed:
            break

async def handle_client(websocket, path):
    global OWNER
    if len(CLIENTS) >= MAX_CONNECTIONS:
        await websocket.close(1008, "Max connections reached")
        return

    ping_task = None
    try:
        # クライアントタイプの確認
        client_type_message = await websocket.recv()
        client_type, client_number = client_type_message.split(',')
        
        if client_type == '@owner':
            if OWNER:
                await websocket.close(1008, "Owner already connected")
                return
            OWNER = websocket
            await websocket.send("オーナーで接続しました。")
            logging.info(f"Owner connected: {client_number}")
        elif client_type == '@client':
            CLIENTS.add(websocket)
            await websocket.send("クライアントの接続が完了しました。")
            logging.info(f"New client connected: {client_number}")
            # クライアントに対してpingを開始
            ping_task = asyncio.create_task(send_ping(websocket))
        else:
            await websocket.close(1008, "Invalid client type")
            return

        async for message in websocket:
            if websocket == OWNER:
                await MESSAGE_QUEUE.put(message)
            else:
                # クライアントからのメッセージは無視
                pass

    except websockets.exceptions.ConnectionClosedError as e:
        logging.error(f"Connection closed: {e}")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        logging.error(traceback.format_exc())
    finally:
        if websocket == OWNER:
            OWNER = None
            logging.info("Owner disconnected")
        elif websocket in CLIENTS:
            CLIENTS.remove(websocket)
            logging.info(f"Client disconnected: {client_number}")
        if ping_task:
            ping_task.cancel()

async def main():
    port = int(os.environ.get("PORT", 8765))
    server = await websockets.serve(
        handle_client, "0.0.0.0", port, ping_interval=None, ping_timeout=240,
    )

    logging.info(f"WebSocket server started on port {port}")

    # メッセージ処理タスクを開始
    asyncio.create_task(process_messages())

    await server.wait_closed()

if __name__ == "__main__":
    asyncio.run(main())