import os
import snappy
import grpc
import json
import asyncio
import psycopg2
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
import websockets

import disperser_pb2
import disperser_pb2_grpc

from starknet_py.net.full_node_client import FullNodeClient

load_dotenv()
GRPC_ENDPOINT = os.getenv("GRPC_ENDPOINT")
ACCOUNT = os.getenv("ACCOUNT")
STARKNET_WS_URL = "wss://docs-demo.strk-mainnet.quiknode.pro/ws"
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", 5432)
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")  # Database name

CHUNK_SIZE = 31  # 0 prefix byte leaves 31 bytes for data
# Adjust the number of workers as needed
executor = ThreadPoolExecutor(max_workers=4)


def chunk_encode(data):
    length_prefix = len(data).to_bytes(4, "big")
    total_length = len(length_prefix) + len(data)
    padding_length = (CHUNK_SIZE - (total_length % CHUNK_SIZE)) % CHUNK_SIZE
    padded_bytes = length_prefix + data + b"\x00" * padding_length
    chunks = [
        padded_bytes[i: i + CHUNK_SIZE]
        for i in range(0, len(padded_bytes), CHUNK_SIZE)
    ]
    # Set first byte to 0 to ensure data is within valid range of bn254 curve
    prefixed_chunks = [b"\x00" + chunk for chunk in chunks]
    return b"".join(prefixed_chunks)


def disperse_blob(request):
    channel = grpc.secure_channel(
        GRPC_ENDPOINT, grpc.ssl_channel_credentials())
    client = disperser_pb2_grpc.DisperserStub(channel)
    response = client.DisperseBlob(request)
    return response


def connect_db():
    connection = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME
    )
    return connection


def log_block_info_to_db(block_number, block_size, request_id):
    try:
        connection = connect_db()
        cursor = connection.cursor()

        log_entry = {
            "block_number": block_number,
            "block_size": block_size,
            "request_id": request_id,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

        insert_query = """
        INSERT INTO Starknet (block_number, block_size, request_id, timestamp)
        VALUES (%s, %s, %s, %s);
        """
        cursor.execute(insert_query, (
            log_entry["block_number"],
            log_entry["block_size"],
            log_entry["request_id"],
            log_entry["timestamp"]
        ))

        connection.commit()
        cursor.close()
        connection.close()

    except Exception as e:
        print(f"Error logging block info to database: {e}")


def log_chain_info_to_db(chain_name, data_posted):
    try:
        connection = connect_db()
        cursor = connection.cursor()

        insert_query = """
        INSERT INTO ChainInfo (chain_name, data_posted)
        VALUES (%s, %s)
        ON CONFLICT (chain_name) DO UPDATE SET
            data_posted = ChainInfo.data_posted + EXCLUDED.data_posted;
        """
        cursor.execute(insert_query, (chain_name, data_posted))

        connection.commit()
        cursor.close()
        connection.close()
    except Exception as e:
        print(f"Error logging chain info to database: {e}")


async def process_block(block_info):
    block_number = block_info['number']

    try:
        json_data = json.dumps(block_info).encode("utf-8")
        json_size = len(json_data) / 1024 / 1024
        snappy_data = chunk_encode(json_data)
        snappy_size = len(snappy_data) / 1024 / 1024
        print(json_size)

        print(
            f"Block {block_number} json:{json_size:.1f}MiB snappy:{snappy_size:.1f}MiB")

        request = disperser_pb2.DisperseBlobRequest(
            data=snappy_data, account_id=ACCOUNT)
        response = await asyncio.get_event_loop().run_in_executor(executor, disperse_blob, request)

        # Log block information
        request_id = response.request_id.decode('utf-8')
        print(f"request_id {request_id}")

        log_block_info_to_db(block_number, len(snappy_data), request_id)

        # Log chain info
        log_chain_info_to_db("Starknet", len(snappy_data))

    except grpc.RpcError as e:
        if e.code() == grpc.StatusCode.INVALID_ARGUMENT:
            print(f"Error dispersing block : {e.details()}")
        else:
            print(f"Unexpected error dispersing block : {e}")
    except Exception as e:
        print(f"Error processing block : {e}")


async def subscribe_to_blocks():
    while True:
        try:
            async with websockets.connect(STARKNET_WS_URL, max_size=None) as websocket:
                subscribe_message = {
                    "id": 1,
                    "jsonrpc": "2.0",
                    "method": "pathfinder_subscribe",
                    "params": {
                        "kind": "newHeads"
                    }
                }
                await websocket.send(json.dumps(subscribe_message))

                while True:
                    message = await websocket.recv()
                    data = json.loads(message)

                    if "result" in data:
                        block_info = data["result"]
                        try:
                            block_header = block_info["result"]
                            await process_block(block_header)
                        except (TypeError, KeyError) as e:
                            print(
                                f"Non-dict result or missing key: {block_info}, Error: {e}")
                    else:
                        print(f"Unexpected response: {data}")

                    await asyncio.sleep(0.3)

        except websockets.exceptions.ConnectionClosedError as e:
            print(f"Connection closed, retrying in 5 seconds: {e}")
            await asyncio.sleep(5)
        except Exception as e:
            print(f"Unexpected error: {e}")
            await asyncio.sleep(5)

if __name__ == '__main__':
    asyncio.run(subscribe_to_blocks())
