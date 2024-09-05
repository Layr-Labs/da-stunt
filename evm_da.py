import os
import snappy
import grpc
import json
import asyncio
import psycopg2
import websockets
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
from web3 import Web3

import disperser_pb2
import disperser_pb2_grpc

load_dotenv()
GRPC_ENDPOINT = os.getenv("GRPC_ENDPOINT")
ACCOUNT = os.getenv("ACCOUNT")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", 5432)
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")  # Database name

CHUNK_SIZE = 31  # 0 prefix byte leaves 31 bytes for data

executor = ThreadPoolExecutor(max_workers=200)  # Adjust the number of workers as needed

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

def to_serializable(val):
    if isinstance(val, bytes):
        return val.hex()
    elif isinstance(val, datetime):
        return val.isoformat()
    return str(val)

def disperse_blob(request):
    channel = grpc.secure_channel(GRPC_ENDPOINT, grpc.ssl_channel_credentials())
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

def log_block_info_to_db(chain_name, block_number, block_size, request_id):
    try:
        connection = connect_db()
        cursor = connection.cursor()
        
        # Create table if it doesn't exist
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {chain_name} (
            id SERIAL PRIMARY KEY,
            block_number BIGINT NOT NULL,
            block_size BIGINT NOT NULL,
            request_id TEXT NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL
        );
        """

        cursor.execute(create_table_query)

        # Insert log entry into the respective chain table
        insert_query = f"""
        INSERT INTO {chain_name} (block_number, block_size, request_id, timestamp)
        VALUES (%s, %s, %s, %s);
        """
        cursor.execute(insert_query, (
            block_number,
            block_size,
            request_id,
            datetime.now(timezone.utc).isoformat()
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

async def process_block(chain_name, block_info, w3):
    block_number = int(block_info["number"], 16)  # Convert hex block number to int

    try:
        # Fetch the full block
        block_data = w3.eth.get_block(block_number, full_transactions=True)
        json_data = json.dumps(dict(block_data), default=to_serializable).encode("utf-8")
        json_size = len(json_data) / 1024 / 1024
        snappy_data = chunk_encode(json_data)
        snappy_size = len(snappy_data) / 1024 / 1024

        print(f"Block {block_number} json:{json_size:.1f}MiB snappy:{snappy_size:.1f}MiB")

        request = disperser_pb2.DisperseBlobRequest(data=snappy_data, account_id=ACCOUNT)

        # Create an async task to disperse the blob and log it without waiting
        asyncio.create_task(disperse_and_log(chain_name, request, block_number, len(snappy_data)))

        # Log chain info
        log_chain_info_to_db(chain_name, len(snappy_data))  # Update this line
    except Exception as e:
        print(f"Error processing block {block_number} for {chain_name}: {e}")

async def disperse_and_log(chain_name, request, block_number, block_size):
    try:
        # Run the disperse_blob in the executor
        response = await asyncio.get_event_loop().run_in_executor(executor, disperse_blob, request)

        # Log block information to the database
        request_id = response.request_id.decode('utf-8')
        log_block_info_to_db(chain_name, block_number, block_size, request_id)
    except Exception as e:
        print(f"Error in disperse_and_log for block {block_number} on {chain_name}: {e}")

async def subscribe_to_blocks(chain_name, rpc_endpoint, ws_endpoint):
    w3 = Web3(Web3.HTTPProvider(rpc_endpoint))
    uri = ws_endpoint
    while True:
        try:
            print(f"Connecting to WebSocket for {chain_name}...")
            async with websockets.connect(uri, max_size=None) as websocket:
                subscribe_message = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "eth_subscribe",
                    "params": ["newHeads"]
                }
                await websocket.send(json.dumps(subscribe_message))
                print(f"Subscription message sent for {chain_name}...")

                while True:
                    message = await websocket.recv()
                    data = json.loads(message)

                    if "params" in data:
                        block_info = data["params"]["result"]
                        await process_block(chain_name, block_info, w3)

        except websockets.exceptions.ConnectionClosedError as e:
            print(f"Connection closed for {chain_name}, retrying in 5 seconds: {e}")
            await asyncio.sleep(5)  # Wait for 5 seconds before retrying
        except Exception as e:
            print(f"Unexpected error for {chain_name}: {e}")
            await asyncio.sleep(5)  # Wait for 5 seconds before retrying

async def main():
    chains = [
        {
            "name": "Optimism",
            "rpc_endpoint": "",
            "ws_endpoint": ""
        },
        {
            "name": "Arbitrum",
            "rpc_endpoint": "",
            "ws_endpoint": ""
        },
        {
            "name": "Base",
            "rpc_endpoint": "",
            "ws_endpoint": ""
        },
        {
            "name": "Fantom",
            "rpc_endpoint": "",
            "ws_endpoint": ""
        },
        {
            "name": "Mantle",
            "rpc_endpoint": "",
            "ws_endpoint": ""
        },
        {
            "name": "Blast",
            "rpc_endpoint": "",
            "ws_endpoint": ""
        },
        {
            "name": "Taiko",
            "rpc_endpoint": "",
            "ws_endpoint": ""
        },
        {
            "name": "zkSync",
            "rpc_endpoint": "",
            "ws_endpoint": ""
        },
        {
            "name": "Xai",
            "rpc_endpoint": "",
            "ws_endpoint": ""
        },
        {
            "name": "Mode",
            "rpc_endpoint": "",
            "ws_endpoint": ""
        }
    ]

    tasks = []
    for chain in chains:
        tasks.append(subscribe_to_blocks(chain["name"], chain["rpc_endpoint"], chain["ws_endpoint"]))

    await asyncio.gather(*tasks)

if __name__ == '__main__':
    asyncio.run(main())