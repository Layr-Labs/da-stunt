import os
import snappy
import grpc
import json
import asyncio
import psycopg2
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
import requests

import disperser_pb2
import disperser_pb2_grpc

load_dotenv()
GRPC_ENDPOINT = os.getenv("GRPC_ENDPOINT")
ACCOUNT = os.getenv("ACCOUNT")
BITCOIN_RPC_URL = os.getenv("BITCOIN_RPC_URL")  # Your Bitcoin node RPC URL
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", 5432)
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")  # Database name

CHUNK_SIZE = 31  # 0 prefix byte leaves 31 bytes for data
executor = ThreadPoolExecutor(max_workers=4)  # Adjust the number of workers as needed

def chunk_encode(data):
    length_prefix = len(data).to_bytes(4, "big")
    total_length = len(length_prefix) + len(data)
    padding_length = (CHUNK_SIZE - (total_length % CHUNK_SIZE)) % CHUNK_SIZE
    padded_bytes = length_prefix + data + b"\x00" * padding_length
    chunks = [
        padded_bytes[i: i + CHUNK_SIZE]
        for i in range(0, len(padded_bytes), CHUNK_SIZE)
    ]

    prefixed_chunks = [b"\x00" + chunk for chunk in chunks]

    return b"".join(prefixed_chunks)

def chunk_decode(data):
    if len(data) % (CHUNK_SIZE + 1) != 0:
        raise ValueError(f"Data length is not a multiple of (CHUNK_SIZE + 1).")

    combined_bytes = b"".join(
        [data[i + 1: i + 1 + CHUNK_SIZE] for i in range(0, len(data), CHUNK_SIZE + 1)]
    )
    length_prefix = combined_bytes[:4]
    json_length = int.from_bytes(length_prefix, "big")
    return combined_bytes[4: 4 + json_length]



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

def log_block_info_to_db(block_height, block_size, request_id):
    try:
        connection = connect_db()
        cursor = connection.cursor()
        
        log_entry = {
            "block_height": block_height,
            "block_size": block_size,
            "request_id": request_id,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        insert_query = """
        INSERT INTO Bitcoin (block_height, block_size, request_id, timestamp)
        VALUES (%s, %s, %s, %s);
        """
        cursor.execute(insert_query, (
            log_entry["block_height"],
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

async def get_latest_block():
    try:
        headers = {
            'Content-Type': 'application/json',
        }

        data = json.dumps({"jsonrpc": "1.0", "id": "curltest", "method": "getbestblockhash", "params": []})
        response = requests.post(BITCOIN_RPC_URL, headers=headers, data=data)
        best_block_hash = response.json()['result']

        data = json.dumps({"jsonrpc": "1.0", "id": "curltest", "method": "getblock", "params": [best_block_hash, 0]})
        response = requests.post(BITCOIN_RPC_URL, headers=headers, data=data)
        block_hex = response.json()['result']
        block_bytes = bytes.fromhex(block_hex)
        
        data = json.dumps({"jsonrpc": "1.0", "id": "curltest", "method": "getblockheader", "params": [best_block_hash, True]})
        response = requests.post(BITCOIN_RPC_URL, headers=headers, data=data)
        block_header = response.json()['result']

        return block_bytes, block_header
    except Exception as e:
        print(f"Error fetching the latest block: {e}")
        return None, None

async def process_block(block_bytes, block_header):
    try:
        block_height = block_header["height"]
        block_size = len(block_bytes)

        snappy_data = chunk_encode(snappy.compress(block_bytes))
        snappy_size = len(snappy_data) / 1024 / 1024

        print(f"Block {block_height} raw:{block_size/1024/1024:.1f}MiB snappy:{snappy_size:.1f}MiB")

        # Validate block encode/decode
        assert snappy.decompress(chunk_decode(snappy_data)) == block_bytes

        request = disperser_pb2.DisperseBlobRequest(data=snappy_data, account_id=ACCOUNT)
        response = await asyncio.get_event_loop().run_in_executor(executor, disperse_blob, request)

        # Log block information
        request_id = response.request_id.decode('utf-8')
        log_block_info_to_db(block_height, len(snappy_data), request_id)

        # Log chain info
        log_chain_info_to_db("Bitcoin", len(snappy_data))  # Log the data written

    except grpc.RpcError as e:
        if e.code() == grpc.StatusCode.INVALID_ARGUMENT:
            print(f"Error dispersing block {block_height}: {e.details()}")
        else:
            print(f"Unexpected error dispersing block {block_height}: {e}")
    except Exception as e:
        print(f"Error processing block {block_height}: {e}")

async def main():
    while True:
        block_bytes, block_header = await get_latest_block()
        if block_bytes and block_header:
            await process_block(block_bytes, block_header)
        await asyncio.sleep(600)  # Wait for 10 minutes before checking for new blocks

if __name__ == '__main__':
    asyncio.run(main())