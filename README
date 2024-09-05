# All Chains Fit on Eigen

## Overview

"All Chains Fit on Eigen" is a groundbreaking demonstration of EigenDA's unparalleled Data Availability (DA) capabilities. This project showcases EigenDA's industry-leading throughput of 15 Mbps by ingesting and dispersing block data from 15+ blockchain networks in real-time.

## What is EigenDA?

EigenDA is a high-performance Data Availability solution designed to support the scalability needs of blockchain ecosystems. With its exceptional throughput, EigenDA enables the creation and operation of high-performing rollups and other Layer 2 solutions.

## Project Description

This project continuously monitors multiple blockchain networks, including Ethereum Layer 2s, alternative Layer 1s, and other prominent chains such as Solana and Bitcoin. For each new block generated on these networks, we:

1. Fetch the full block data
2. Eencode the data
3. Disperse the data through EigenDA
4. Log metadata about the dispersed blocks

By processing more than 10 MB of data per second and dispersing over 20 blobs per second, this project demonstrates EigenDA's capacity to handle the combined throughput of multiple high-activity blockchain networks simultaneously.

## Supported Chains

The project currently supports the following chains (and more):

- Ethereum Layer 2s: Optimism, Arbitrum, Base, zkSync, Taiko, Mantle, Scroll
- Alternative Layer 1s: Avalanche, BNB Chain, Polygon, Fantom
- Other prominent chains: Solana, Ethereum, Bitcoin

## Technical Details

The project consists of several Python scripts, each dedicated to handling specific chains or groups of chains:

- `evm_da.py`: Handles EVM-compatible chains
- `poa_evmda.py`: Handles POA EVM-compatible chains
- `solda.py`: Processes Solana blocks
- `btcda.py`: Manages Bitcoin block ingestion
- `celda.py`: Handles Celestia block processing

These scripts utilize WebSocket connections to subscribe to new blocks, fetch full block data, compress it using Snappy, and disperse it through EigenDA using gRPC calls.

## Requirements

See `requirements.txt` for a full list of dependencies. Key requirements include:

- Python 3.7+
- `web3` for interacting with EVM chains
- `grpcio` and `protobuf` for EigenDA communication
- `psycopg2` for PostgreSQL database interactions
- `python-snappy` for data compression

## Setup and Usage

1. Clone the repository
2. Install dependencies: `pip install -r requirements.txt`
3. Set up environment variables (see `.env.example`)
4. Run the desired script(s), e.g., `python evm_da.py`