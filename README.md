# Solana JSON-RPC caching server

___Disclaimer:___ _This project is an early stage Work-In-Progress and is not ready for production use_. 

This cache server implementation aims to provide a general solution for both offloading Solana validator RPC service and improving the overall speed and stability of the RPC. It achieves it by caching and updating some of the heaviest and most frequent requests and keeps the requested info updated with the use of PubSub API.

The server itself is a singe binary which is designed to be deployed in front of the validator as its public RPC entrypoint. 

## Running the server

To build and run the server you will need the Cargo package manager installed, which comes together with Rust compiler. Those two can be obtained [here](https://www.rust-lang.org/learn/get-started) by following "Installing Rust" guideline.

```bash
# build
$ cargo build --release
# run
./target/release/rpccache
```

#### Configuration

The server supports a number of configuration options, which are the following:

- `-r, --rpc-api-url` — validator or cluster JSON-RPC HTTP endpoint.
- `-w, --websocket-url` — validator or cluster PubSub endpoint.
- `-l, --listen` — cache server bind address.
- `-a, --account-request-limit` — sets a maximum number of concurrent [getAccountInfo](https://docs.solana.com/developing/clients/jsonrpc-api#getaccountinfo) requests the cache is allowed to send to the cluster/validator.
- `-p, --program-request-limit` — sets a maximum number of concurrent [getProgramAccounts](https://docs.solana.com/developing/clients/jsonrpc-api#getaccountinfo) requests the cache is allowed to send to the cluster/validator.
- `-b, --body-cache-size` — sets the maximum amount of cached responses.

## Features

#### Implemented methods

In the current version caching is implemented for these methods: 
- [getAccountInfo](https://docs.solana.com/developing/clients/jsonrpc-api#getaccountinfo)
- [getProgramAccounts](https://docs.solana.com/developing/clients/jsonrpc-api#getprogramaccounts)

Requests to other methods are passed through to the validator.

Please note that `"encoding": "jsonParsed"` is not yet supported.

#### Unlikely to be implemented

Features that are unlikely to be implemented:

- Root and Single commitments.

---
***Disclaimer:*** *This project is an early stage Work-In-Progress and is not ready for production use*. 

