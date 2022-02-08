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
- `-p, --program-request-limit` — sets a maximum number of concurrent [getProgramAccounts](https://docs.solana.com/developing/clients/jsonrpc-api#getaccountinfo) requests the cacher is allowed to send to the cluster/validator.
- `-A, --account-request-queue-size` — sets a maximum number of [getAccountInfo](https://docs.solana.com/developing/clients/jsonrpc-api#getaccountinfo) requests that are allowed to wait for the permit to send the request to validator.
- `-P, --program-request-queue-size` — sets a maximum number of [getProgramAccounts](https://docs.solana.com/developing/clients/jsonrpc-api#getaccountinfo) requests that are allowed to wait for the permit to send the request to validator.
- `-b, --body-cache-size` — sets the maximum amount of cached responses.
- `-c, --websocket-connections` — sets the number of websocket connections to validator
- `-t, --time-to-live` — duration of time for which values will be kept in cache
- `-d, --slot-distance` — sets the maximum slot distance for health check purposes
- `--log-file` - file, which should be used for the output of generated logs
- `--config` - limits related configuration file in TOML format
- `--ignore-base58-limit` — flag whether to ignore base58 overflowing size limit
- `--log-format` — the format, in which to output the logs: plain | json
- `--request-timeout` - time duration, upon of elapsing of which passthrough
  requests will be aborted, and the client will be notified of request timeout,
  default is 60 seconds. Timeouts for `getAccountinfo` and `getProgramAccounts`
  requests are configured separately via configuration file.
- `--rules` — path to firewall rules written in lua
- `--identity` — optional identity key for cacherpc service, should be base58 encoded public key 
- `--control-socket-path` — path to socket file, e.g. /run/cacherpc.sock

#### Configuration file
Some configuration parameters can be loaded from TOML formatted file, and can be
re-read from it during application runtime, in order to dynamically reapply them.
Example configuration:
```toml
[rpc.request_limits]
account_info = 10 # concurrent getAccountinfo requests to validator
program_accounts = 50 # concurrent getProgramAccounts requests to validator

[rpc.request_queue_size]
account_info = 10 # number of getAccountinfo requests that can wait in queue before making request to validator
program_accounts = 10 # number of getProgramAccounts requests that can wait in queue before making request to validator

[rpc]
ignore_base58_limit = true

[rpc.timeouts]
account_info_request = 30 # timeout in seconds, before getAccountinfo is aborted
program_accounts_request = 60 # timeout in seconds, before getProgramAccounts is aborted
account_info_backoff = 30 # time duration during which getAccountinfo will be repeatedly retried, in case of failure
program_accounts_backoff = 60 # time duration during which getProgramAccounts will be repeatedly retried, in case of failure
```


#### Commands 
Running instance of caching server supports several commands that can be sent to
it via unix domain socket:
- `cache-rpc config-reload` - reload limits related configuration from file
  (must have been started with `--config <path>` option)
- `cache-rpc waf-reload` - reload WAF rules from lua file, (must have been started with `--rules <path>` option)
- `cache-rpc subscriptions off` - prevent caching server from initiating new
  subscriptions after fetching data via rpc requests
- `cache-rpc subscriptions on` - allow caching server to initiate new
  subscriptions after fetching data via rpc requests (default)
- `cache-rpc subscriptions status` - print out the current status of
  subscriptions allowance (on or off)

#### Metrics
Caching server provides various metrics, which are available in [Prometheus](https://prometheus.io/) compatible format. Metrics can be retrieved via `/metrics` HTTP endpoint.


## Features

#### Implemented methods

In the current version caching is implemented for these methods: 
- [getAccountInfo](https://docs.solana.com/developing/clients/jsonrpc-api#getaccountinfo)
- [getProgramAccounts](https://docs.solana.com/developing/clients/jsonrpc-api#getprogramaccounts)

Requests to other methods are passed through to the validator.

#### Unlikely to be implemented

Features that are unlikely to be implemented:

- Root and Single commitments.

---
***Disclaimer:*** *This project is an early stage Work-In-Progress and is not ready for production use*. 

