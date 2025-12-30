# pharos-exporter
pharos exporter for validators

## Usage

Run the `start` command with your RPC endpoint, BLS key, and log path:

```bash
go run . start \
  -rpc https://YOUR_RPC \
  -my-bls-key 0xYOUR_BLS_KEY \
  -log-path /data/pharos-node/domain/light/log/consensus.log
```

### Notes
- `-log-from-start` reads the log from the beginning; omit it to tail only new lines.
- The log tailer follows file rotation (e.g. `consensus.log` renamed to `consensus.log.x`).
- `-check-block-proof`, `-check-validator-set`, `-check-propose` and `-check-endorse` are enabled by default.

### Options
Use `-h` to see all available flags and defaults:

```bash
go run . start -h
```

Example output:
```text
Usage of start:
  -check-block-proof
        check signedBlsKeys metrics (default true)
  -check-endorse
        check endorse metrics (default true)
  -check-propose
        check propose metrics (default true)
  -check-validator-set
        check validator set metrics (default true)
  -exporter-port string
        metrics listen port (default "9123")
  -log-from-start
        start reading log from beginning (default: false)
  -log-path string
        path to log file to tail
  -log-poll-interval duration
        poll interval for log tailing (default 1s)
  -my-bls-key string
        my BLS pubkey (0x...)
  -rpc string
        JSON-RPC endpoint (default "https://atlantic-rpc.dplabs-internal.com/")
  -rpc-poll-interval duration
        poll interval for latest block (default 1s)
```
