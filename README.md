# falkordb-utils

Small utility scripts for working with FalkorDB.

## Scripts

### `scripts/wait-for-falkordb.sh`

Waits for a FalkorDB server to start accepting TCP connections.

Usage:

```bash
./scripts/wait-for-falkordb.sh [host] [port] [timeout_seconds]
```

Defaults:

- `host`: `127.0.0.1`
- `port`: `6379`
- `timeout_seconds`: `30`

You can also use environment variables:

```bash
FALKORDB_HOST=127.0.0.1 FALKORDB_PORT=6379 FALKORDB_TIMEOUT=30 ./scripts/wait-for-falkordb.sh
```
