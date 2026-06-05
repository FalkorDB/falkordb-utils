#!/usr/bin/env bash

set -euo pipefail

host="${1:-${FALKORDB_HOST:-127.0.0.1}}"
port="${2:-${FALKORDB_PORT:-6379}}"
timeout="${3:-${FALKORDB_TIMEOUT:-30}}"

if ! [[ "$port" =~ ^[0-9]+$ ]] || ! [[ "$timeout" =~ ^[0-9]+$ ]]; then
  echo "port and timeout must be numeric" >&2
  exit 1
fi

end_time=$((SECONDS + timeout))

while (( SECONDS < end_time )); do
  if (echo >"/dev/tcp/${host}/${port}") >/dev/null 2>&1; then
    echo "FalkorDB is available at ${host}:${port}"
    exit 0
  fi

  sleep 1
done

echo "Timed out waiting for FalkorDB at ${host}:${port}" >&2
exit 1
