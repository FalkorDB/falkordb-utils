#!/usr/bin/env python3
"""
FalkorDB Memory Scanner
=======================
Scan all keys in a FalkorDB/Redis instance and report memory usage per key,
sorted by size. Useful for identifying large graphs and optimizing storage.

Usage:
    python scan_memory.py
    python scan_memory.py --host myhost:6379 --password secret
    python scan_memory.py --top 50
"""

import argparse
import os
import redis


def scan(host: str = "localhost", port: int = 6379,
         username: str | None = None, password: str | None = None,
         top_n: int = 100):

    r = redis.Redis(host=host, port=port, username=username,
                    password=password, decode_responses=True)
    try:
        r.ping()
        print(f"✅ Connected to {host}:{port}")
    except redis.ConnectionError as e:
        print(f"❌ Connection failed: {e}")
        return
    except redis.AuthenticationError as e:
        print(f"❌ Auth failed: {e}")
        return

    print("Scanning keys...\n")
    results = []
    total_kb = 0
    cursor = 0

    while True:
        cursor, keys = r.scan(cursor=cursor, match="*", count=1000)
        for key in keys:
            key_type = r.type(key)
            if key_type == "graphdata":
                try:
                    raw = r.execute_command("GRAPH.MEMORY", "USAGE", key)
                    size_kb = int(raw[1]) * 1024  # MB → KB
                except Exception:
                    size_kb = 0
            else:
                size_bytes = r.memory_usage(key) or 0
                size_kb = size_bytes / 1024

            total_kb += size_kb
            results.append((key, size_kb, key_type))

        if cursor == 0:
            break

    results.sort(key=lambda x: x[1], reverse=True)
    results = results[:top_n]

    print(f"{'KEY':<50} {'SIZE':>12}  {'TYPE'}")
    print("─" * 70)
    for key, kb, ktype in results:
        if kb >= 1024:
            size_str = f"{kb / 1024:.1f} MB"
        else:
            size_str = f"{kb:.1f} KB"
        print(f"{key:<50} {size_str:>12}  {ktype}")

    print(f"\nTotal: {len(results)} keys shown (top {top_n}), "
          f"aggregate size: {total_kb / 1024:.1f} MB")


def main():
    p = argparse.ArgumentParser(description="Scan FalkorDB keys and report memory usage.")
    p.add_argument("--host", default="localhost", help="host (or host:port)")
    p.add_argument("-p", "--port", type=int, default=6379)
    p.add_argument("-u", "--username", default=None)
    p.add_argument("-a", "--password", default=os.environ.get("FALKORDB_PASSWORD"))
    p.add_argument("--top", type=int, default=100, help="Show top N keys by size")
    args = p.parse_args()

    host = args.host
    port = args.port
    if ":" in host:
        host, _, p_str = host.partition(":")
        port = int(p_str)

    scan(host, port, args.username, args.password, args.top)


if __name__ == "__main__":
    main()
