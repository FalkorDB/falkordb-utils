#!/usr/bin/env python3
"""
FalkorDB Health Check
=====================
Quick connectivity and status check for a FalkorDB instance.
Reports server info, loaded graphs, memory, and basic stats.

Usage:
    python health_check.py
    python health_check.py --host myhost --port 6379
"""

import argparse
import os
from typing import Optional


def check(host: str = "localhost", port: int = 6379,
          username: Optional[str] = None, password: Optional[str] = None):
    import redis

    r = redis.Redis(host=host, port=port, username=username,
                    password=password, decode_responses=True)

    # Connectivity
    print(f"🔌 Connecting to {host}:{port} ...")
    try:
        r.ping()
        print("  ✅ Connection OK\n")
    except redis.ConnectionError as e:
        print(f"  ❌ Connection FAILED: {e}")
        return
    except redis.AuthenticationError as e:
        print(f"  ❌ Auth FAILED: {e}")
        return

    # Server info
    info = r.info()
    print("═══ Server ═══")
    print(f"  Redis version:   {info.get('redis_version', '?')}")
    print(f"  OS:              {info.get('os', '?')}")
    print(f"  Uptime:          {info.get('uptime_in_days', '?')} days")
    print(f"  Connected clients: {info.get('connected_clients', '?')}")
    print()

    # Memory
    print("═══ Memory ═══")
    mem = info.get("used_memory_human", "?")
    mem_peak = info.get("used_memory_peak_human", "?")
    print(f"  Used:  {mem}")
    print(f"  Peak:  {mem_peak}")
    print()

    # Graphs
    print("═══ Graphs ═══")
    cursor, count = 0, 0
    while True:
        cursor, keys = r.scan(cursor=cursor, match="*", count=500)
        for key in keys:
            if r.type(key) == "graphdata":
                count += 1
                try:
                    raw = r.execute_command("GRAPH.MEMORY", "USAGE", key)
                    size = f"{int(raw[1])} MB"
                except Exception:
                    size = "?"
                print(f"  📊 {key}  ({size})")
        if cursor == 0:
            break

    if count == 0:
        print("  (no graphs found)")
    print(f"\n  Total graphs: {count}")


def main():
    p = argparse.ArgumentParser(description="FalkorDB health check.")
    p.add_argument("--host", default="localhost")
    p.add_argument("-p", "--port", type=int, default=6379)
    p.add_argument("-u", "--username", default=None)
    p.add_argument("-a", "--password", default=os.environ.get("FALKORDB_PASSWORD"))
    args = p.parse_args()

    check(args.host, args.port, args.username, args.password)


if __name__ == "__main__":
    main()
