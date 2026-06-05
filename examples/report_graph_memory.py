#!/usr/bin/env python3
"""
Graph Memory Reporter
=====================
Monitor per-graph memory usage on a FalkorDB instance using the native CLI
commands and write output reports.

For every graph (optionally filtered by prefix) it collects:
  * ``GRAPH.MEMORY USAGE <graph> [SAMPLES n]``  -> module memory size (MB)
  * ``MEMORY USAGE <graph>``                     -> total key size (bytes)

Outputs (written to --out-dir):
  memory.csv   graph,bytes,graph_mem_mb   (one row per graph, sorted desc)
  detail.txt   raw GRAPH.MEMORY USAGE     (per-graph breakdown)
  summary.txt  totals, avg/min/max, top consumers (also printed to stdout)

Usage:
    python examples/report_graph_memory.py
    python examples/report_graph_memory.py --prefix rndgraph
    python examples/report_graph_memory.py --host myhost:6379 --out-dir ./report
"""

from __future__ import annotations

import argparse
import os
from datetime import datetime
from pathlib import Path

import redis


def human_bytes(num: float) -> str:
    """Format a byte count as B/KiB/MiB/GiB/TiB."""
    for unit in ("B", "KiB", "MiB", "GiB"):
        if num < 1024:
            return f"{num:.0f} {unit}" if unit == "B" else f"{num:.2f} {unit}"
        num /= 1024
    return f"{num:.2f} TiB"


def graph_memory_mb(client: redis.Redis, name: str, samples: int) -> float:
    """Return the module-reported memory size in MB (raw[1] of GRAPH.MEMORY USAGE)."""
    try:
        raw = client.execute_command("GRAPH.MEMORY", "USAGE", name, "SAMPLES", samples)
        return float(raw[1])
    except Exception:
        return 0.0


def graph_memory_raw(client: redis.Redis, name: str, samples: int):
    try:
        return client.execute_command("GRAPH.MEMORY", "USAGE", name, "SAMPLES", samples)
    except Exception as exc:  # noqa: BLE001 - surfaced verbatim in the detail report
        return [f"(GRAPH.MEMORY USAGE failed: {exc})"]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Report per-graph memory usage on a FalkorDB instance."
    )
    parser.add_argument("--host", default="localhost", help="host (or host:port)")
    parser.add_argument("-p", "--port", type=int, default=6379)
    parser.add_argument("-u", "--username", default=None)
    parser.add_argument("-a", "--password", default=os.environ.get("FALKORDB_PASSWORD"))
    parser.add_argument("--prefix", default="",
                        help="only report graphs whose name starts with this prefix")
    parser.add_argument("--samples", type=int, default=100,
                        help="SAMPLES arg for GRAPH.MEMORY USAGE")
    parser.add_argument("--top", type=int, default=20,
                        help="how many top consumers to list in the summary")
    parser.add_argument("--out-dir", default=None,
                        help="report output directory "
                             "(default ~/Documents/work/scratch/graph-memory-reports/<ts>)")
    args = parser.parse_args()

    host = args.host
    port = args.port
    if ":" in host:
        host, _, port_str = host.partition(":")
        port = int(port_str)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(
        args.out_dir
        or Path.home() / "Documents/work/scratch/graph-memory-reports" / ts
    )
    out_dir.mkdir(parents=True, exist_ok=True)

    client = redis.Redis(host=host, port=port, username=args.username,
                         password=args.password, decode_responses=True)
    try:
        client.ping()
    except redis.RedisError as exc:
        print(f"❌ cannot reach FalkorDB at {host}:{port}: {exc}")
        return

    print(f"=== report_graph_memory ===")
    print(f"target  : {host}:{port}")
    print(f"filter  : {args.prefix or '<all graphs>'}")
    print(f"out dir : {out_dir}\n")

    graphs = client.execute_command("GRAPH.LIST") or []
    if args.prefix:
        graphs = [g for g in graphs if g.startswith(args.prefix)]

    if not graphs:
        suffix = f" matching prefix '{args.prefix}'" if args.prefix else ""
        print(f"No graphs found{suffix} on {host}:{port}.")
        return

    print(f"Collecting memory for {len(graphs)} graph(s)...")
    rows = []  # (name, bytes, mb)
    detail_path = out_dir / "detail.txt"
    with detail_path.open("w") as detail:
        for i, name in enumerate(graphs, start=1):
            mb = graph_memory_mb(client, name, args.samples)
            try:
                key_bytes = client.memory_usage(name) or 0
            except redis.RedisError:
                key_bytes = 0
            rows.append((name, int(key_bytes), mb))

            detail.write(f"########## {name} ##########\n")
            detail.write(f"{graph_memory_raw(client, name, args.samples)}\n\n")

            if i % 100 == 0 or i == len(graphs):
                print(f"  ...{i}/{len(graphs)}")

    rows.sort(key=lambda row: row[1], reverse=True)

    csv_path = out_dir / "memory.csv"
    with csv_path.open("w") as csv:
        csv.write("graph,bytes,graph_mem_mb\n")
        for name, key_bytes, mb in rows:
            csv.write(f"{name},{key_bytes},{mb:.4f}\n")

    total = sum(r[1] for r in rows)
    count = len(rows)
    avg = total / count if count else 0
    min_v = min((r[1] for r in rows), default=0)
    max_v = max((r[1] for r in rows), default=0)

    lines = [
        "FalkorDB graph memory report",
        f"generated : {datetime.now()}",
        f"target    : {host}:{port}",
        f"filter    : {args.prefix or '<all graphs>'}",
        "",
        f"graphs    : {count}",
        f"total     : {human_bytes(total)}  ({total} bytes)",
        f"average   : {human_bytes(avg)}  ({avg:.0f} bytes)",
        f"min       : {human_bytes(min_v)}  ({min_v} bytes)",
        f"max       : {human_bytes(max_v)}  ({max_v} bytes)",
        "",
        f"Top {args.top} graphs by memory:",
        f"  {'GRAPH':<40} {'BYTES':>15} {'HUMAN':>12} {'GRAPH.MEM MB':>14}",
    ]
    for name, key_bytes, mb in rows[: args.top]:
        lines.append(
            f"  {name:<40} {key_bytes:>15} {human_bytes(key_bytes):>12} {mb:>14.2f}"
        )

    summary = "\n".join(lines)
    (out_dir / "summary.txt").write_text(summary + "\n")
    print(summary)

    print("\nReports written to:")
    print(f"  {csv_path}")
    print(f"  {detail_path}")
    print(f"  {out_dir / 'summary.txt'}")


if __name__ == "__main__":
    main()
