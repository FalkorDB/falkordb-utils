#!/usr/bin/env python3
"""
Random Graph Generator
======================
Create many FalkorDB graphs, each populated with a RANDOM number of nodes in
a configurable range. Defaults to the requested workload:

    10,000 graphs, each with a random 10,000 - 100,000 nodes.

Nodes are created server-side in batches via ``UNWIND range(...) CREATE``, so
the query text stays tiny regardless of how many nodes a graph has.

WARNING: the defaults are LARGE — 10,000 graphs averaging ~55K nodes is
~550 million nodes total. Smoke-test first with small overrides, e.g.:

    python examples/create_random_graphs.py --num-graphs 5 \\
        --min-nodes 100 --max-nodes 500

Usage:
    python examples/create_random_graphs.py
    python examples/create_random_graphs.py --num-graphs 100
    python examples/create_random_graphs.py --host myhost:6379 -a secret
"""

from __future__ import annotations

import argparse
import os
import random
import time

import redis
from falkordb import FalkorDB


def populate_graph(graph, total_nodes: int, batch: int) -> None:
    """Create ``total_nodes`` :Node rows in ``graph`` in batches of ``batch``."""
    start = 1
    while start <= total_nodes:
        end = min(start + batch - 1, total_nodes)
        # range() is evaluated server-side, so the query text is constant size.
        graph.query(f"UNWIND range({start}, {end}) AS i CREATE (:Node {{id: i}})")
        start = end + 1


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create N FalkorDB graphs, each with a random node count."
    )
    parser.add_argument("--host", default="localhost", help="host (or host:port)")
    parser.add_argument("-p", "--port", type=int, default=6379)
    parser.add_argument("-u", "--username", default=None)
    parser.add_argument("-a", "--password", default=os.environ.get("FALKORDB_PASSWORD"))
    parser.add_argument("--num-graphs", type=int, default=10_000,
                        help="number of graphs to create")
    parser.add_argument("--min-nodes", type=int, default=10_000,
                        help="min nodes per graph (inclusive)")
    parser.add_argument("--max-nodes", type=int, default=100_000,
                        help="max nodes per graph (inclusive)")
    parser.add_argument("--batch", type=int, default=10_000,
                        help="nodes created per CREATE query")
    parser.add_argument("--prefix", default="rndgraph", help="graph name prefix")
    parser.add_argument("--start-index", type=int, default=1,
                        help="first graph index (for resuming)")
    parser.add_argument("--seed", type=int, default=42,
                        help="RNG seed for reproducible node counts")
    parser.add_argument("--progress-every", type=int, default=100,
                        help="heartbeat interval in graphs")
    args = parser.parse_args()

    if args.min_nodes < 1 or args.max_nodes < args.min_nodes:
        parser.error("require 1 <= --min-nodes <= --max-nodes")

    host = args.host
    port = args.port
    if ":" in host:
        host, _, port_str = host.partition(":")
        port = int(port_str)

    # FalkorDB connects eagerly, so a bad endpoint fails fast here.
    try:
        db = FalkorDB(host=host, port=port,
                      username=args.username, password=args.password)
    except redis.RedisError as exc:
        parser.error(f"cannot reach FalkorDB at {host}:{port}: {exc}")

    rng = random.Random(args.seed)

    print(f"=== create_random_graphs ===")
    print(f"target      : {host}:{port}")
    print(f"graphs      : {args.num_graphs} "
          f"(prefix '{args.prefix}_', start index {args.start_index})")
    print(f"nodes/graph : random [{args.min_nodes}, {args.max_nodes}]")
    print(f"batch size  : {args.batch} nodes/query")
    print(f"seed        : {args.seed}\n")

    started_at = time.perf_counter()
    total_nodes = 0
    last_index = args.start_index + args.num_graphs - 1

    for index in range(args.start_index, last_index + 1):
        name = f"{args.prefix}_{index}"
        nodes = rng.randint(args.min_nodes, args.max_nodes)
        graph = db.select_graph(name)
        populate_graph(graph, nodes, args.batch)
        total_nodes += nodes

        done = index - args.start_index + 1
        if done % args.progress_every == 0 or index == last_index:
            elapsed = time.perf_counter() - started_at
            print(f"[{done}/{args.num_graphs}] last={name} ({nodes:,} nodes) "
                  f"| cumulative {total_nodes:,} nodes | {elapsed:.0f}s elapsed")

    elapsed = time.perf_counter() - started_at
    print(f"\nDone: created {args.num_graphs} graph(s), "
          f"{total_nodes:,} nodes total, in {elapsed:.0f}s.")


if __name__ == "__main__":
    main()
