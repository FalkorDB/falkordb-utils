#!/usr/bin/env python3
"""
Random Graph Generator
======================
Create many FalkorDB graphs, each populated with a RANDOM number of nodes in
a configurable range, then wired together with random ``:CONNECTED_TO`` edges.
Defaults to the requested workload:

    10,000 graphs, each with a random 10,000 - 100,000 nodes.

Nodes are created server-side in batches via ``UNWIND range(...) CREATE``, so
the query text stays tiny regardless of how many nodes a graph has. Edges are
likewise created server-side by collecting the node list once and picking
random endpoints, so no per-row lookup or :id index is required.

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


def add_edges(graph, total_nodes: int, total_edges: int, batch: int) -> None:
    """Create ``total_edges`` random ``:CONNECTED_TO`` edges between existing nodes.

    Endpoints are chosen uniformly at random. ``collect()`` materialises the
    node list once per batch so endpoints can be picked by random index without
    an ``:id`` index or a per-row ``MATCH``. Self-loops are skipped, so the
    actual edge count is marginally below ``total_edges``.
    """
    if total_edges <= 0 or total_nodes < 2:
        return
    created = 0
    while created < total_edges:
        chunk = min(batch, total_edges - created)
        graph.query(
            "MATCH (n:Node) "
            "WITH collect(n) AS nodes, count(n) AS node_count "
            f"UNWIND range(1, {chunk}) AS edge_index "
            "WITH nodes[toInteger(rand() * node_count)] AS source, "
            "nodes[toInteger(rand() * node_count)] AS target "
            "WHERE source <> target "
            "CREATE (source)-[:CONNECTED_TO]->(target)"
        )
        created += chunk


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
                        help="nodes (or edges) created per CREATE query")
    parser.add_argument("--edges-per-node", type=float, default=2.0,
                        help="avg edges per node; edges = round(nodes * this), "
                             "0 disables edge creation")
    parser.add_argument("--prefix", default="rndgraph", help="graph name prefix")
    parser.add_argument("--start-index", type=int, default=1,
                        help="first graph index (for resuming)")
    parser.add_argument("--seed", type=int, default=42,
                        help="RNG seed for reproducible node counts")
    parser.add_argument("--progress-every", type=int, default=100,
                        help="heartbeat interval in graphs")
    parser.add_argument("--small-graph-nodes", type=int, default=0,
                        help="also create one extra '<prefix>_small' graph with "
                             "this many nodes and no edges (a deliberate sub-1MB "
                             "control graph); 0 disables")
    parser.add_argument("--drop-existing", action="store_true",
                        help="delete existing graphs matching --prefix before "
                             "creating (makes a run idempotent)")
    args = parser.parse_args()

    if args.min_nodes < 1 or args.max_nodes < args.min_nodes:
        parser.error("require 1 <= --min-nodes <= --max-nodes")
    if args.small_graph_nodes < 0:
        parser.error("--small-graph-nodes must be >= 0")

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
    print(f"edges/graph : ~{args.edges_per_node:g} x nodes (:CONNECTED_TO)")
    print(f"batch size  : {args.batch} rows/query")
    print(f"seed        : {args.seed}\n")

    if args.drop_existing:
        existing = db.connection.execute_command("GRAPH.LIST") or []
        stale = [g.decode() if isinstance(g, bytes) else g for g in existing]
        stale = [g for g in stale
                 if g == args.prefix or g.startswith(f"{args.prefix}_")]
        for stale_graph in stale:
            db.connection.execute_command("GRAPH.DELETE", stale_graph)
        print(f"dropped {len(stale)} existing graph(s) "
              f"with prefix '{args.prefix}'\n")

    started_at = time.perf_counter()
    total_nodes = 0
    total_edges = 0
    last_index = args.start_index + args.num_graphs - 1

    for index in range(args.start_index, last_index + 1):
        name = f"{args.prefix}_{index}"
        nodes = rng.randint(args.min_nodes, args.max_nodes)
        edges = int(round(nodes * args.edges_per_node))
        graph = db.select_graph(name)
        populate_graph(graph, nodes, args.batch)
        add_edges(graph, nodes, edges, args.batch)
        total_nodes += nodes
        total_edges += edges

        done = index - args.start_index + 1
        if done % args.progress_every == 0 or index == last_index:
            elapsed = time.perf_counter() - started_at
            print(f"[{done}/{args.num_graphs}] last={name} "
                  f"({nodes:,} nodes, {edges:,} edges) | cumulative "
                  f"{total_nodes:,} nodes / {total_edges:,} edges | "
                  f"{elapsed:.0f}s elapsed")

    graphs_created = args.num_graphs
    if args.small_graph_nodes > 0:
        small_name = f"{args.prefix}_small"
        small_graph = db.select_graph(small_name)
        populate_graph(small_graph, args.small_graph_nodes, args.batch)
        total_nodes += args.small_graph_nodes
        graphs_created += 1
        print(f"\nsmall graph : {small_name} "
              f"({args.small_graph_nodes:,} nodes, 0 edges) "
              f"-- deliberate sub-1MB control")

    elapsed = time.perf_counter() - started_at
    print(f"\nDone: created {graphs_created} graph(s), "
          f"{total_nodes:,} nodes and {total_edges:,} edges total, "
          f"in {elapsed:.0f}s.")


if __name__ == "__main__":
    main()
