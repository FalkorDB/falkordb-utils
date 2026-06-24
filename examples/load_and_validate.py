#!/usr/bin/env python3
"""
Load and Validate
=================
Write a fixed, pre-known graph to one FalkorDB instance (DB 1), then validate
that every node and relationship also exists on a second instance (DB 2). Useful
for checking replication, GRAPH.COPY, backup/restore, or a migration between
servers.

Two steps, two instances, each given its own connection:

    # 1. Load the known dataset into DB 1
    python examples/load_and_validate.py load --host db1-host:6379

    # 2. Validate DB 2 contains everything that was written
    python examples/load_and_validate.py validate --host db2-host:6379

The two steps share one source of truth: the deterministic ``build_known_dataset``
function. ``validate`` recomputes exactly what ``load`` wrote, so the validator is
always aware of the loaded data and cannot drift from the loader.

By default ``validate`` compares DB 2 against that embedded dataset (so it needs
nothing but DB 2). Pass ``--source-host`` to instead diff DB 2 directly against
the live graph on DB 1:

    python examples/load_and_validate.py validate \\
        --host db2-host:6379 --source-host db1-host:6379

The dataset is deterministic for a given ``--nodes``, so ``load`` and ``validate``
must use the same ``--nodes`` (and ``--graph``) value. ``validate`` exits non-zero
if anything is missing or mismatched.
"""

from __future__ import annotations

import argparse
import os
import sys

import redis
from falkordb import FalkorDB

NODE_LABEL = "Node"
RING_REL = "LINKS_TO"
EXTRA_REL = "RELATES_TO"


def build_known_dataset(node_count: int) -> "tuple[list[dict], list[dict]]":
    """Return the deterministic ``(nodes, relationships)`` for ``node_count``.

    Pure function: the same ``node_count`` always yields identical data, which is
    what lets ``validate`` recompute exactly what ``load`` wrote.
    """
    nodes = [
        {
            "id": i,
            "name": f"node-{i}",
            "value": i * 10,
            "category": i % 5,
            "active": (i % 2 == 0),
        }
        for i in range(1, node_count + 1)
    ]

    relationships = []
    for i in range(1, node_count + 1):
        ring_target = (i % node_count) + 1
        relationships.append({
            "type": RING_REL,
            "source": i,
            "target": ring_target,
            "weight": i % 7,
        })
        if i % 3 == 0:
            extra_target = ((i + 1) % node_count) + 1
            relationships.append({
                "type": EXTRA_REL,
                "source": i,
                "target": extra_target,
                "weight": (i * 2) % 11,
            })
    return nodes, relationships


# --------------------------------------------------------------------------- #
# Connection helpers
# --------------------------------------------------------------------------- #

def resolve_host_port(host: str, port: int) -> "tuple[str, int]":
    """Split a ``host:port`` value into (host, port)."""
    if ":" in host:
        host, _, port_str = host.partition(":")
        port = int(port_str)
    return host, port


def connect_graph(host: str, port: int, username, password, graph_name: str):
    """Return the selected graph on the given FalkorDB instance (fails fast)."""
    db = FalkorDB(host=host, port=port, username=username, password=password)
    db.connection.ping()
    return db.select_graph(graph_name)


# --------------------------------------------------------------------------- #
# Graph reads/writes
# --------------------------------------------------------------------------- #

def node_count_in_graph(graph) -> int:
    """Number of nodes currently in ``graph`` (0 if the graph is empty/new)."""
    try:
        return int(graph.query("MATCH (n) RETURN count(n)").result_set[0][0])
    except redis.ResponseError:
        return 0


def ensure_id_index(graph) -> None:
    """Create the :Node(id) index, ignoring the error if it already exists."""
    try:
        graph.query(f"CREATE INDEX FOR (n:{NODE_LABEL}) ON (n.id)")
    except redis.ResponseError:
        pass  # index already exists


def write_dataset(graph, nodes: "list[dict]", relationships: "list[dict]") -> None:
    """Write the known nodes and relationships into ``graph`` (parameterized)."""
    ensure_id_index(graph)

    graph.query(
        f"UNWIND $rows AS row "
        f"CREATE (:{NODE_LABEL} {{id: row.id, name: row.name, value: row.value, "
        f"category: row.category, active: row.active}})",
        params={"rows": nodes},
    )

    by_type: "dict[str, list[dict]]" = {}
    for rel in relationships:
        by_type.setdefault(rel["type"], []).append(rel)

    for rel_type, rows in by_type.items():
        graph.query(
            f"UNWIND $rows AS row "
            f"MATCH (a:{NODE_LABEL} {{id: row.source}}), "
            f"(b:{NODE_LABEL} {{id: row.target}}) "
            f"CREATE (a)-[:{rel_type} {{weight: row.weight}}]->(b)",
            params={"rows": rows},
        )


def read_nodes(graph) -> "dict[int, dict]":
    """Return ``{id: properties}`` for every :Node in ``graph``."""
    rows = graph.query(
        f"MATCH (n:{NODE_LABEL}) RETURN n.id, properties(n)"
    ).result_set
    return {int(node_id): props for node_id, props in rows}


def read_relationships(graph) -> "set[tuple]":
    """Return a hashable set describing every relationship in ``graph``."""
    rows = graph.query(
        "MATCH (a)-[r]->(b) RETURN a.id, b.id, type(r), properties(r)"
    ).result_set
    return {
        (rel_type, int(src), int(dst), tuple(sorted(props.items())))
        for src, dst, rel_type, props in rows
    }


# --------------------------------------------------------------------------- #
# Expected-vs-actual comparison
# --------------------------------------------------------------------------- #

def expected_nodes(nodes: "list[dict]") -> "dict[int, dict]":
    """Canonical ``{id: properties}`` map from the known node list."""
    return {n["id"]: dict(n) for n in nodes}


def expected_relationships(relationships: "list[dict]") -> "set[tuple]":
    """Canonical relationship set matching ``read_relationships`` shape."""
    return {
        (r["type"], r["source"], r["target"],
         tuple(sorted({"weight": r["weight"]}.items())))
        for r in relationships
    }


def compare(expected_nodes_map, expected_rels, actual_nodes_map, actual_rels):
    """Return (missing_nodes, mismatched_nodes, missing_rels)."""
    missing_nodes = []
    mismatched_nodes = []
    for node_id, props in expected_nodes_map.items():
        if node_id not in actual_nodes_map:
            missing_nodes.append(node_id)
        elif actual_nodes_map[node_id] != props:
            mismatched_nodes.append(node_id)

    missing_rels = expected_rels - actual_rels
    return missing_nodes, mismatched_nodes, missing_rels


# --------------------------------------------------------------------------- #
# Subcommands
# --------------------------------------------------------------------------- #

def cmd_load(args: argparse.Namespace) -> int:
    host, port = resolve_host_port(args.host, args.port)
    nodes, relationships = build_known_dataset(args.nodes)
    graph = connect_graph(host, port, args.username, args.password, args.graph)

    print("=== load_and_validate : load ===")
    print(f"target : {host}:{port}")
    print(f"graph  : {args.graph}")
    print(f"dataset: {len(nodes)} nodes / {len(relationships)} relationships\n")

    existing = node_count_in_graph(graph)
    if existing > 0:
        if args.on_existing == "abort":
            print(f"❌ graph '{args.graph}' already has {existing} node(s). "
                  f"Use --on-existing overwrite or skip.")
            return 1
        if args.on_existing == "skip":
            print(f"graph '{args.graph}' already has {existing} node(s); "
                  f"skipping (--on-existing skip).")
            return 0
        print(f"graph '{args.graph}' already has {existing} node(s); "
              f"deleting and rewriting (--on-existing overwrite).")
        graph.delete()
        graph = connect_graph(host, port, args.username, args.password, args.graph)

    write_dataset(graph, nodes, relationships)
    print(f"✅ wrote {len(nodes)} nodes and {len(relationships)} relationships "
          f"to '{args.graph}' on {host}:{port}.")
    return 0


def cmd_validate(args: argparse.Namespace) -> int:
    host, port = resolve_host_port(args.host, args.port)
    target = connect_graph(host, port, args.username, args.password, args.graph)

    print("=== load_and_validate : validate ===")
    print(f"target (DB 2) : {host}:{port}  graph '{args.graph}'")

    if args.source_host:
        src_host, src_port = resolve_host_port(args.source_host, args.port)
        src_graph = args.source_graph or args.graph
        source = connect_graph(src_host, src_port, args.username, args.password,
                               src_graph)
        exp_nodes = read_nodes(source)
        exp_rels = read_relationships(source)
        print(f"source (DB 1) : {src_host}:{src_port}  graph '{src_graph}' "
              f"(live source of truth)")
    else:
        nodes, relationships = build_known_dataset(args.nodes)
        exp_nodes = expected_nodes(nodes)
        exp_rels = expected_relationships(relationships)
        print(f"source        : embedded known dataset ({args.nodes} nodes)")

    print(f"expected      : {len(exp_nodes)} nodes / {len(exp_rels)} relationships\n")

    actual_nodes = read_nodes(target)
    actual_rels = read_relationships(target)
    missing_nodes, mismatched_nodes, missing_rels = compare(
        exp_nodes, exp_rels, actual_nodes, actual_rels)

    print(f"nodes         : {len(exp_nodes) - len(missing_nodes)}/{len(exp_nodes)} "
          f"present, {len(mismatched_nodes)} mismatched")
    print(f"relationships : {len(exp_rels) - len(missing_rels)}/{len(exp_rels)} "
          f"present")

    if not missing_nodes and not mismatched_nodes and not missing_rels:
        print("\n✅ PASS: DB 2 contains all data written to DB 1.")
        return 0

    print("\n❌ FAIL: DB 2 is missing data or differs from DB 1.")
    if missing_nodes:
        shown = sorted(missing_nodes)[:20]
        print(f"  missing nodes ({len(missing_nodes)}): {shown}"
              f"{' ...' if len(missing_nodes) > 20 else ''}")
    if mismatched_nodes:
        for node_id in sorted(mismatched_nodes)[:10]:
            print(f"  mismatched node id={node_id}: "
                  f"expected {exp_nodes[node_id]} got {actual_nodes.get(node_id)}")
    if missing_rels:
        sample = list(missing_rels)[:10]
        print(f"  missing relationships ({len(missing_rels)}): {sample}"
              f"{' ...' if len(missing_rels) > 10 else ''}")
    return 1


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load a known graph into DB 1 and validate it on DB 2."
    )
    sub = parser.add_subparsers(dest="command", required=True)

    def add_common(sp):
        sp.add_argument("--host", default="localhost", help="host (or host:port)")
        sp.add_argument("-p", "--port", type=int, default=6379)
        sp.add_argument("-u", "--username", default=None)
        sp.add_argument("-a", "--password",
                        default=os.environ.get("FALKORDB_PASSWORD"))
        sp.add_argument("--graph", default="known_data", help="graph name")
        sp.add_argument("--nodes", type=int, default=100,
                        help="size of the deterministic known dataset")

    load = sub.add_parser("load", help="write the known dataset to DB 1")
    add_common(load)
    load.add_argument("--on-existing", choices=("abort", "overwrite", "skip"),
                      default="abort",
                      help="what to do if the graph already has data")

    validate = sub.add_parser("validate",
                              help="check DB 2 contains the known dataset")
    add_common(validate)
    validate.add_argument("--source-host", default=None,
                          help="compare against this live DB 1 graph instead of "
                               "the embedded dataset (host or host:port)")
    validate.add_argument("--source-graph", default=None,
                          help="source graph name on --source-host "
                               "(defaults to --graph)")

    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        if args.command == "load":
            sys.exit(cmd_load(args))
        else:
            sys.exit(cmd_validate(args))
    except redis.RedisError as exc:
        raise SystemExit(f"❌ FalkorDB error: {exc}")


if __name__ == "__main__":
    main()
