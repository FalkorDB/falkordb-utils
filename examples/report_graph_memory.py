#!/usr/bin/env python3
"""
Graph Memory Reporter
=====================
Report per-graph memory usage on a FalkorDB instance and write CSV/text reports.

For every graph (optionally filtered by prefix) it collects:
  * an estimated size in bytes, computed from node/edge counts via a measured
        model (tunable: --bytes-per-node, --bytes-per-edge, --graph-overhead-bytes).
        This is the metric the report sorts and aggregates on, because it has
        sub-MB resolution and distinguishes graphs that fall in the same MB bucket.
  * ``GRAPH.MEMORY USAGE <graph> [SAMPLES n]``  -> the module's ``total_graph_sz_mb``
        (integer MB). Native but coarse: it rounds to whole MB (so graphs in the
        same bucket look identical, and anything <1 MB reads 0) and under-counts
        real RSS. Shown as the SRV_MB reference column.
  * ``MEMORY USAGE <graph>``                     -> Redis key overhead only (~32 B
        for a module-managed type); NOT the graph's size. Recorded in the CSV only.

Outputs (written to --out-dir):
  memory.csv   graph,server_mb,est_size_bytes,est_size_human,nodes,edges,key_overhead_bytes
  labels.csv   graph,label,nodes,est_node_bytes        (per-graph node size by label)
  detail.txt   raw GRAPH.MEMORY USAGE                 (per-graph breakdown)
  summary.txt  totals, avg, smallest/largest, top consumers, node-size-by-label

Usage:
    python examples/report_graph_memory.py
    python examples/report_graph_memory.py --prefix rndgraph
    python examples/report_graph_memory.py --host myhost:6379 --out-dir ./report
"""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import redis


# --------------------------------------------------------------------------- #
# FalkorDB command wrappers — one function per server command we issue.
# --------------------------------------------------------------------------- #

def list_graphs(client: redis.Redis, prefix: str) -> "list[str]":
    """``GRAPH.LIST``, optionally filtered to names starting with ``prefix``."""
    graphs = client.execute_command("GRAPH.LIST") or []
    if prefix:
        graphs = [g for g in graphs if g.startswith(prefix)]
    return graphs


def read_memory_breakdown(client: redis.Redis, name: str, samples: int):
    """Raw ``GRAPH.MEMORY USAGE`` reply (flat key/value list) for ``name``.

    Returns a one-element placeholder list on error so it can still be written
    verbatim to the detail report.
    """
    try:
        return client.execute_command("GRAPH.MEMORY", "USAGE", name, "SAMPLES", samples)
    except Exception as exc:  # noqa: BLE001 - surfaced verbatim in the detail report
        return [f"(GRAPH.MEMORY USAGE failed: {exc})"]


def server_mb(breakdown) -> float:
    """``total_graph_sz_mb`` (the value at index 1) from a breakdown reply."""
    try:
        return float(breakdown[1])
    except (IndexError, TypeError, ValueError):
        return 0.0


def read_key_overhead_bytes(client: redis.Redis, name: str) -> int:
    """``MEMORY USAGE`` for the graph key (Redis key overhead, not graph size)."""
    try:
        return int(client.memory_usage(name) or 0)
    except redis.RedisError:
        return 0


def count_nodes_and_edges(client: redis.Redis, name: str) -> "tuple[int, int]":
    """Return ``(node_count, edge_count)`` via read-only count queries."""
    def _count(query: str) -> int:
        try:
            reply = client.execute_command("GRAPH.RO_QUERY", name, query)
            return int(reply[1][0][0])
        except Exception:
            return 0

    return (_count("MATCH (n) RETURN count(n)"),
            _count("MATCH ()-[r]->() RETURN count(r)"))


def count_nodes_by_label(client: redis.Redis, name: str) -> "dict[str, int]":
    """Return ``{label: node_count}`` (CALL db.labels() + one count per label)."""
    try:
        reply = client.execute_command("GRAPH.RO_QUERY", name, "CALL db.labels()")
        labels = [row[0] for row in reply[1]]
    except Exception:
        return {}

    counts: "dict[str, int]" = {}
    for label in labels:
        escaped = label.replace("`", "``")
        try:
            reply = client.execute_command(
                "GRAPH.RO_QUERY", name, f"MATCH (n:`{escaped}`) RETURN count(n)")
            counts[label] = int(reply[1][0][0])
        except Exception:
            counts[label] = 0
    return counts


# --------------------------------------------------------------------------- #
# Size model + per-graph record
# --------------------------------------------------------------------------- #

def human_bytes(num: float) -> str:
    """Format a byte count using binary units (B/KiB/MiB/GiB/TiB)."""
    for unit in ("B", "KiB", "MiB", "GiB"):
        if abs(num) < 1024:
            return f"{num:.0f} {unit}" if unit == "B" else f"{num:.1f} {unit}"
        num /= 1024
    return f"{num:.1f} TiB"


def estimate_graph_bytes(nodes: int, edges: int, *, overhead: int,
                         bytes_per_node: int, bytes_per_edge: int) -> int:
    """Estimated resident size of a graph from its element counts.

    GRAPH.MEMORY USAGE only reports whole MB, so it cannot distinguish graphs in
    the same MB bucket and reads 0 below 1 MB. This estimate gives every graph a
    distinct, sub-MB size. Constants are measured defaults, tunable via the CLI.
    """
    return overhead + nodes * bytes_per_node + edges * bytes_per_edge


@dataclass
class GraphMemory:
    """Collected memory facts for a single graph."""
    name: str
    server_mb: float
    nodes: int
    edges: int
    key_overhead_bytes: int
    estimated_bytes: int
    label_nodes: "dict[str, int]"

    @property
    def size_display(self) -> str:
        """Human-readable estimated size, marked with ``~`` as an estimate."""
        return f"~{human_bytes(self.estimated_bytes)}"


def collect_graph_memory(client: redis.Redis, name: str, args: argparse.Namespace,
                         detail_file) -> GraphMemory:
    """Issue every per-graph command once and assemble a ``GraphMemory`` record."""
    breakdown = read_memory_breakdown(client, name, args.samples)
    nodes, edges = count_nodes_and_edges(client, name)
    label_nodes = {} if args.no_by_label else count_nodes_by_label(client, name)
    estimated = estimate_graph_bytes(
        nodes, edges,
        overhead=args.graph_overhead_bytes,
        bytes_per_node=args.bytes_per_node,
        bytes_per_edge=args.bytes_per_edge,
    )
    detail_file.write(f"########## {name} ##########\n{breakdown}\n\n")
    return GraphMemory(
        name=name,
        server_mb=server_mb(breakdown),
        nodes=nodes,
        edges=edges,
        key_overhead_bytes=read_key_overhead_bytes(client, name),
        estimated_bytes=estimated,
        label_nodes=label_nodes,
    )


def aggregate_label_nodes(records: "list[GraphMemory]") -> "dict[str, int]":
    """Sum node counts per label across all graphs."""
    totals: "dict[str, int]" = {}
    for record in records:
        for label, nodes in record.label_nodes.items():
            totals[label] = totals.get(label, 0) + nodes
    return totals


# --------------------------------------------------------------------------- #
# Report writers
# --------------------------------------------------------------------------- #

def write_memory_csv(path: Path, records: "list[GraphMemory]") -> None:
    """One row per graph, sorted as given."""
    with path.open("w") as csv:
        csv.write("graph,server_mb,est_size_bytes,est_size_human,"
                  "nodes,edges,key_overhead_bytes\n")
        for r in records:
            csv.write(f"{r.name},{r.server_mb:.4f},{r.estimated_bytes},"
                      f"{human_bytes(r.estimated_bytes)},"
                      f"{r.nodes},{r.edges},{r.key_overhead_bytes}\n")


def write_labels_csv(path: Path, records: "list[GraphMemory]",
                     bytes_per_node: int) -> None:
    """One row per (graph, label) with the estimated node-only size."""
    with path.open("w") as csv:
        csv.write("graph,label,nodes,est_node_bytes\n")
        for r in records:
            for label, nodes in r.label_nodes.items():
                csv.write(f"{r.name},{label},{nodes},{nodes * bytes_per_node}\n")


def build_summary(records: "list[GraphMemory]", args: argparse.Namespace,
                  host: str, port: int) -> str:
    """Assemble the human-readable summary text (also written to summary.txt)."""
    count = len(records)
    est_total = sum(r.estimated_bytes for r in records)
    srv_total_mb = sum(r.server_mb for r in records)
    avg_bytes = est_total / count if count else 0
    sub_1mb = sum(1 for r in records if r.server_mb < 1)
    smallest = f"{records[-1].size_display} ({records[-1].name})" if records else "n/a"
    largest = f"{records[0].size_display} ({records[0].name})" if records else "n/a"

    lines = [
        "FalkorDB graph memory report",
        f"generated     : {datetime.now()}",
        f"target        : {host}:{port}",
        f"filter        : {args.prefix or '<all graphs>'}",
        "",
        f"graphs        : {count}",
        f"sub-1MB (srv) : {sub_1mb}  (GRAPH.MEMORY USAGE rounds these down to 0 MB)",
        f"est. total    : {human_bytes(est_total)}",
        f"est. average  : {human_bytes(avg_bytes)}",
        f"server total  : {srv_total_mb:.0f} MB  (integer-MB; under-counts real RSS)",
        f"smallest      : {smallest}",
        f"largest       : {largest}",
        "",
        f"size estimate = {args.graph_overhead_bytes} B/graph "
        f"+ {args.bytes_per_node} B/node + {args.bytes_per_edge} B/edge "
        "(~ = estimated; SRV_MB = native GRAPH.MEMORY USAGE)",
        "",
        f"Top {args.top} graphs by estimated size:",
        f"  {'GRAPH':<32} {'EST_SIZE':>12} {'NODES':>9} {'EDGES':>10} {'SRV_MB':>7}",
    ]
    for r in records[: args.top]:
        lines.append(
            f"  {r.name:<32} {r.size_display:>12} {r.nodes:>9} "
            f"{r.edges:>10} {r.server_mb:>7.0f}"
        )

    if not args.no_by_label:
        ranked = sorted(aggregate_label_nodes(records).items(),
                        key=lambda kv: kv[1], reverse=True)
        lines += [
            "",
            f"Node size by label (estimated, {args.bytes_per_node} B/node, "
            f"summed over {count} graph(s)):",
            f"  {'LABEL':<24} {'NODES':>14} {'EST_NODE_SIZE':>14}",
        ]
        for label, nodes in ranked[: args.top_labels]:
            lines.append(
                f"  {label:<24} {nodes:>14,} {human_bytes(nodes * args.bytes_per_node):>14}"
            )

    return "\n".join(lines)


# --------------------------------------------------------------------------- #
# CLI plumbing
# --------------------------------------------------------------------------- #

def parse_args() -> argparse.Namespace:
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
    parser.add_argument("--bytes-per-node", type=int, default=70,
                        help="estimated bytes/node for the size estimate "
                             "(measured default)")
    parser.add_argument("--bytes-per-edge", type=int, default=60,
                        help="estimated bytes/edge for the size estimate "
                             "(measured default)")
    parser.add_argument("--graph-overhead-bytes", type=int, default=440_000,
                        help="estimated fixed per-graph overhead for the size "
                             "estimate (measured default)")
    parser.add_argument("--top-labels", type=int, default=20,
                        help="how many labels to list in the by-label breakdown")
    parser.add_argument("--no-by-label", action="store_true",
                        help="skip the per-label node-size breakdown "
                             "(avoids one query per label per graph)")
    parser.add_argument("--out-dir", default=None,
                        help="report output directory "
                             "(default ~/Documents/work/scratch/graph-memory-reports/<ts>)")
    return parser.parse_args()


def resolve_host_port(args: argparse.Namespace) -> "tuple[str, int]":
    """Split a ``host:port`` --host value into (host, port)."""
    host, port = args.host, args.port
    if ":" in host:
        host, _, port_str = host.partition(":")
        port = int(port_str)
    return host, port


def connect(host: str, port: int, args: argparse.Namespace) -> "redis.Redis | None":
    """Build a decoded Redis client and verify reachability with PING."""
    client = redis.Redis(host=host, port=port, username=args.username,
                         password=args.password, decode_responses=True)
    try:
        client.ping()
    except redis.RedisError as exc:
        print(f"❌ cannot reach FalkorDB at {host}:{port}: {exc}")
        return None
    return client


def make_out_dir(args: argparse.Namespace) -> Path:
    """Resolve and create the report output directory."""
    out_dir = Path(
        args.out_dir
        or Path.home() / "Documents/work/scratch/graph-memory-reports"
        / datetime.now().strftime("%Y%m%d_%H%M%S")
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir


def main() -> None:
    args = parse_args()
    host, port = resolve_host_port(args)
    out_dir = make_out_dir(args)

    client = connect(host, port, args)
    if client is None:
        return

    print("=== report_graph_memory ===")
    print(f"target  : {host}:{port}")
    print(f"filter  : {args.prefix or '<all graphs>'}")
    print(f"out dir : {out_dir}\n")

    graphs = list_graphs(client, args.prefix)
    if not graphs:
        suffix = f" matching prefix '{args.prefix}'" if args.prefix else ""
        print(f"No graphs found{suffix} on {host}:{port}.")
        return

    print(f"Collecting memory for {len(graphs)} graph(s)...")
    detail_path = out_dir / "detail.txt"
    records: "list[GraphMemory]" = []
    with detail_path.open("w") as detail:
        for i, name in enumerate(graphs, start=1):
            records.append(collect_graph_memory(client, name, args, detail))
            if i % 100 == 0 or i == len(graphs):
                print(f"  ...{i}/{len(graphs)}")

    records.sort(key=lambda r: r.estimated_bytes, reverse=True)

    memory_csv = out_dir / "memory.csv"
    write_memory_csv(memory_csv, records)
    labels_csv = out_dir / "labels.csv"
    if not args.no_by_label:
        write_labels_csv(labels_csv, records, args.bytes_per_node)

    summary = build_summary(records, args, host, port)
    (out_dir / "summary.txt").write_text(summary + "\n")
    print(summary)

    print("\nReports written to:")
    print(f"  {memory_csv}")
    if not args.no_by_label:
        print(f"  {labels_csv}")
    print(f"  {detail_path}")
    print(f"  {out_dir / 'summary.txt'}")


if __name__ == "__main__":
    main()
