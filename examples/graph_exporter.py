#!/usr/bin/env python3
"""
Graph Exporter — FalkorDB → CSV
================================
Export nodes and edges from a FalkorDB graph back to CSV files,
one file per label/relationship type.

Usage:
    python graph_exporter.py -g my_graph -o ./exported/
    python graph_exporter.py -g my_graph -o ./exported/ --labels Person,Company
    python graph_exporter.py -g my_graph -o ./exported/ --rels KNOWS,WORKS_AT
"""

import argparse
import csv
import os
from typing import List, Optional


def export_graph(graph_name: str, output_dir: str, host: str = "localhost",
                 port: int = 6379, username: Optional[str] = None,
                 password: Optional[str] = None,
                 labels: Optional[List[str]] = None,
                 rels: Optional[List[str]] = None):
    from falkordb import FalkorDB

    db = FalkorDB(host=host, port=port, username=username, password=password)
    g = db.select_graph(graph_name)
    os.makedirs(output_dir, exist_ok=True)

    # Discover labels
    if labels is None:
        res = g.query("CALL db.labels()")
        labels = [row[0] for row in res.result_set]

    # Discover rel types
    if rels is None:
        res = g.query("CALL db.relationshipTypes()")
        rels = [row[0] for row in res.result_set]

    total_nodes = 0
    total_edges = 0

    # Export nodes per label
    for label in labels:
        res = g.query(f"MATCH (n:{label}) RETURN n")
        if not res.result_set:
            continue

        rows = []
        all_keys: set = set()
        for record in res.result_set:
            node = record[0]
            props = dict(node.properties) if hasattr(node, "properties") else {}
            props["id"] = node.id if hasattr(node, "id") else props.get("id", "")
            all_keys.update(props.keys())
            rows.append(props)

        fieldnames = sorted(all_keys, key=lambda k: (k != "id", k))
        path = os.path.join(output_dir, f"nodes_{label}.csv")
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        total_nodes += len(rows)
        print(f"  📄 {path}  ({len(rows)} nodes)")

    # Export edges per type
    for rel_type in rels:
        res = g.query(
            f"MATCH (s)-[r:{rel_type}]->(t) "
            f"RETURN s.id, t.id, r"
        )
        if not res.result_set:
            continue

        rows = []
        extra_keys: set = set()
        for record in res.result_set:
            src_id, tgt_id, rel = record[0], record[1], record[2]
            props = dict(rel.properties) if hasattr(rel, "properties") else {}
            extra_keys.update(props.keys())
            row = {"source": src_id, "target": tgt_id, "type": rel_type, **props}
            rows.append(row)

        fieldnames = ["source", "target", "type"] + sorted(extra_keys)
        path = os.path.join(output_dir, f"edges_{rel_type}.csv")
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        total_edges += len(rows)
        print(f"  📄 {path}  ({len(rows)} edges)")

    print(f"\n✅ Exported {total_nodes} nodes, {total_edges} edges to {output_dir}")


def main():
    p = argparse.ArgumentParser(description="Export FalkorDB graph to CSV files.")
    p.add_argument("-g", "--graph", required=True, help="Graph name")
    p.add_argument("-o", "--output", required=True, help="Output directory")
    p.add_argument("-H", "--host", default="localhost")
    p.add_argument("-p", "--port", type=int, default=6379)
    p.add_argument("-u", "--username", default=None)
    p.add_argument("-a", "--password", default=None)
    p.add_argument("--labels", default=None, help="Comma-separated labels to export")
    p.add_argument("--rels", default=None, help="Comma-separated relationship types")
    args = p.parse_args()

    labels = args.labels.split(",") if args.labels else None
    rels = args.rels.split(",") if args.rels else None

    export_graph(args.graph, args.output, args.host, args.port,
                 args.username, args.password, labels, rels)


if __name__ == "__main__":
    main()
