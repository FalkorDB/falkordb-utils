#!/usr/bin/env python3
"""
Graph Schema Inspector
======================
Connects to a FalkorDB graph and reports its schema:
  - Node labels and counts
  - Relationship types and counts
  - Property keys per label/type
  - Index definitions

Usage:
    python graph_schema_inspector.py -g my_graph
    python graph_schema_inspector.py -g my_graph -h remotehost -p 6379
"""

import argparse
import os
from typing import Optional


def inspect(graph_name: str, host: str = "localhost", port: int = 6379,
            username: Optional[str] = None, password: Optional[str] = None):
    from falkordb import FalkorDB

    db = FalkorDB(host=host, port=port, username=username, password=password)
    g = db.select_graph(graph_name)

    print(f"🔍 Schema for graph: {graph_name}\n")

    # Node labels
    print("═══ Node Labels ═══")
    try:
        result = g.query("CALL db.labels()")
        labels = [row[0] for row in result.result_set]
        for label in labels:
            count_res = g.query(f"MATCH (n:{label}) RETURN count(n)")
            count = count_res.result_set[0][0] if count_res.result_set else "?"
            # Get sample properties
            props_res = g.query(f"MATCH (n:{label}) RETURN keys(n) LIMIT 1")
            props = props_res.result_set[0][0] if props_res.result_set else []
            print(f"  :{label}  ({count} nodes)")
            if props:
                print(f"    properties: {', '.join(props)}")
        if not labels:
            print("  (none)")
    except Exception as e:
        print(f"  ❌ Error: {e}")

    print()

    # Relationship types
    print("═══ Relationship Types ═══")
    try:
        result = g.query("CALL db.relationshipTypes()")
        rel_types = [row[0] for row in result.result_set]
        for rt in rel_types:
            count_res = g.query(f"MATCH ()-[r:{rt}]->() RETURN count(r)")
            count = count_res.result_set[0][0] if count_res.result_set else "?"
            # Get sample properties
            props_res = g.query(f"MATCH ()-[r:{rt}]->() RETURN keys(r) LIMIT 1")
            props = props_res.result_set[0][0] if props_res.result_set else []
            print(f"  :{rt}  ({count} edges)")
            if props:
                print(f"    properties: {', '.join(props)}")
        if not rel_types:
            print("  (none)")
    except Exception as e:
        print(f"  ❌ Error: {e}")

    print()

    # Indexes
    print("═══ Indexes ═══")
    try:
        result = g.query("CALL db.indexes()")
        if result.result_set:
            for row in result.result_set:
                print(f"  {row}")
        else:
            print("  (none)")
    except Exception as e:
        print(f"  ❌ Error: {e}")


def main():
    p = argparse.ArgumentParser(description="Inspect FalkorDB graph schema.")
    p.add_argument("-g", "--graph", required=True, help="Graph name")
    p.add_argument("-H", "--host", default="localhost")
    p.add_argument("-p", "--port", type=int, default=6379)
    p.add_argument("-u", "--username", default=None)
    p.add_argument("-a", "--password", default=None)
    args = p.parse_args()

    inspect(args.graph, args.host, args.port, args.username, args.password)


if __name__ == "__main__":
    main()
