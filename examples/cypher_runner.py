#!/usr/bin/env python3
"""
Cypher Query Runner
===================
Run Cypher queries against FalkorDB — from a .cypher file,
a one-liner, or in interactive REPL mode.

Usage:
    # Run a .cypher file
    python cypher_runner.py -g my_graph -f queries.cypher

    # Run a one-liner
    python cypher_runner.py -g my_graph -q "MATCH (n) RETURN n LIMIT 5"

    # Interactive REPL
    python cypher_runner.py -g my_graph
"""

import argparse
import sys
import time
from typing import Optional


def connect(graph_name: str, host: str, port: int,
            username: Optional[str], password: Optional[str]):
    from falkordb import FalkorDB
    db = FalkorDB(host=host, port=port, username=username, password=password)
    return db.select_graph(graph_name)


def run_query(graph, query: str, verbose: bool = True):
    """Run a single query and print results."""
    query = query.strip()
    if not query or query.startswith("//") or query.startswith("--"):
        return

    t0 = time.time()
    try:
        result = graph.query(query)
        elapsed = time.time() - t0

        if result.result_set:
            # Print header from first row
            if hasattr(result, "header"):
                print("\t".join(str(h) for h in result.header))
                print("-" * 60)
            for row in result.result_set:
                print("\t".join(str(col) for col in row))

        stats_parts = []
        if hasattr(result, "nodes_created") and result.nodes_created:
            stats_parts.append(f"Nodes created: {result.nodes_created}")
        if hasattr(result, "relationships_created") and result.relationships_created:
            stats_parts.append(f"Rels created: {result.relationships_created}")
        if hasattr(result, "nodes_deleted") and result.nodes_deleted:
            stats_parts.append(f"Nodes deleted: {result.nodes_deleted}")
        if hasattr(result, "relationships_deleted") and result.relationships_deleted:
            stats_parts.append(f"Rels deleted: {result.relationships_deleted}")

        if verbose:
            rows_count = len(result.result_set) if result.result_set else 0
            stats_parts.append(f"{rows_count} row(s) in {elapsed:.3f}s")
            print(f"  ({', '.join(stats_parts)})")

    except Exception as e:
        print(f"❌ Error: {e}")


def run_file(graph, filepath: str):
    """Run all queries from a .cypher file (semicolon-delimited)."""
    with open(filepath, encoding="utf-8") as f:
        content = f.read()

    queries = [q.strip() for q in content.split(";") if q.strip()]
    print(f"📄 Running {len(queries)} queries from {filepath}\n")
    for i, q in enumerate(queries, 1):
        if q.startswith("//") or q.startswith("--"):
            continue
        print(f"[{i}/{len(queries)}] {q[:80]}{'...' if len(q) > 80 else ''}")
        run_query(graph, q)
        print()


def repl(graph, graph_name: str):
    """Interactive Cypher REPL."""
    print(f"🔮 FalkorDB REPL — graph: {graph_name}")
    print("   Type a Cypher query and press Enter. Use 'exit' or Ctrl-D to quit.\n")

    while True:
        try:
            query = input("cypher> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nBye!")
            break
        if query.lower() in ("exit", "quit", "\\q"):
            break
        if not query:
            continue
        run_query(graph, query)
        print()


def main():
    p = argparse.ArgumentParser(description="Run Cypher queries against FalkorDB.")
    p.add_argument("-g", "--graph", required=True, help="Graph name")
    p.add_argument("-H", "--host", default="localhost")
    p.add_argument("-p", "--port", type=int, default=6379)
    p.add_argument("-u", "--username", default=None)
    p.add_argument("-a", "--password", default=None)
    p.add_argument("-q", "--query", default=None, help="Single Cypher query")
    p.add_argument("-f", "--file", default=None, help="Path to .cypher file")
    args = p.parse_args()

    graph = connect(args.graph, args.host, args.port, args.username, args.password)

    if args.query:
        run_query(graph, args.query)
    elif args.file:
        run_file(graph, args.file)
    else:
        repl(graph, args.graph)


if __name__ == "__main__":
    main()
