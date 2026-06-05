"""
Small smoke benchmark for a local FalkorDB instance (no auth).

Phases (all timed):
  1. Bulk CREATE — N nodes via parameterized batched UNWIND.
  2. Bulk relationships — random edges between created nodes.
  3. Index creation — on Person.id.
  4. Point-lookup — N indexed lookups by id.
  5. 1-hop traversal — N MATCH (a)-[r]->(b) lookups.
  6. Aggregation — node/edge counts.

Reports total wall time and per-op latency for each phase.

Run:
    python examples/bench_basic.py                 # defaults: 10k nodes, 20k edges
    python examples/bench_basic.py --nodes 5000 --edges 10000 --lookups 1000
"""

from __future__ import annotations

import argparse
import random
import time
from contextlib import contextmanager

from falkordb import FalkorDB

GRAPH_NAME = "bench_basic"


@contextmanager
def timer(label: str, ops: int | None = None):
    start = time.perf_counter()
    yield
    elapsed = time.perf_counter() - start
    if ops:
        per = (elapsed / ops) * 1000
        rate = ops / elapsed
        print(f"  {label:<26} {elapsed:7.3f}s   {per:7.3f} ms/op   {rate:9.1f} ops/s")
    else:
        print(f"  {label:<26} {elapsed:7.3f}s")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="localhost")
    p.add_argument("--port", type=int, default=6379)
    p.add_argument("--nodes", type=int, default=10_000)
    p.add_argument("--edges", type=int, default=20_000)
    p.add_argument("--lookups", type=int, default=2_000)
    p.add_argument("--batch", type=int, default=1_000)
    args = p.parse_args()

    print(f"FalkorDB benchmark — {args.host}:{args.port}")
    print(f"  nodes={args.nodes}  edges={args.edges}  lookups={args.lookups}  batch={args.batch}\n")

    db = FalkorDB(host=args.host, port=args.port)
    db.flushdb()
    g = db.select_graph(GRAPH_NAME)

    rng = random.Random(42)

    # Phase 1 — bulk CREATE nodes
    print("[1] CREATE nodes")
    with timer("total", args.nodes):
        for start in range(0, args.nodes, args.batch):
            batch = [
                {"id": i, "name": f"person-{i}", "age": rng.randint(18, 80)}
                for i in range(start, min(start + args.batch, args.nodes))
            ]
            g.query(
                "UNWIND $rows AS row CREATE (:Person {id: row.id, name: row.name, age: row.age})",
                {"rows": batch},
            )

    # Phase 2 — bulk relationships
    print("\n[2] CREATE relationships")
    with timer("total", args.edges):
        for start in range(0, args.edges, args.batch):
            batch = [
                {"a": rng.randint(0, args.nodes - 1), "b": rng.randint(0, args.nodes - 1)}
                for _ in range(start, min(start + args.batch, args.edges))
            ]
            g.query(
                "UNWIND $rows AS row "
                "MATCH (a:Person {id: row.a}), (b:Person {id: row.b}) "
                "CREATE (a)-[:KNOWS]->(b)",
                {"rows": batch},
            )

    # Phase 3 — index
    print("\n[3] CREATE INDEX on :Person(id)")
    with timer("total"):
        g.query("CREATE INDEX FOR (p:Person) ON (p.id)")

    # Phase 4 — point lookups
    print("\n[4] Point lookups by indexed id")
    ids = [rng.randint(0, args.nodes - 1) for _ in range(args.lookups)]
    with timer("total", args.lookups):
        for i in ids:
            g.query("MATCH (p:Person {id: $id}) RETURN p", {"id": i})

    # Phase 5 — 1-hop traversal
    print("\n[5] 1-hop traversal")
    with timer("total", args.lookups):
        for i in ids:
            g.query(
                "MATCH (p:Person {id: $id})-[:KNOWS]->(n) RETURN n LIMIT 25",
                {"id": i},
            )

    # Phase 6 — aggregations
    print("\n[6] Aggregations")
    with timer("count nodes"):
        n = g.query("MATCH (n) RETURN count(n)").result_set[0][0]
    with timer("count edges"):
        m = g.query("MATCH ()-[r]->() RETURN count(r)").result_set[0][0]
    print(f"  nodes={n}  edges={m}")

    db.flushdb()


if __name__ == "__main__":
    main()
