#!/usr/bin/env python3
"""
FalkorDB Upgrade Validator
==========================
Seeds a deterministic dataset into FalkorDB and validates that it survives
an instance upgrade.

Typical usage:
    # Right after creating the instance — seed + sanity-verify
    python upgrade_validator.py --host my.falkordb.cloud -p 6379 \
        -a "$FALKORDB_PASSWORD" --mode write

    # After upgrading the instance — verify everything is intact
    python upgrade_validator.py --host my.falkordb.cloud -p 6379 \
        -a "$FALKORDB_PASSWORD" --mode verify

SSL is ON by default (cloud-style). Use --no-ssl for plain TCP.

Exit code is 0 on success, 1 on any validation mismatch.
"""

import argparse
import hashlib
import json
import os
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Tuple

from falkordb import FalkorDB


GRAPH_NAME_DEFAULT = "upgrade_validation"


# ---------------------------------------------------------------------------
# Deterministic dataset
# ---------------------------------------------------------------------------
# Keep this stable across runs — verify recomputes expected values from it.

PEOPLE: List[Dict[str, Any]] = [
    {"id": 1, "name": "Alice",   "age": 30, "score": 9.5,  "active": True,  "tags": ["admin", "eu"]},
    {"id": 2, "name": "Bob",     "age": 25, "score": 7.25, "active": True,  "tags": ["user"]},
    {"id": 3, "name": "Carol",   "age": 41, "score": 8.0,  "active": False, "tags": ["user", "us"]},
    {"id": 4, "name": "Dan",     "age": 19, "score": 6.6,  "active": True,  "tags": []},
    {"id": 5, "name": "Eve",     "age": 55, "score": 9.99, "active": False, "tags": ["admin"]},
]

COMPANIES: List[Dict[str, Any]] = [
    {"id": 100, "name": "Acme",     "founded": 1947},
    {"id": 200, "name": "Globex",   "founded": 1989},
    {"id": 300, "name": "Initech",  "founded": 1996},
]

# (person_id, person_id, since)
KNOWS: List[Tuple[int, int, int]] = [
    (1, 2, 2010),
    (1, 3, 2012),
    (2, 3, 2015),
    (3, 4, 2018),
    (4, 5, 2020),
    (5, 1, 2021),
]

# (person_id, company_id, role)
WORKS_AT: List[Tuple[int, int, str]] = [
    (1, 100, "engineer"),
    (2, 100, "manager"),
    (3, 200, "engineer"),
    (4, 300, "intern"),
    (5, 200, "cto"),
]

INDEXES = [
    "CREATE INDEX FOR (p:Person) ON (p.id)",
    "CREATE INDEX FOR (c:Company) ON (c.id)",
]


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

def connect(host: str, port: int, username: str, password: str, ssl: bool, graph: str):
    db = FalkorDB(
        host=host,
        port=port,
        username=username,
        password=password,
        ssl=ssl,
    )
    return db.select_graph(graph)


# ---------------------------------------------------------------------------
# Write
# ---------------------------------------------------------------------------

def seed(g) -> None:
    print(f"🧹 Dropping graph (if exists) ...")
    try:
        g.delete()
    except Exception as e:
        # OK if it doesn't exist
        print(f"  (skip) {e}")

    print(f"📥 Creating {len(PEOPLE)} Person nodes ...")
    g.query(
        "UNWIND $rows AS r "
        "CREATE (:Person {id: r.id, name: r.name, age: r.age, "
        "score: r.score, active: r.active, tags: r.tags})",
        {"rows": PEOPLE},
    )

    print(f"📥 Creating {len(COMPANIES)} Company nodes ...")
    g.query(
        "UNWIND $rows AS r "
        "CREATE (:Company {id: r.id, name: r.name, founded: r.founded})",
        {"rows": COMPANIES},
    )

    print(f"🔗 Creating {len(KNOWS)} :KNOWS edges ...")
    g.query(
        "UNWIND $rows AS r "
        "MATCH (a:Person {id: r.a}), (b:Person {id: r.b}) "
        "CREATE (a)-[:KNOWS {since: r.since}]->(b)",
        {"rows": [{"a": a, "b": b, "since": s} for a, b, s in KNOWS]},
    )

    print(f"🔗 Creating {len(WORKS_AT)} :WORKS_AT edges ...")
    g.query(
        "UNWIND $rows AS r "
        "MATCH (p:Person {id: r.p}), (c:Company {id: r.c}) "
        "CREATE (p)-[:WORKS_AT {role: r.role}]->(c)",
        {"rows": [{"p": p, "c": c, "role": role} for p, c, role in WORKS_AT]},
    )

    for q in INDEXES:
        print(f"🔧 {q}")
        try:
            g.query(q)
        except Exception as e:
            # already exists is fine
            print(f"  (skip) {e}")

    print("✅ Seed complete")


# ---------------------------------------------------------------------------
# Verify
# ---------------------------------------------------------------------------

def _scalar(g, q: str, params: Dict[str, Any] = None) -> Any:
    res = g.query(q, params or {})
    return res.result_set[0][0] if res.result_set else None


def _checksum(rows: List[List[Any]]) -> str:
    payload = json.dumps(rows, sort_keys=True, default=str).encode()
    return hashlib.sha256(payload).hexdigest()


def _expected_person_checksum() -> str:
    rows = [
        [p["id"], p["name"], p["age"], p["score"], p["active"], list(p["tags"])]
        for p in sorted(PEOPLE, key=lambda x: x["id"])
    ]
    return _checksum(rows)


def _expected_company_checksum() -> str:
    rows = [
        [c["id"], c["name"], c["founded"]]
        for c in sorted(COMPANIES, key=lambda x: x["id"])
    ]
    return _checksum(rows)


def _expected_knows_checksum() -> str:
    rows = sorted([[a, b, s] for a, b, s in KNOWS])
    return _checksum(rows)


def _expected_works_at_checksum() -> str:
    rows = sorted([[p, c, r] for p, c, r in WORKS_AT])
    return _checksum(rows)


def verify(g) -> bool:
    failures: List[str] = []

    def check(label: str, actual: Any, expected: Any):
        ok = actual == expected
        flag = "✅" if ok else "❌"
        print(f"  {flag} {label}: got={actual!r} expected={expected!r}")
        if not ok:
            failures.append(label)

    print("═══ Counts ═══")
    check("Person count",   _scalar(g, "MATCH (p:Person)  RETURN count(p)"), len(PEOPLE))
    check("Company count",  _scalar(g, "MATCH (c:Company) RETURN count(c)"), len(COMPANIES))
    check("KNOWS count",    _scalar(g, "MATCH ()-[r:KNOWS]->()    RETURN count(r)"), len(KNOWS))
    check("WORKS_AT count", _scalar(g, "MATCH ()-[r:WORKS_AT]->() RETURN count(r)"), len(WORKS_AT))

    print("\n═══ Aggregates ═══")
    check("sum(age)",
          _scalar(g, "MATCH (p:Person) RETURN sum(p.age)"),
          sum(p["age"] for p in PEOPLE))
    check("max(score)",
          _scalar(g, "MATCH (p:Person) RETURN max(p.score)"),
          max(p["score"] for p in PEOPLE))
    check("count(active)",
          _scalar(g, "MATCH (p:Person) WHERE p.active = true RETURN count(p)"),
          sum(1 for p in PEOPLE if p["active"]))
    check("min(founded)",
          _scalar(g, "MATCH (c:Company) RETURN min(c.founded)"),
          min(c["founded"] for c in COMPANIES))

    print("\n═══ Content checksums ═══")
    person_rows = g.query(
        "MATCH (p:Person) RETURN p.id, p.name, p.age, p.score, p.active, p.tags "
        "ORDER BY p.id"
    ).result_set
    check("Person checksum", _checksum(person_rows), _expected_person_checksum())

    company_rows = g.query(
        "MATCH (c:Company) RETURN c.id, c.name, c.founded ORDER BY c.id"
    ).result_set
    check("Company checksum", _checksum(company_rows), _expected_company_checksum())

    knows_rows = g.query(
        "MATCH (a:Person)-[r:KNOWS]->(b:Person) "
        "RETURN a.id, b.id, r.since ORDER BY a.id, b.id"
    ).result_set
    check("KNOWS checksum", _checksum(knows_rows), _expected_knows_checksum())

    works_rows = g.query(
        "MATCH (p:Person)-[r:WORKS_AT]->(c:Company) "
        "RETURN p.id, c.id, r.role ORDER BY p.id, c.id"
    ).result_set
    check("WORKS_AT checksum", _checksum(works_rows), _expected_works_at_checksum())

    print("\n═══ Traversal sanity ═══")
    # Friend-of-friend for Alice (id=1): expected via KNOWS graph
    adj: Dict[int, set] = {p["id"]: set() for p in PEOPLE}
    for a, b, _ in KNOWS:
        adj[a].add(b)
    expected_fof = sorted({x for nbr in adj[1] for x in adj[nbr] if x != 1})
    actual_fof = [
        row[0] for row in g.query(
            "MATCH (a:Person {id: 1})-[:KNOWS]->()-[:KNOWS]->(f:Person) "
            "WHERE f.id <> 1 RETURN DISTINCT f.id ORDER BY f.id"
        ).result_set
    ]
    check("Alice FoF ids", actual_fof, expected_fof)

    print("\n═══ Indexes ═══")
    idx_rows = g.query("CALL db.indexes()").result_set
    idx_labels = sorted({row[0] for row in idx_rows})
    check("indexed labels", idx_labels, ["Company", "Person"])

    print()
    if failures:
        print(f"❌ {len(failures)} check(s) failed: {failures}")
        return False
    print("✅ All checks passed — data is intact")
    return True


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> int:
    p = argparse.ArgumentParser(description="FalkorDB upgrade validator (seed + verify).")
    p.add_argument("--host", default=os.environ.get("FALKORDB_HOST", "localhost"))
    p.add_argument("-p", "--port", type=int,
                   default=int(os.environ.get("FALKORDB_PORT", "6379")))
    p.add_argument("-u", "--username", default=os.environ.get("FALKORDB_USERNAME"))
    p.add_argument("-a", "--password", default=os.environ.get("FALKORDB_PASSWORD"))
    p.add_argument("-g", "--graph", default=GRAPH_NAME_DEFAULT)
    p.add_argument("--mode", choices=["write", "verify", "all", "loop"], default="all",
                   help="write = seed only, verify = check once, "
                        "loop = verify continuously (Ctrl+C to stop), "
                        "all = seed then verify once")
    p.add_argument("--interval", type=float, default=1.0,
                   help="Seconds between iterations in loop mode (default: 1.0)")
    ssl_group = p.add_mutually_exclusive_group()
    ssl_group.add_argument("--ssl", dest="ssl", action="store_true", default=True,
                           help="Use SSL/TLS (default)")
    ssl_group.add_argument("--no-ssl", dest="ssl", action="store_false",
                           help="Disable SSL/TLS")
    args = p.parse_args()

    scheme = "rediss" if args.ssl else "redis"
    print(f"🔌 Connecting to {scheme}://{args.host}:{args.port}  graph={args.graph}")

    if args.mode == "loop":
        return run_loop(args, scheme)

    g = connect(args.host, args.port, args.username, args.password, args.ssl, args.graph)

    if args.mode in ("write", "all"):
        print("\n──── SEED ────")
        seed(g)

    if args.mode in ("verify", "all"):
        print("\n──── VERIFY ────")
        ok = verify(g)
        return 0 if ok else 1

    return 0


def run_loop(args, scheme: str) -> int:
    print(f"🔁 Loop mode — verifying every {args.interval}s. Ctrl+C to stop.\n")
    iteration = 0
    ok_count = 0
    fail_count = 0
    err_count = 0
    g = None

    try:
        while True:
            iteration += 1
            ts = datetime.now().strftime("%H:%M:%S")
            print(f"───── iter {iteration} @ {ts} "
                  f"(ok={ok_count} fail={fail_count} err={err_count}) ─────")
            try:
                if g is None:
                    g = connect(args.host, args.port, args.username,
                                args.password, args.ssl, args.graph)
                if verify(g):
                    ok_count += 1
                else:
                    fail_count += 1
            except KeyboardInterrupt:
                raise
            except Exception as e:
                err_count += 1
                print(f"  ⚠️  transient error: {type(e).__name__}: {e}")
                # Force reconnect on next iteration
                g = None
            print()
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print(f"\n🛑 Stopped after {iteration} iterations: "
              f"ok={ok_count} fail={fail_count} err={err_count}")
        return 0 if fail_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
