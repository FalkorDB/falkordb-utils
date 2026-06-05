#!/usr/bin/env python3
"""
Bulk CSV Loader for FalkorDB
============================
Loads nodes and edges from CSV files into a FalkorDB graph using
batched UNWIND+MERGE queries for high throughput.

CSV Conventions:
  Node CSV  — must have an `id` column; optional `labels` column.
              Filename like `nodes_Person.csv` auto-derives the label.
  Edge CSV  — must have `source`, `target`, `type` columns.
              Optional `source_label` / `target_label` for faster matching.

Usage:
    # Load a whole folder of CSVs
    python bulk_csv_loader.py ./data/sample_network -g infra

    # Custom connection
    python bulk_csv_loader.py ./data/sample_network -g infra -h myhost -p 6379 -u user -a pass

    # Larger batches for big datasets
    python bulk_csv_loader.py ./data/sample_network -g infra -b 5000
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────

_LABEL_SAFE = re.compile(r"[^0-9A-Za-z_]")


def sanitize_label(raw: str) -> str:
    """Strip characters FalkorDB doesn't allow in labels."""
    s = (raw or "").strip().replace(":", "_")
    s = _LABEL_SAFE.sub("_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "Unknown"


def split_labels(raw: Optional[str]) -> List[str]:
    if not raw:
        return []
    parts = re.split(r"[;|,]", str(raw))
    seen: set[str] = set()
    out: List[str] = []
    for p in parts:
        lbl = sanitize_label(p)
        if lbl and lbl not in seen:
            out.append(lbl)
            seen.add(lbl)
    return out


def coerce(v: Any) -> Any:
    """Best-effort CSV string → Python type."""
    if v is None:
        return None
    if isinstance(v, (int, float, bool)):
        return v
    s = str(v).strip()
    if s == "":
        return None
    low = s.lower()
    if low in ("true", "false"):
        return low == "true"
    if re.fullmatch(r"[-+]?\d+", s):
        try:
            return int(s)
        except ValueError:
            return s
    if re.fullmatch(r"[-+]?\d*\.\d+", s):
        try:
            return float(s)
        except ValueError:
            return s
    return s


def read_csv(path: str) -> Tuple[List[str], List[Dict[str, Any]]]:
    with open(path, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []
        rows = [
            {k.strip(): coerce(v) for k, v in row.items() if k is not None}
            for row in reader
        ]
    return headers, rows


def batched(items: list, size: int) -> Iterable[list]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


# ──────────────────────────────────────────────
# Loader
# ──────────────────────────────────────────────

@dataclass(frozen=True)
class _NodeKey:
    labels: Tuple[str, ...]


@dataclass(frozen=True)
class _EdgeKey:
    src_labels: Tuple[str, ...]
    tgt_labels: Tuple[str, ...]
    rel_type: str


class BulkCSVLoader:
    """Load a folder of CSV files into a FalkorDB graph."""

    def __init__(self, graph_name: str, host: str = "localhost", port: int = 6379,
                 username: str | None = None, password: str | None = None,
                 batch_size: int = 1000):
        from falkordb import FalkorDB
        self.db = FalkorDB(host=host, port=port, username=username, password=password)
        self.graph = self.db.select_graph(graph_name)
        self.batch_size = batch_size

    # ── public API ──

    def load_folder(self, folder: str) -> dict:
        """Load all CSVs in *folder*. Returns stats dict."""
        csvs = sorted(f for f in os.listdir(folder) if f.lower().endswith(".csv"))
        node_files, edge_files = [], []
        for name in csvs:
            path = os.path.join(folder, name)
            hdrs, _ = read_csv(path)
            h = {c.lower() for c in hdrs}
            if {"source", "target", "type"}.issubset(h):
                edge_files.append(path)
            elif "id" in h:
                node_files.append(path)
            else:
                print(f"⚠️  Skipping unrecognized CSV: {name}")

        print(f"📂 Found {len(node_files)} node + {len(edge_files)} edge CSV(s)")
        stats = {"nodes": 0, "edges": 0, "errors": 0}

        for path in node_files:
            stats["nodes"] += self._load_nodes(path)
        for path in edge_files:
            stats["edges"] += self._load_edges(path)

        print(f"✅ Done — {stats['nodes']} nodes, {stats['edges']} edges loaded")
        return stats

    def load_single_csv(self, path: str, kind: str = "auto") -> int:
        """Load a single CSV. *kind* is 'node', 'edge', or 'auto'."""
        hdrs, _ = read_csv(path)
        h = {c.lower() for c in hdrs}
        if kind == "auto":
            kind = "edge" if {"source", "target", "type"}.issubset(h) else "node"
        return self._load_nodes(path) if kind == "node" else self._load_edges(path)

    # ── internals ──

    def _load_nodes(self, path: str) -> int:
        name = os.path.basename(path)
        hdrs, rows = read_csv(path)
        if not rows:
            return 0

        low = {h.lower(): h for h in hdrs}
        lbl_col = low.get("labels") or low.get("label")

        groups: Dict[_NodeKey, List[dict]] = {}
        for row in rows:
            nid = row.get("id")
            if nid is None:
                continue
            raw = row.get(lbl_col) if lbl_col else None
            labels = split_labels(raw) or split_labels(
                os.path.splitext(name)[0].replace("nodes_", "")
            ) or ["Node"]
            props = {k: v for k, v in row.items()
                     if k not in ("id", lbl_col) and v is not None}
            key = _NodeKey(tuple(labels))
            groups.setdefault(key, []).append({"id": nid, "props": props})

        total = 0
        for key, items in groups.items():
            lbl = ":" + ":".join(key.labels)
            q = f"UNWIND $rows AS row MERGE (n{lbl} {{id: row.id}}) SET n += row.props"
            total += self._run(q, items, f"nodes {lbl} ({name})")
        return total

    def _load_edges(self, path: str) -> int:
        name = os.path.basename(path)
        hdrs, rows = read_csv(path)
        if not rows:
            return 0

        low = {h.lower(): h for h in hdrs}
        sl = low.get("source_label") or low.get("source_labels")
        tl = low.get("target_label") or low.get("target_labels")

        groups: Dict[_EdgeKey, List[dict]] = {}
        for row in rows:
            src, tgt = row.get("source"), row.get("target")
            if src is None or tgt is None:
                continue
            rel = sanitize_label(str(row.get("type") or "RELATED_TO"))
            sl_list = split_labels(row.get(sl)) if sl else []
            tl_list = split_labels(row.get(tl)) if tl else []
            props = {k: v for k, v in row.items()
                     if k not in ("source", "target", "type", sl, tl) and v is not None}
            key = _EdgeKey(tuple(sl_list), tuple(tl_list), rel)
            groups.setdefault(key, []).append({"source": src, "target": tgt, "props": props})

        total = 0
        for key, items in groups.items():
            s = ":" + ":".join(key.src_labels) if key.src_labels else ""
            t = ":" + ":".join(key.tgt_labels) if key.tgt_labels else ""
            q = (f"UNWIND $rows AS row "
                 f"MATCH (s{s} {{id: row.source}}) "
                 f"MATCH (t{t} {{id: row.target}}) "
                 f"MERGE (s)-[r:{key.rel_type}]->(t) "
                 f"SET r += row.props")
            total += self._run(q, items, f"edges :{key.rel_type} ({name})")
        return total

    def _run(self, cypher: str, rows: list, label: str) -> int:
        loaded = 0
        t0 = time.time()
        for batch in batched(rows, self.batch_size):
            self.graph.query(cypher, {"rows": batch})
            loaded += len(batch)
        elapsed = time.time() - t0
        print(f"  ✅ {loaded:,} {label}  ({elapsed:.1f}s)")
        return loaded


# ──────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(
        description="Bulk-load CSV files into FalkorDB (MERGE-based upsert).",
        add_help=False,
    )
    p.add_argument("--help", action="help")
    p.add_argument("csv_folder", help="Folder with node/edge CSVs")
    p.add_argument("-g", "--graph", required=True, help="Graph name")
    p.add_argument("-h", "--host", default="localhost")
    p.add_argument("-p", "--port", type=int, default=6379)
    p.add_argument("-u", "--username", default=None)
    p.add_argument("-a", "--password", default=None)
    p.add_argument("-b", "--batch-size", type=int, default=1000)
    args = p.parse_args()

    loader = BulkCSVLoader(
        graph_name=args.graph, host=args.host, port=args.port,
        username=args.username, password=args.password,
        batch_size=args.batch_size,
    )
    loader.load_folder(args.csv_folder)


if __name__ == "__main__":
    main()
