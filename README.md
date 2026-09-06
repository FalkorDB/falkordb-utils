# FalkorDB Utilities

Python utility scripts for working with [FalkorDB](https://www.falkordb.com/) — a graph database built on Redis.

## Setup

```bash
pip install -r requirements.txt
```

## Examples

### 1. Bulk CSV Loader (`bulk_csv_loader.py`)

Load a folder of CSV files into a graph. Nodes are loaded first, then edges.

```bash
# Load the sample IT network dataset
python examples/bulk_csv_loader.py ./data/sample_network -g infra

# Custom connection + batch size
python examples/bulk_csv_loader.py ./data/sample_network -g infra -h myhost -p 6379 -b 5000
```

**CSV conventions:**
- **Node CSVs** — must have an `id` column. `labels` column is optional (derived from filename: `nodes_Person.csv` → label `Person`)
- **Edge CSVs** — must have `source`, `target`, `type` columns. Optional: `source_label`, `target_label`

**Also usable as a library:**
```python
from examples.bulk_csv_loader import BulkCSVLoader

loader = BulkCSVLoader(graph_name="infra", host="localhost")
loader.load_folder("./data/sample_network")
loader.load_single_csv("./extra_nodes.csv", kind="node")
```

### 2. Graph Schema Inspector (`graph_schema_inspector.py`)

Introspect labels, relationship types, property keys, and indexes.

```bash
python examples/graph_schema_inspector.py -g infra
```

### 3. Graph Exporter (`graph_exporter.py`)

Export graph data back to CSV — one file per label/rel type.

```bash
python examples/graph_exporter.py -g infra -o ./exported/
python examples/graph_exporter.py -g infra -o ./exported/ --labels Person,Company
```

### 4. Cypher Query Runner (`cypher_runner.py`)

Run queries from a file, one-liner, or interactive REPL.

```bash
# One-liner
python examples/cypher_runner.py -g infra -q "MATCH (n) RETURN count(n)"

# Run a .cypher file
python examples/cypher_runner.py -g infra -f queries.cypher

# Interactive REPL
python examples/cypher_runner.py -g infra
```

### 5. Memory Scanner (`scan_memory.py`)

Report memory usage per key, sorted by size.

```bash
python examples/scan_memory.py
python examples/scan_memory.py --host myhost:6379 --top 50
```

### 6. Health Check (`health_check.py`)

Quick connectivity test + server stats + loaded graphs.

```bash
python examples/health_check.py
python examples/health_check.py --host myhost -p 6379
```

### 7. Random Graph Generator (`create_random_graphs.py`)

Create many graphs, each with a **random** node count in a range. Defaults to
10,000 graphs of 10,000–100,000 nodes each (smoke-test small first!).

```bash
# Smoke test: 5 small graphs
python examples/create_random_graphs.py --num-graphs 5 --min-nodes 100 --max-nodes 500

# Full default workload: 10,000 graphs x random 10k-100k nodes
python examples/create_random_graphs.py

# Custom range / connection
python examples/create_random_graphs.py --num-graphs 100 \
    --min-nodes 50000 --max-nodes 80000 --prefix demo --host myhost:6379 -a secret
```

Nodes are created server-side in batches via `UNWIND range(...) CREATE`. Use
`--start-index` to resume an interrupted run; `--seed` makes node counts
reproducible.

### 8. Graph Memory Reporter (`report_graph_memory.py`)

Walk every graph (optionally filtered by prefix) and report memory usage via
`GRAPH.MEMORY USAGE` (MB) and `MEMORY USAGE` (bytes). Writes `memory.csv`,
`detail.txt`, and a `summary.txt` (totals, avg/min/max, top consumers).

```bash
# Report on all graphs
python examples/report_graph_memory.py

# Only the graphs created above, custom output dir
python examples/report_graph_memory.py --prefix rndgraph --out-dir ./report
```

## Sample Data

| Dataset | Path | Description |
|---------|------|-------------|
| IT Network | `data/sample_network/` | ~30 node/edge CSVs: machines, interfaces, applications, processes, etc. |
| Family Tree | `data/sample_family_tree/` | 59-row family tree with names, dates, relationships |

## Java Examples

| Example | Path | Description |
|---------|------|-------------|
| Replica read routing | `java-examples/replica-read-routing/` | Connection factory that sends writes to the primary and spreads reads across replicas. Covers read preferences, staleness, pool sizing, and verifying which node served a read. Requires jfalkordb 0.11.1+ |

## Internal

`internal/README.md` holds FalkorDB internal runbooks, including how to reproduce the replica read
benchmark on FalkorDB Cloud and which results are safe to quote. Not customer facing.
