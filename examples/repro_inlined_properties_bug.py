"""
Minimal repro for FalkorDB bug:
  "Encountered unhandled type in inlined properties"
  when a Cypher parameter is used as inlined pattern properties.

Run:
    python examples/repro_inlined_properties_bug.py
"""

from falkordb import FalkorDB


def reset_graph(db: FalkorDB, graph_name: str) -> None:
    """Drop only this script's own graph.

    Never use flushdb() here: it would wipe every graph on the instance.
    """
    try:
        db.select_graph(graph_name).delete()
    except Exception:
        pass  # graph does not exist yet, nothing to drop

GRAPH_NAME = "repro_inlined_properties_bug"
PROPS = {"name": "Alice", "age": 30}


def try_query(graph, label, query, params=None):
    """Run a query, print PASS/FAIL with the server's error if any."""
    try:
        graph.query(query, params)
        print(f"  PASS  {label:<30} {query}")
    except Exception as e:
        print(f"  FAIL  {label:<30} {query}")
        print(f"        -> {e}")


def main():
    db = FalkorDB(host="localhost", port=6379)
    reset_graph(db, GRAPH_NAME)
    graph = db.select_graph(GRAPH_NAME)

    print("These should ALL succeed but the server rejects the parameter form:\n")

    print("[1] Literal inline map (works):")
    try_query(graph, "literal map", "CREATE (n:Person {name: 'Alice', age: 30})")

    print("\n[2] Same map via parameter (BUG: fails):")
    try_query(graph, "CREATE (n $props)", "CREATE (n:Person $props)", {"props": PROPS})
    try_query(graph, "MERGE  (n $props)", "MERGE (n:Person $props)", {"props": PROPS})
    try_query(graph, "MATCH  (n $props)", "MATCH (n:Person $props) RETURN n", {"props": PROPS})
    try_query(
        graph,
        "edge   (a)-[r:R $props]->(b)",
        "CREATE (a:X)-[r:KNOWS $props]->(b:Y)",
        {"props": PROPS},
    )

    print("\n[3] Workaround — use SET instead of inlined properties (works):")
    try_query(
        graph,
        "CREATE + SET n = $props",
        "CREATE (n:Person) SET n = $props",
        {"props": PROPS},
    )

    reset_graph(db, GRAPH_NAME)


if __name__ == "__main__":
    main()
