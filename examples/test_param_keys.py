"""
End-to-end validator for FalkorDB parameter / map-key serialization.

What this exercises (relates to falkordb-py issue #211 and PR #212):

  * Round-tripping property bags whose keys are not valid Cypher identifiers
    (e.g. '@type', UUIDs with hyphens, reserved keywords, leading digits,
    whitespace, dots, colons, quotes, backslashes, unicode, emoji).
  * Persisting nodes built from such property bags via parameterized
    CREATE statements, then reading them back with MATCH ... RETURN
    properties(n) and verifying equality.
  * Inline-map literals built from the same dictionaries.
  * Client-side rejection of empty keys and keys containing a literal
    backtick (these cannot be expressed in FalkorDB's CYPHER header).

Usage:
    python examples/test_param_keys.py
    python examples/test_param_keys.py --host 127.0.0.1 --port 6379 \\
                                       --graph param_key_validator

Exit code is 0 if every case passes, 1 otherwise.

To validate the PR #212 fix specifically, install the client from the
branch first:
    pip install -U "git+https://github.com/FalkorDB/falkordb-py.git@fix/backtick-param-keys"
"""

from __future__ import annotations

import argparse
import sys
import traceback
from dataclasses import dataclass
from typing import Any, Callable

from falkordb import FalkorDB

# --- Test data ----------------------------------------------------------------

# Keys that should round-trip cleanly once the PR #212 fix is applied.
# Without the fix, every one of these (except 'plain') would raise a
# server-side parse error.
EDGE_CASE_KEYS: list[str] = [
    "plain",
    "@type",
    "0be6ffd7-3844-46a3-a699-bf3b77c573cd",
    "MATCH",
    "RETURN",
    "123abc",
    "1",
    "with space",
    "with.dot",
    "with:colon",
    'with"quote',
    "with\\backslash",
    "日本語",
    "🚀",
]

# Keys the client must reject up front (FalkorDB's CYPHER header parser
# does not support these — emitting them would produce a confusing
# server-side parse error).
INVALID_KEYS: list[str] = [
    "",
    "back`tick",
    "double``tick",
    "leading`",
    "`trailing",
]

REPRESENTATIVE_PROPS: dict[str, Any] = {
    "id": "079b2482-c885-45ef-ba01-0995d64c0ae9",
    "@type": "account",
    "display_name": "General Dynamics Ordnance and Tactical Systems",
    "domain_name": "gd-ots.com",
    "estimated_employee_count": 5000,
    "websites": ["gd-ots.com"],
    "0be6ffd7-3844-46a3-a699-bf3b77c573cd": 17.0,
    "811badf4-7f58-4ea6-b6d9-79c0185825cf": 0.0,
}


# --- Test runner --------------------------------------------------------------


@dataclass
class CaseResult:
    name: str
    passed: bool
    detail: str = ""


def run(name: str, fn: Callable[[], None]) -> CaseResult:
    try:
        fn()
    except Exception as e:  # noqa: BLE001
        return CaseResult(name, False, f"{type(e).__name__}: {e}")
    return CaseResult(name, True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[1].strip())
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--graph", default="param_key_validator")
    args = parser.parse_args()

    print(f"Connecting to FalkorDB at {args.host}:{args.port} ...")
    db = FalkorDB(host=args.host, port=args.port)
    db.flushdb()
    g = db.select_graph(args.graph)

    results: list[CaseResult] = []

    # ------------------------------------------------------------------ #
    # 1. Each edge-case key works as a top-level parameter name.
    # ------------------------------------------------------------------ #
    def make_param_case(key: str) -> Callable[[], None]:
        def _case() -> None:
            quoted = key.replace("`", "``")
            res = g.query(f"RETURN $`{quoted}`", {key: "ok"})
            assert res.result_set == [["ok"]], res.result_set

        return _case

    for k in EDGE_CASE_KEYS:
        results.append(run(f"param-name {k!r}", make_param_case(k)))

    # ------------------------------------------------------------------ #
    # 2. Each edge-case key works as a key inside a $props dict literal.
    # ------------------------------------------------------------------ #
    def make_map_case(key: str) -> Callable[[], None]:
        def _case() -> None:
            res = g.query("RETURN $props", {"props": {key: 42}})
            assert res.result_set == [[{key: 42}]], res.result_set

        return _case

    for k in EDGE_CASE_KEYS:
        results.append(run(f"map-key   {k!r}", make_map_case(k)))

    # ------------------------------------------------------------------ #
    # 3. Persist a representative property bag, read it back, compare.
    #    Uses CREATE + SET n = $props rather than CREATE (n $props),
    #    because the inlined-properties form is not supported on every
    #    FalkorDB build. The SET-from-parameter form is the property bag
    #    contract our PR fix is about. We exclude list-valued properties
    #    here because graph property values must be primitives or arrays
    #    of primitives (the round-trip with lists is exercised in case 4).
    # ------------------------------------------------------------------ #
    SCALAR_PROPS = {
        k: v
        for k, v in REPRESENTATIVE_PROPS.items()
        if not isinstance(v, dict)
    }

    def persist_case() -> None:
        g.query(
            "CREATE (n:account) SET n = $props",
            {"props": SCALAR_PROPS},
        )
        res = g.query(
            "MATCH (n:account {id: $id}) RETURN properties(n)",
            {"id": SCALAR_PROPS["id"]},
        )
        assert len(res.result_set) == 1, res.result_set
        got = dict(res.result_set[0][0])
        assert got == SCALAR_PROPS, (got, SCALAR_PROPS)

    results.append(run("persist & read-back representative props", persist_case))

    # ------------------------------------------------------------------ #
    # 4. Round-trip the FULL property bag (including lists) through
    #    RETURN $props — this is the exact failure mode reported in
    #    issue #211 ("if I pass that dictionary back in as parameter").
    # ------------------------------------------------------------------ #
    def round_trip_case() -> None:
        res = g.query("RETURN $props", {"props": REPRESENTATIVE_PROPS})
        got = dict(res.result_set[0][0])
        assert got == REPRESENTATIVE_PROPS, (got, REPRESENTATIVE_PROPS)

    results.append(run("issue #211 round-trip full property bag", round_trip_case))

    # ------------------------------------------------------------------ #
    # 5. Nested dict-in-list-in-dict — recursion must quote at every level.
    # ------------------------------------------------------------------ #
    def nested_case() -> None:
        nested = {
            "outer key": [
                {"inner@key": 1, "another-key": 2},
                {"plain": 3},
            ]
        }
        res = g.query("RETURN $nested", {"nested": nested})
        assert res.result_set == [[nested]], res.result_set

    results.append(run("nested dict-in-list-in-dict", nested_case))

    # ------------------------------------------------------------------ #
    # 6. Invalid keys must be rejected client-side with ValueError.
    # ------------------------------------------------------------------ #
    def make_reject_case(key: str) -> Callable[[], None]:
        def _case() -> None:
            try:
                g.query("RETURN 1", {key: "x"})
            except ValueError:
                return  # expected
            raise AssertionError(f"expected ValueError for key {key!r}")

        return _case

    for k in INVALID_KEYS:
        results.append(run(f"reject param-name {k!r}", make_reject_case(k)))

    def make_reject_map_case(key: str) -> Callable[[], None]:
        def _case() -> None:
            try:
                g.query("RETURN $p", {"p": {key: 1}})
            except ValueError:
                return
            raise AssertionError(f"expected ValueError for nested key {key!r}")

        return _case

    for k in INVALID_KEYS:
        results.append(run(f"reject map-key   {k!r}", make_reject_map_case(k)))

    # ------------------------------------------------------------------ #
    # Report
    # ------------------------------------------------------------------ #
    passed = sum(r.passed for r in results)
    failed = len(results) - passed

    width = max(len(r.name) for r in results) + 2
    for r in results:
        mark = "PASS" if r.passed else "FAIL"
        print(f"  [{mark}] {r.name.ljust(width)} {r.detail}")

    print()
    print(f"Total: {len(results)}    Passed: {passed}    Failed: {failed}")

    db.flushdb()
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:  # noqa: BLE001
        traceback.print_exc()
        sys.exit(2)
