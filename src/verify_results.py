"""Fail unless MapReduce and DuckDB counts match byte-for-byte logically."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path


def canonical_digest(path: Path) -> tuple[str, dict]:
    document = json.loads(path.read_text(encoding="utf-8"))
    canonical = json.dumps(document, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(canonical).hexdigest(), document


def compare_results(mapreduce: Path, duckdb: Path) -> dict:
    checks = []
    for name in ("skills", "pairs", "triples"):
        mr_hash, mr = canonical_digest(mapreduce / f"{name}.json")
        db_hash, db = canonical_digest(duckdb / f"{name}.json")
        checks.append(
            {
                "stack": name,
                "equal": mr == db,
                "different_fields": [
                    key for key in sorted(mr.keys() | db.keys())
                    if key not in mr or key not in db or mr[key] != db[key]
                ],
                "sha256_mapreduce": mr_hash,
                "sha256_duckdb": db_hash,
                "unique_combinations": mr["unique_combinations"],
                "total_emissions": sum(item["count"] for item in mr["counts"]),
            }
        )
    result = {"all_equal": all(item["equal"] for item in checks), "checks": checks}
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("mapreduce", type=Path)
    parser.add_argument("duckdb", type=Path)
    parser.add_argument("--out", type=Path)
    args = parser.parse_args()
    result = compare_results(args.mapreduce, args.duckdb)
    rendered = json.dumps(result, ensure_ascii=False, indent=2) + "\n"
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    if not result["all_equal"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

