"""Independently verify the DevOps variant filter in DuckDB against raw S3 data."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import duckdb

from common import S3Config
from duckdb_support import configure_s3, sql_string


VARIANT_SQL_PATTERN = (
    "dev[[:space:]-]*(sec[[:space:]-]*)?ops|девопс|site reliability|"
    "(^|[^a-z])sre([^a-z]|$)|platform engineer"
)


def ids_hash(values: list[str]) -> str:
    payload = "\n".join(sorted(values)).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw-key", default="raw/it_vacancies_2024.csv")
    parser.add_argument("--variant-key", default="variants/devops/vacancies.ndjson")
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()

    config = S3Config.from_env()
    raw_uri = f"s3://{config.bucket}/{args.raw_key}"
    variant_uri = f"s3://{config.bucket}/{args.variant_key}"

    con = duckdb.connect(database=":memory:")
    configure_s3(con, config)

    con.execute(
        f"""
        CREATE TEMP TABLE sql_variant_ids AS
        SELECT DISTINCT CAST(vacancy_id AS VARCHAR) AS vacancy_id
        FROM read_csv_auto({sql_string(raw_uri)}, header=true, all_varchar=true)
        WHERE regexp_matches(
            lower(coalesce(name, '')),
            {sql_string(VARIANT_SQL_PATTERN)}
        )
        """
    )
    con.execute(
        f"""
        CREATE TEMP TABLE prepared_variant_ids AS
        SELECT DISTINCT CAST(vacancy_id AS VARCHAR) AS vacancy_id
        FROM read_ndjson_auto({sql_string(variant_uri)})
        """
    )

    sql_ids = [row[0] for row in con.execute("SELECT vacancy_id FROM sql_variant_ids").fetchall()]
    prepared_ids = [
        row[0] for row in con.execute("SELECT vacancy_id FROM prepared_variant_ids").fetchall()
    ]
    sql_only = con.execute(
        "SELECT count(*) FROM (SELECT * FROM sql_variant_ids EXCEPT SELECT * FROM prepared_variant_ids)"
    ).fetchone()[0]
    prepared_only = con.execute(
        "SELECT count(*) FROM (SELECT * FROM prepared_variant_ids EXCEPT SELECT * FROM sql_variant_ids)"
    ).fetchone()[0]
    con.close()

    result = {
        "raw_s3_object": raw_uri,
        "variant_s3_object": variant_uri,
        "sql_condition": VARIANT_SQL_PATTERN,
        "duckdb_selected": len(sql_ids),
        "prepared_selected": len(prepared_ids),
        "sql_only": sql_only,
        "prepared_only": prepared_only,
        "exact_id_set_match": sql_only == 0 and prepared_only == 0,
        "sql_ids_sha256": ids_hash(sql_ids),
        "prepared_ids_sha256": ids_hash(prepared_ids),
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
