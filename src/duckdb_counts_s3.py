"""DuckDB-only counterpart to MapReduce: the same three counts read directly from RustFS."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import duckdb

from common import S3Config
from duckdb_analysis import count_document, sql_string


def run(args) -> dict:
    config = S3Config.from_env()
    endpoint = config.endpoint.removeprefix("http://").removeprefix("https://")
    s3_uri = f"s3://{config.bucket}/{args.key}"
    started = time.perf_counter()
    con = duckdb.connect(database=":memory:")
    try:
        con.execute("LOAD httpfs")
        con.execute(f"PRAGMA threads={args.threads}")
        con.execute(f"SET s3_endpoint={sql_string(endpoint)}")
        con.execute(f"SET s3_access_key_id={sql_string(config.access_key)}")
        con.execute(f"SET s3_secret_access_key={sql_string(config.secret_key)}")
        con.execute("SET s3_region='us-east-1'")
        con.execute("SET s3_url_style='path'")
        con.execute(f"SET s3_use_ssl={'true' if config.endpoint.startswith('https://') else 'false'}")
        con.execute(
            f"CREATE TABLE vacancies AS SELECT * FROM read_ndjson_auto({sql_string(s3_uri)})"
        )
        vacancies = con.execute("SELECT count(*) FROM vacancies").fetchone()[0]
        con.execute(
            "CREATE TABLE skill_rows AS SELECT vacancy_id, unnest(skills) AS skill FROM vacancies"
        )
        con.execute(
            """
            CREATE TABLE pair_rows AS
            SELECT a.vacancy_id, a.skill AS skill1, b.skill AS skill2
            FROM skill_rows a JOIN skill_rows b USING (vacancy_id)
            WHERE a.skill < b.skill
            """
        )
        con.execute(
            """
            CREATE TABLE triple_rows AS
            SELECT a.vacancy_id, a.skill AS skill1, b.skill AS skill2, c.skill AS skill3
            FROM skill_rows a JOIN skill_rows b USING (vacancy_id)
            JOIN skill_rows c USING (vacancy_id)
            WHERE a.skill < b.skill AND b.skill < c.skill
            """
        )
        documents = {
            "skills": count_document(con, "skill_rows", ["skill"], 1, vacancies),
            "pairs": count_document(con, "pair_rows", ["skill1", "skill2"], 2, vacancies),
            "triples": count_document(
                con, "triple_rows", ["skill1", "skill2", "skill3"], 3, vacancies
            ),
        }
        args.out_dir.mkdir(parents=True, exist_ok=True)
        for name, document in documents.items():
            (args.out_dir / f"{name}.json").write_text(
                json.dumps(document, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
            )
    finally:
        con.close()
    summary = {
        "method": "duckdb-sql-counts-only",
        "s3_object": s3_uri,
        "direct_s3_read": True,
        "threads": args.threads,
        "vacancies": vacancies,
        "elapsed_seconds": round(time.perf_counter() - started, 6),
        "outputs": {
            name: {
                "unique": document["unique_combinations"],
                "total": sum(item["count"] for item in document["counts"]),
            }
            for name, document in documents.items()
        },
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return summary


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--key", default="variants/devops/vacancies.ndjson")
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--threads", type=int, default=4)
    run(parser.parse_args())


if __name__ == "__main__":
    main()

