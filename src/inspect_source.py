"""Profile the source CSV and validate the DevOps variant condition."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import duckdb


VARIANT_SQL = r"""
regexp_matches(
  lower(coalesce(name, '')),
  'dev[[:space:]-]*(sec[[:space:]-]*)?ops|девопс|site reliability|(^|[^a-z])sre([^a-z]|$)|platform engineer'
)
"""


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("csv", type=Path)
    parser.add_argument("--out", type=Path)
    args = parser.parse_args()

    started = time.perf_counter()
    con = duckdb.connect()
    source = "read_csv(?, header=true, all_varchar=true, ignore_errors=false)"
    query = f"""
    WITH src AS (SELECT * FROM {source})
    SELECT
      count(*) AS rows,
      count(DISTINCT vacancy_id) AS distinct_vacancies,
      count(*) - count(DISTINCT vacancy_id) AS duplicate_rows,
      min(published_at) AS min_published_at,
      max(published_at) AS max_published_at,
      count(*) FILTER (WHERE {VARIANT_SQL}) AS devops_rows,
      count(DISTINCT vacancy_id) FILTER (WHERE {VARIANT_SQL}) AS devops_distinct_vacancies,
      count(*) FILTER (
        WHERE {VARIANT_SQL}
          AND upper(currency) = 'RUR'
          AND (try_cast(salary_from AS DOUBLE) > 0 OR try_cast(salary_to AS DOUBLE) > 0)
      ) AS devops_salary_rows,
      count(*) FILTER (WHERE {VARIANT_SQL} AND coalesce(key_skills, '') <> '') AS devops_with_skills
    FROM src
    """
    columns = [item[0] for item in con.execute(query, [str(args.csv)]).description]
    values = con.fetchone()
    result = dict(zip(columns, values))
    result["source_bytes"] = args.csv.stat().st_size
    result["elapsed_seconds"] = round(time.perf_counter() - started, 3)
    result["variant_sql"] = " ".join(VARIANT_SQL.split())
    text = json.dumps(result, ensure_ascii=False, indent=2, default=str)
    print(text)
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(text + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()

