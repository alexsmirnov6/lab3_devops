"""Independent DuckDB SQL analysis of the same RustFS object."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import boto3
import duckdb
from botocore.client import Config

from common import S3Config


def s3_client(config: S3Config):
    return boto3.client(
        "s3",
        endpoint_url=config.endpoint,
        aws_access_key_id=config.access_key,
        aws_secret_access_key=config.secret_key,
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def rows_as_dicts(cursor) -> list[dict]:
    columns = [item[0] for item in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def write_json(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def count_document(con, table: str, columns: list[str], stack_size: int, vacancies: int) -> dict:
    select_skills = ", ".join(columns)
    order_skills = ", ".join(columns)
    result = con.execute(
        f"SELECT {select_skills}, count(*)::BIGINT AS count "
        f"FROM {table} GROUP BY {select_skills} ORDER BY count DESC, {order_skills}"
    ).fetchall()
    return {
        "variant": 11,
        "segment": "DevOps инженер",
        "stack_size": stack_size,
        "vacancies": vacancies,
        "unique_combinations": len(result),
        "counts": [
            {"skills": list(row[:-1]), "count": row[-1]}
            for row in result
        ],
    }


def run(args) -> dict:
    config = S3Config.from_env()
    s3 = s3_client(config)
    head_before = s3.head_object(Bucket=config.bucket, Key=args.key)
    started = time.perf_counter()
    con = duckdb.connect(database=":memory:")
    con.execute("LOAD httpfs")
    con.execute(f"PRAGMA threads={args.threads}")
    endpoint = config.endpoint.removeprefix("http://").removeprefix("https://")
    con.execute(f"SET s3_endpoint={sql_string(endpoint)}")
    con.execute(f"SET s3_access_key_id={sql_string(config.access_key)}")
    con.execute(f"SET s3_secret_access_key={sql_string(config.secret_key)}")
    con.execute("SET s3_region='us-east-1'")
    con.execute("SET s3_url_style='path'")
    con.execute(f"SET s3_use_ssl={'true' if config.endpoint.startswith('https://') else 'false'}")
    s3_uri = f"s3://{config.bucket}/{args.key}"
    con.execute(
        f"CREATE TABLE vacancies AS SELECT * FROM read_ndjson_auto({sql_string(s3_uri)})"
    )
    try:
        vacancies = con.execute("SELECT count(*) FROM vacancies").fetchone()[0]
        con.execute(
            """
            CREATE TABLE skill_rows AS
            SELECT vacancy_id, area, salary_mid_rur_gross, unnest(skills) AS skill
            FROM vacancies
            """
        )
        con.execute(
            """
            CREATE TABLE pair_rows AS
            SELECT a.vacancy_id, a.area, a.salary_mid_rur_gross,
                   a.skill AS skill1, b.skill AS skill2
            FROM skill_rows a
            JOIN skill_rows b USING (vacancy_id)
            WHERE a.skill < b.skill
            """
        )
        con.execute(
            """
            CREATE TABLE triple_rows AS
            SELECT a.vacancy_id, a.area, a.salary_mid_rur_gross,
                   a.skill AS skill1, b.skill AS skill2, c.skill AS skill3
            FROM skill_rows a
            JOIN skill_rows b USING (vacancy_id)
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
        for name, document in documents.items():
            write_json(args.out_dir / f"{name}.json", document)

        baseline = con.execute(
            "SELECT count(*), median(salary_mid_rur_gross) FROM vacancies "
            "WHERE salary_mid_rur_gross IS NOT NULL"
        ).fetchone()
        salary_base = {"n": baseline[0], "median_rur_gross": baseline[1]}

        pair_salary = rows_as_dicts(
            con.execute(
                """
                WITH skill_stats AS (
                    SELECT skill, count(*) AS demand_n,
                           count(salary_mid_rur_gross) AS salary_n,
                           median(salary_mid_rur_gross) AS salary_median
                    FROM skill_rows GROUP BY skill
                ), pair_stats AS (
                    SELECT skill1, skill2, count(*) AS demand_n,
                           count(salary_mid_rur_gross) AS salary_n,
                           median(salary_mid_rur_gross) AS combo_median
                    FROM pair_rows GROUP BY skill1, skill2
                )
                SELECT p.skill1, p.skill2, p.demand_n, p.salary_n,
                       CASE WHEN s1.demand_n >= s2.demand_n THEN p.skill1 ELSE p.skill2 END AS base_skill,
                       CASE WHEN s1.demand_n >= s2.demand_n THEN s1.demand_n ELSE s2.demand_n END AS base_demand_n,
                       CASE WHEN s1.demand_n >= s2.demand_n THEN s1.salary_n ELSE s2.salary_n END AS base_salary_n,
                       round(CASE WHEN s1.demand_n >= s2.demand_n THEN s1.salary_median ELSE s2.salary_median END, 2) AS base_median,
                       round(p.combo_median, 2) AS combo_median,
                       round(p.combo_median - CASE WHEN s1.demand_n >= s2.demand_n THEN s1.salary_median ELSE s2.salary_median END, 2) AS delta_rur
                FROM pair_stats p
                JOIN skill_stats s1 ON s1.skill=p.skill1
                JOIN skill_stats s2 ON s2.skill=p.skill2
                WHERE p.salary_n >= ?
                ORDER BY delta_rur DESC, p.salary_n DESC, p.skill1, p.skill2
                """,
                [args.min_pair_salary],
            )
        )

        triple_salary = rows_as_dicts(
            con.execute(
                """
                WITH pair_stats AS (
                    SELECT skill1, skill2, count(*) AS demand_n,
                           count(salary_mid_rur_gross) AS salary_n,
                           median(salary_mid_rur_gross) AS salary_median
                    FROM pair_rows GROUP BY skill1, skill2
                ), triple_stats AS (
                    SELECT skill1, skill2, skill3, count(*) AS demand_n,
                           count(salary_mid_rur_gross) AS salary_n,
                           median(salary_mid_rur_gross) AS combo_median
                    FROM triple_rows GROUP BY skill1, skill2, skill3
                ), candidates AS (
                    SELECT t.*, p.skill1 AS b1, p.skill2 AS b2, p.demand_n AS base_demand_n,
                           p.salary_n AS base_salary_n, p.salary_median AS base_median,
                           row_number() OVER (PARTITION BY t.skill1,t.skill2,t.skill3
                             ORDER BY p.demand_n DESC, p.skill1, p.skill2) AS rn
                    FROM triple_stats t
                    JOIN pair_stats p ON (p.skill1=t.skill1 AND p.skill2=t.skill2)
                                      OR (p.skill1=t.skill1 AND p.skill2=t.skill3)
                                      OR (p.skill1=t.skill2 AND p.skill2=t.skill3)
                )
                SELECT skill1, skill2, skill3, demand_n, salary_n,
                       b1 || ' + ' || b2 AS base_pair, base_demand_n, base_salary_n,
                       round(base_median, 2) AS base_median,
                       round(combo_median, 2) AS combo_median,
                       round(combo_median-base_median, 2) AS delta_rur
                FROM candidates
                WHERE rn=1 AND salary_n >= ?
                ORDER BY delta_rur DESC, salary_n DESC, skill1, skill2, skill3
                """,
                [args.min_triple_salary],
            )
        )

        regions = [row[0] for row in con.execute(
            "SELECT area FROM vacancies GROUP BY area ORDER BY count(*) DESC, area LIMIT 3"
        ).fetchall()]
        placeholders = ",".join("?" for _ in regions)
        region_summary = rows_as_dicts(
            con.execute(
                f"""
                SELECT area AS region, count(*) AS vacancies,
                       count(salary_mid_rur_gross) AS salary_n,
                       round(median(salary_mid_rur_gross), 2) AS median_salary_rur_gross,
                       count(*) FILTER (WHERE len(skills)>0) AS vacancies_with_skills,
                       round(avg(len(skills)), 2) AS avg_skills_per_vacancy
                FROM vacancies WHERE area IN ({placeholders})
                GROUP BY area ORDER BY vacancies DESC
                """,
                regions,
            )
        )
        region_skills = rows_as_dicts(
            con.execute(
                f"""
                WITH ranked AS (
                    SELECT area AS region, skill, count(*) AS vacancies,
                           round(100.0*count(*)/max(region_total), 2) AS share_pct,
                           row_number() OVER (PARTITION BY area ORDER BY count(*) DESC, skill) AS rank
                    FROM (
                        SELECT s.*, count(DISTINCT v.vacancy_id) OVER (PARTITION BY s.area) AS region_total
                        FROM skill_rows s JOIN vacancies v USING (vacancy_id)
                        WHERE s.area IN ({placeholders})
                    ) x
                    GROUP BY area, skill
                )
                SELECT region, rank, skill, vacancies, share_pct FROM ranked
                WHERE rank <= 10 ORDER BY region, rank
                """,
                regions,
            )
        )
    finally:
        con.close()

    head_after = s3.head_object(Bucket=config.bucket, Key=args.key)
    object_unchanged = (
        head_before["ETag"] == head_after["ETag"]
        and head_before["ContentLength"] == head_after["ContentLength"]
        and head_before.get("Metadata", {}).get("sha256")
        == head_after.get("Metadata", {}).get("sha256")
    )
    if not object_unchanged:
        raise RuntimeError("S3 object changed during DuckDB analysis")

    salary_result = {
        "method": "duckdb-sql",
        "salary_definition": "RUR monthly gross; midpoint for two bounds, disclosed bound for one-sided salary; net divided by 0.87",
        "overall": salary_base,
        "minimum_salary_observations": {
            "pairs": args.min_pair_salary,
            "triples": args.min_triple_salary,
        },
        "pair_effects": pair_salary,
        "triple_effects": triple_salary,
    }
    write_json(args.out_dir / "salary_effects.json", salary_result)
    write_json(
        args.out_dir / "regions.json",
        {"selected_by": "three regions with the most DevOps vacancies", "summary": region_summary, "top_skills": region_skills},
    )
    summary = {
        "method": "duckdb-sql",
        "s3_object": f"s3://{config.bucket}/{args.key}",
        "direct_s3_read": True,
        "object_bytes": head_before["ContentLength"],
        "object_sha256_metadata": head_before.get("Metadata", {}).get("sha256"),
        "s3_object_unchanged_during_query": object_unchanged,
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
        "salary_rows": salary_base["n"],
        "regions": regions,
    }
    if args.summary_out:
        args.summary_out.parent.mkdir(parents=True, exist_ok=True)
        args.summary_out.write_text(
            json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return summary


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--key", default="variants/devops/vacancies.ndjson")
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--min-pair-salary", type=int, default=20)
    parser.add_argument("--min-triple-salary", type=int, default=15)
    parser.add_argument("--summary-out", type=Path)
    run(parser.parse_args())


if __name__ == "__main__":
    main()

