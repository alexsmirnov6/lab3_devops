"""Create the canonical DevOps segment used by both calculation methods."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import time
from collections import Counter
from pathlib import Path

from common import is_devops_title, optional_float, parse_bool, parse_skills, salary_midpoint_rur_gross


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path)
    parser.add_argument("--out-dir", type=Path, required=True)
    args = parser.parse_args()

    prepare(args.source, args.out_dir)


def prepare(source_path: Path, out_dir: Path) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    output = out_dir / "devops_vacancies.ndjson"
    labels_path = out_dir / "skill_labels.json"
    metadata_path = out_dir / "dataset_metadata.json"

    started = time.perf_counter()
    source_rows = 0
    selected_rows = 0
    with_skills = 0
    with_salary = 0
    region_counts: Counter[str] = Counter()
    label_counts: dict[str, Counter[str]] = {}
    min_date: str | None = None
    max_date: str | None = None

    with source_path.open("r", encoding="utf-8-sig", newline="") as source, output.open(
        "w", encoding="utf-8", newline="\n"
    ) as target:
        reader = csv.DictReader(source)
        for row in reader:
            source_rows += 1
            if not is_devops_title(row.get("name")):
                continue
            selected_rows += 1
            pairs = parse_skills(row.get("key_skills"))
            skills = [key for key, _ in pairs]
            for key, label in pairs:
                label_counts.setdefault(key, Counter())[label] += 1
            if skills:
                with_skills += 1
            salary = salary_midpoint_rur_gross(row)
            if salary is not None:
                with_salary += 1
            area = (row.get("area") or "Не указан").strip() or "Не указан"
            region_counts[area] += 1
            published_at = (row.get("published_at") or "").strip()
            if published_at:
                min_date = min(min_date, published_at) if min_date else published_at
                max_date = max(max_date, published_at) if max_date else published_at
            record = {
                "vacancy_id": row.get("vacancy_id"),
                "name": row.get("name"),
                "published_at": published_at or None,
                "area": area,
                "salary_from": optional_float(row.get("salary_from")),
                "salary_to": optional_float(row.get("salary_to")),
                "currency": row.get("currency") or None,
                "gross": parse_bool(row.get("gross")),
                "salary_mid_rur_gross": salary,
                "skills": skills,
            }
            target.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")

    labels = {
        key: sorted(counter.items(), key=lambda item: (-item[1], item[0].casefold()))[0][0]
        for key, counter in sorted(label_counts.items())
    }
    labels_path.write_text(json.dumps(labels, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    metadata = {
        "variant": 11,
        "segment": "DevOps инженер",
        "filter": "Название содержит DevOps, DevSecOps, SRE, Site Reliability или Platform Engineer",
        "source_file": source_path.name,
        "source_bytes": source_path.stat().st_size,
        "source_sha256": sha256(source_path),
        "source_rows": source_rows,
        "selected_rows": selected_rows,
        "selected_with_skills": with_skills,
        "selected_with_salary_rur": with_salary,
        "published_at_min": min_date,
        "published_at_max": max_date,
        "variant_object": output.name,
        "variant_bytes": output.stat().st_size,
        "variant_sha256": sha256(output),
        "top_regions": region_counts.most_common(10),
        "elapsed_seconds": round(time.perf_counter() - started, 3),
    }
    metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(metadata, ensure_ascii=False, indent=2))
    return metadata


if __name__ == "__main__":
    main()

