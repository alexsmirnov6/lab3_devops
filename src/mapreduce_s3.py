"""Custom MapReduce over NDJSON chunks read from RustFS with S3 Range GET."""

from __future__ import annotations

import argparse
import itertools
import json
import os
import time
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import boto3
from botocore.client import Config

from common import S3Config


def make_client(config: dict):
    return boto3.client(
        "s3",
        endpoint_url=config["endpoint"],
        aws_access_key_id=config["access_key"],
        aws_secret_access_key=config["secret_key"],
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def read_complete_lines(s3, bucket: str, key: str, start: int, end: int, size: int) -> bytes:
    fetch_start = max(0, start - 1)
    fetch_end = min(size - 1, end + 65536)
    body = s3.get_object(Bucket=bucket, Key=key, Range=f"bytes={fetch_start}-{fetch_end}")["Body"].read()
    left = 0
    if start > 0:
        first_newline = body.find(b"\n")
        if first_newline < 0:
            return b""
        left = first_newline + 1
    requested_end = end - fetch_start
    if end < size - 1:
        right_newline = body.find(b"\n", max(left, requested_end + 1))
        if right_newline < 0:
            raise RuntimeError("Line exceeds the 64 KiB chunk overlap")
        right = right_newline + 1
    else:
        right = len(body)
    return body[left:right]


def map_chunk(task: dict):
    if task["inject_failure"] and task["index"] == 1 and task["attempt"] == 0:
        raise RuntimeError("Injected mapper failure for retry verification")
    s3 = make_client(task["config"])
    payload = read_complete_lines(
        s3, task["bucket"], task["key"], task["start"], task["end"], task["size"]
    )
    counters = [Counter(), Counter(), Counter()]
    emitted = [[], [], []]
    rows = 0
    for line in payload.splitlines():
        if not line:
            continue
        rows += 1
        skills = json.loads(line)["skills"]
        groups = (
            skills,
            ("\u001f".join(item) for item in itertools.combinations(skills, 2)),
            ("\u001f".join(item) for item in itertools.combinations(skills, 3)),
        )
        if task["mode"] == "optimized":
            for counter, values in zip(counters, groups):
                counter.update(values)
        else:
            for target, values in zip(emitted, groups):
                target.extend(values)
    if task["mode"] == "optimized":
        payload_out = [dict(counter) for counter in counters]
    else:
        payload_out = emitted
    return {"index": task["index"], "rows": rows, "payload": payload_out, "pid": os.getpid()}


def sorted_records(counter: Counter, width: int) -> list[dict]:
    records = []
    for joined, count in sorted(counter.items(), key=lambda item: (-item[1], item[0])):
        skills = joined.split("\u001f") if width > 1 else [joined]
        records.append({"skills": skills, "count": count})
    return records


def run(args) -> dict:
    config_obj = S3Config.from_env()
    config = {
        "endpoint": config_obj.endpoint,
        "access_key": config_obj.access_key,
        "secret_key": config_obj.secret_key,
    }
    s3 = make_client(config)
    size = s3.head_object(Bucket=config_obj.bucket, Key=args.key)["ContentLength"]
    tasks = []
    for index, start in enumerate(range(0, size, args.chunk_bytes)):
        tasks.append(
            {
                "index": index,
                "start": start,
                "end": min(size - 1, start + args.chunk_bytes - 1),
                "size": size,
                "bucket": config_obj.bucket,
                "key": args.key,
                "config": config,
                "mode": args.mode,
                "attempt": 0,
                "inject_failure": args.inject_failure,
            }
        )

    started = time.perf_counter()
    final = [Counter(), Counter(), Counter()]
    rows = 0
    retries = 0
    workers = set()
    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        future_to_task = {pool.submit(map_chunk, task): task for task in tasks}
        while future_to_task:
            for future in as_completed(list(future_to_task)):
                task = future_to_task.pop(future)
                try:
                    result = future.result()
                except Exception:
                    if task["attempt"] >= args.max_retries:
                        raise
                    task = dict(task, attempt=task["attempt"] + 1)
                    retries += 1
                    future_to_task[pool.submit(map_chunk, task)] = task
                    continue
                rows += result["rows"]
                workers.add(result["pid"])
                if args.mode == "optimized":
                    for counter, values in zip(final, result["payload"]):
                        counter.update(values)
                else:
                    for counter, values in zip(final, result["payload"]):
                        counter.update(values)

    args.out_dir.mkdir(parents=True, exist_ok=True)
    outputs = {}
    for width, name, counter in zip((1, 2, 3), ("skills", "pairs", "triples"), final):
        path = args.out_dir / f"{name}.json"
        document = {
            "variant": 11,
            "segment": "DevOps инженер",
            "stack_size": width,
            "vacancies": rows,
            "unique_combinations": len(counter),
            "counts": sorted_records(counter, width),
        }
        path.write_text(json.dumps(document, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        outputs[name] = {"path": str(path), "unique": len(counter), "total": sum(counter.values())}
    summary = {
        "method": f"mapreduce-{args.mode}",
        "s3_object": f"s3://{config_obj.bucket}/{args.key}",
        "object_bytes": size,
        "chunk_bytes": args.chunk_bytes,
        "chunks": len(tasks),
        "workers": args.workers,
        "worker_pids": sorted(workers),
        "vacancies": rows,
        "retries": retries,
        "elapsed_seconds": round(time.perf_counter() - started, 6),
        "outputs": outputs,
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
    parser.add_argument("--mode", choices=("baseline", "optimized"), default="optimized")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--chunk-bytes", type=int, default=1024 * 1024)
    parser.add_argument("--max-retries", type=int, default=2)
    parser.add_argument("--inject-failure", action="store_true")
    parser.add_argument("--summary-out", type=Path)
    args = parser.parse_args()
    run(args)


if __name__ == "__main__":
    main()

