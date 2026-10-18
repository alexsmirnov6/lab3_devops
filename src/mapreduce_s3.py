"""Custom MapReduce over NDJSON chunks read from RustFS with S3 Range GET."""

from __future__ import annotations

import argparse
import itertools
import json
import logging
import os
import pickle
import time
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

from botocore.exceptions import ClientError, ConnectionError, HTTPClientError

from common import S3Config
from s3_io import client as make_client


class InjectedMapperFailure(RuntimeError):
    """A retryable failure used only by the recovery experiment."""


def read_complete_lines(s3, bucket: str, key: str, start: int, end: int, size: int) -> bytes:
    """Read records whose first byte belongs to the inclusive range [start, end]."""
    if size == 0:
        return b""
    if not 0 <= start <= end < size:
        raise ValueError("Expected 0 <= start <= end < object size")

    def fetch(left, right):
        response = s3.get_object(Bucket=bucket, Key=key, Range=f"bytes={left}-{right}")
        with response["Body"] as stream:
            return stream.read()

    fetch_start = max(0, start - 1)
    body = bytearray(fetch(fetch_start, end))
    # Only extend a partial last record. A newline at end already completes it.
    position = end + 1
    while body[-1:] != b"\n" and position < size:
        block = fetch(position, min(size - 1, position + 65535))
        if not block:
            raise IOError(f"Unexpected end of S3 response at byte {position}")
        newline = block.find(b"\n")
        body.extend(block if newline < 0 else block[:newline + 1])
        position += len(block)
        if newline >= 0:
            break
    if start > 0:
        newline = body.find(b"\n")
        return bytes(body[newline + 1:]) if newline >= 0 else b""
    return bytes(body)


def count_skills(lines, use_combiner: bool):
    payload = [Counter() for _ in range(3)] if use_combiner else [[], [], []]
    rows = 0
    for line in lines:
        if not line.strip():
            continue
        record = json.loads(line)
        skills = record["skills"]
        if not isinstance(skills, list) or any(not isinstance(skill, str) for skill in skills):
            raise ValueError("skills must be a list of strings")
        # Prepared data already has this form; normalize defensively for other inputs.
        skills = sorted(set(skills))
        rows += 1
        for width, target in enumerate(payload, 1):
            groups = itertools.combinations(skills, width)
            if use_combiner:
                target.update(groups)
            else:
                target.extend(groups)
    return rows, payload


def map_chunk(task: dict):
    if task["inject_failure"] and task["index"] == 1 and task["attempt"] == 0:
        raise InjectedMapperFailure("Injected mapper failure for retry verification")
    s3 = make_client(task["config"])
    payload = read_complete_lines(
        s3, task["bucket"], task["key"], task["start"], task["end"], task["size"]
    )
    rows, payload_out = count_skills(payload.split(b"\n"), task["mode"] == "optimized")
    return {
        "index": task["index"], "rows": rows, "payload": payload_out, "pid": os.getpid(),
        "intermediate_items": sum(map(len, payload_out)),
        "intermediate_pickle_bytes": len(pickle.dumps(payload_out, protocol=pickle.HIGHEST_PROTOCOL)),
    }


def sorted_records(counter: Counter) -> list[dict]:
    records = []
    for skills, count in sorted(counter.items(), key=lambda item: (-item[1], item[0])):
        records.append({"skills": list(skills), "count": count})
    return records


def retryable(error: Exception) -> bool:
    if isinstance(error, (InjectedMapperFailure, ConnectionError, HTTPClientError)):
        return True
    if isinstance(error, ClientError):
        status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
        code = error.response.get("Error", {}).get("Code")
        return status >= 500 or status in (408, 429) or code in ("SlowDown", "RequestTimeout")
    return False


def save_results(counters, rows: int, output_dir: Path) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)
    outputs = {}
    for width, name, counter in zip((1, 2, 3), ("skills", "pairs", "triples"), counters):
        path = output_dir / f"{name}.json"
        document = {
            "variant": 11,
            "segment": "DevOps инженер",
            "stack_size": width,
            "vacancies": rows,
            "unique_combinations": len(counter),
            "counts": sorted_records(counter),
        }
        path.write_text(json.dumps(document, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        outputs[name] = {"path": str(path), "unique": len(counter), "total": sum(counter.values())}
    return outputs


def run(args) -> dict:
    if args.workers < 1 or args.chunk_bytes < 1 or args.max_retries < 0:
        raise ValueError("workers/chunk_bytes must be positive; max_retries must be nonnegative")
    config = S3Config.from_env()
    s3 = make_client(config)
    size = s3.head_object(Bucket=config.bucket, Key=args.key)["ContentLength"]
    tasks = []
    for index, start in enumerate(range(0, size, args.chunk_bytes)):
        tasks.append(
            {
                "index": index,
                "start": start,
                "end": min(size - 1, start + args.chunk_bytes - 1),
                "size": size,
                "bucket": config.bucket,
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
    intermediate_items = 0
    intermediate_bytes = 0
    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        future_to_task = {pool.submit(map_chunk, task): task for task in tasks}
        while future_to_task:
            for future in as_completed(list(future_to_task)):
                task = future_to_task.pop(future)
                try:
                    result = future.result()
                except Exception as error:
                    context = f"chunk {task['index']} bytes={task['start']}-{task['end']} attempt={task['attempt'] + 1}"
                    logging.warning("Mapper %s failed: %s", context, error)
                    if not retryable(error) or task["attempt"] >= args.max_retries:
                        raise RuntimeError(f"Mapper {context} failed: {error}") from error
                    task = dict(task, attempt=task["attempt"] + 1)
                    retries += 1
                    future_to_task[pool.submit(map_chunk, task)] = task
                    continue
                rows += result["rows"]
                workers.add(result["pid"])
                intermediate_items += result["intermediate_items"]
                intermediate_bytes += result["intermediate_pickle_bytes"]
                for counter, values in zip(final, result["payload"]):
                    counter.update(values)

    outputs = save_results(final, rows, args.out_dir)

    summary = {
        "method": f"mapreduce-{args.mode}",
        "s3_object": f"s3://{config.bucket}/{args.key}",
        "object_bytes": size,
        "chunk_bytes": args.chunk_bytes,
        "chunks": len(tasks),
        "workers": args.workers,
        "worker_pids": sorted(workers),
        "vacancies": rows,
        "retries": retries,
        "intermediate_items": intermediate_items,
        "intermediate_pickle_bytes": intermediate_bytes,
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

