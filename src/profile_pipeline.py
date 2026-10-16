"""Run reproducible time and peak-RSS measurements for both implementations."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import psutil


def peak_tree_rss(process: subprocess.Popen) -> tuple[int, str, str]:
    root = psutil.Process(process.pid)
    peak = 0
    while process.poll() is None:
        total = 0
        try:
            members = [root, *root.children(recursive=True)]
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            members = []
        for member in members:
            try:
                total += member.memory_info().rss
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        peak = max(peak, total)
        time.sleep(0.01)
    stdout, stderr = process.communicate()
    return peak, stdout, stderr


def execute(command: list[str]) -> dict:
    started = time.perf_counter()
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
    )
    peak, stdout, stderr = peak_tree_rss(process)
    elapsed = time.perf_counter() - started
    if process.returncode != 0:
        raise RuntimeError(f"Command failed ({process.returncode}):\n{stderr}\n{stdout}")
    summary = json.loads(stdout)
    return {
        "wall_seconds": round(elapsed, 6),
        "reported_compute_seconds": summary["elapsed_seconds"],
        "peak_rss_mib": round(peak / 1024 / 1024, 3),
        "vacancies": summary["vacancies"],
        "outputs": summary["outputs"],
    }


def aggregate(runs: list[dict]) -> dict:
    return {
        "attempts": len(runs),
        "wall_seconds_median": round(sorted(item["wall_seconds"] for item in runs)[len(runs) // 2], 6),
        "peak_rss_mib_median": round(sorted(item["peak_rss_mib"] for item in runs)[len(runs) // 2], 3),
        "runs": runs,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--attempts", type=int, default=3)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--chunk-bytes", type=int, default=512 * 1024)
    args = parser.parse_args()
    source = Path(__file__).resolve().parent
    python = sys.executable
    measurements: dict[str, dict] = {}
    with tempfile.TemporaryDirectory(prefix="hw3-profile-") as directory:
        root = Path(directory)
        commands = {
            "mapreduce_baseline": [
                python, "-X", "utf8", str(source / "mapreduce_s3.py"),
                "--out-dir", str(root / "baseline"), "--mode", "baseline",
                "--workers", str(args.workers), "--chunk-bytes", str(args.chunk_bytes),
            ],
            "mapreduce_optimized": [
                python, "-X", "utf8", str(source / "mapreduce_s3.py"),
                "--out-dir", str(root / "optimized"), "--mode", "optimized",
                "--workers", str(args.workers), "--chunk-bytes", str(args.chunk_bytes),
            ],
            "duckdb_sql": [
                python, "-X", "utf8", str(source / "duckdb_counts_s3.py"),
                "--out-dir", str(root / "duckdb"), "--threads", str(args.workers),
            ],
        }
        for method, command in commands.items():
            runs = [execute(command) for _ in range(args.attempts)]
            measurements[method] = aggregate(runs)

    before = measurements["mapreduce_baseline"]
    after = measurements["mapreduce_optimized"]
    result = {
        "measurement": "wall clock plus maximum sum of RSS for the process tree, sampled every 10 ms",
        "attempts_per_method": args.attempts,
        "workers_or_threads": args.workers,
        "chunk_bytes": args.chunk_bytes,
        "methods": measurements,
        "optimization_effect": {
            "wall_speedup": round(before["wall_seconds_median"] / after["wall_seconds_median"], 3),
            "peak_memory_reduction_pct": round(
                100 * (before["peak_rss_mib_median"] - after["peak_rss_mib_median"])
                / before["peak_rss_mib_median"],
                2,
            ),
        },
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

