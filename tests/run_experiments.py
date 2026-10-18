"""Reproduce local measurements without a RustFS installation or credentials."""

import argparse
import hashlib
import json
import platform
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from profile_pipeline import aggregate, execute
from range_server import range_server
from verify_results import compare_results


def duckdb_local(source, out_dir):
    import duckdb
    from duckdb_counts_s3 import count_source

    started = time.perf_counter()
    with duckdb.connect() as con:
        con.execute("PRAGMA threads=4")
        rows, documents = count_source(con, str(source))
    out_dir.mkdir(parents=True, exist_ok=True)
    for name, document in documents.items():
        (out_dir / f"{name}.json").write_text(
            json.dumps(document, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
    print(json.dumps({
        "elapsed_seconds": time.perf_counter() - started, "vacancies": rows,
        "outputs": {name: {"unique": doc["unique_combinations"]} for name, doc in documents.items()},
    }))


def experiments(source, output, attempts):
    data = source.read_bytes()
    python = [sys.executable, "-X", "utf8"]
    result = {
        "environment": {"platform": platform.platform(), "python": platform.python_version()},
        "transport": "localhost HTTP HEAD/Range GET fixture; not RustFS",
        "dataset": {"path": source.relative_to(ROOT).as_posix(), "bytes": len(data),
                    "sha256": hashlib.sha256(data).hexdigest()},
        "attempts_per_case": attempts,
        "memory": "Maximum summed process-tree RSS, sampled every 10 ms; shared pages may be counted repeatedly",
        "intermediate_size": "Sum of successful mapper payload pickle sizes; excludes IPC framing and failed attempts",
        "cases": [],
    }
    with tempfile.TemporaryDirectory(prefix="lab3-experiments-") as tmp, range_server(data):
        root = Path(tmp)
        db_out = root / "duckdb"
        db_runs = [execute(python + [str(Path(__file__).resolve()), "--duckdb", str(source),
                                    "--out-dir", str(db_out)]) for _ in range(attempts)]
        result["duckdb_local_file"] = aggregate(db_runs)
        for workers, chunk in ((1, 1048576), (2, 1048576), (4, 1048576), (4, 262144)):
            case = {"workers": workers, "chunk_bytes": chunk, "methods": {}}
            for mode in ("baseline", "optimized"):
                out = root / mode
                command = python + [str(ROOT / "src/mapreduce_s3.py"), "--out-dir", str(out),
                                    "--mode", mode, "--workers", str(workers), "--chunk-bytes", str(chunk)]
                runs = []
                for _ in range(attempts):
                    measured = execute(command)
                    measured["verification"] = compare_results(out, db_out)
                    if not measured["verification"]["all_equal"]:
                        raise RuntimeError("MapReduce and DuckDB differ")
                    # Temporary output paths are not part of the experiment evidence.
                    for item in measured["outputs"].values():
                        item.pop("path", None)
                    runs.append(measured)
                case["methods"][mode] = aggregate(runs)
                print(f"workers={workers} chunk={chunk} {mode}: complete", flush=True)
            result["cases"].append(case)
        recovered = root / "recovered"
        failure = execute(python + [str(ROOT / "src/mapreduce_s3.py"), "--out-dir", str(recovered),
                                   "--workers", "2", "--chunk-bytes", "262144", "--inject-failure"])
        failure["verification"] = compare_results(recovered, db_out)
        if failure["retries"] != 1 or not failure["verification"]["all_equal"]:
            raise RuntimeError("Recovery verification failed")
        for item in failure["outputs"].values():
            item.pop("path", None)
        result["mapper_failure"] = failure
        historical = json.loads((ROOT / "evidence/verification.json").read_text(encoding="utf-8"))
        fresh = failure["verification"]
        result["matches_original_result_hashes"] = all(
            old["sha256_mapreduce"] == new["sha256_mapreduce"]
            for old, new in zip(historical["checks"], fresh["checks"])
        )
        if not result["matches_original_result_hashes"]:
            raise RuntimeError("Results changed from the original recorded run")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    write_notes(result, output.with_name("experiment_notes.md"))


def write_notes(result, path):
    lines = [
        "# Проверка после рефакторинга", "",
        "Дата: 18 октября 2026. Среда: " + result["environment"]["platform"] +
        "; Python " + result["environment"]["python"] + ".", "",
        "Вход: `data/prepared/devops_vacancies.ndjson`, " + str(result["dataset"]["bytes"]) +
        " байт; SHA-256: `" + result["dataset"]["sha256"] + "`.", "",
        "MapReduce читает данные через boto3 с локального HTTP-стенда с HEAD и Range GET; "
        "используется настоящий ProcessPoolExecutor. DuckDB выполняет тот же SQL подсчёта, "
        "но читает локальный файл. Это проверка алгоритма и локальные замеры, а не повтор "
        "эксперимента с RustFS/httpfs. Старые журналы RustFS оставлены без изменений.", "",
        "Каждая строка — медиана " + str(result["attempts_per_case"]) + " последовательных запусков. "
        "Время включает запуск Python, создание процессов и запись JSON. RSS — максимум суммы "
        "памяти дерева процессов с шагом 10 мс; общие страницы могут учитываться несколько раз.", "",
        "| Процессы | Чанк, КиБ | Режим | Задачи | Время, с | RSS, МиБ | Элементы Mapper | Pickle, байт |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for case in result["cases"]:
        for mode, measurement in case["methods"].items():
            run = measurement["runs"][0]
            lines.append(f"| {case['workers']} | {case['chunk_bytes'] // 1024} | {mode} | "
                         f"{run['chunks']} | {measurement['wall_seconds_median']:.3f} | "
                         f"{measurement['peak_rss_mib_median']:.1f} | {run['intermediate_items']} | "
                         f"{run['intermediate_pickle_bytes']} |")
    first = result["cases"][0]["methods"]
    before = first["baseline"]["runs"][0]["intermediate_pickle_bytes"]
    after = first["optimized"]["runs"][0]["intermediate_pickle_bytes"]
    lines += ["", f"Для 1 процесса и чанка 1 МиБ combiner уменьшил объём Pickle на "
              f"{100 * (1 - after / before):.1f}%; отношение медиан времени baseline/optimized — "
              f"{first['baseline']['wall_seconds_median'] / first['optimized']['wall_seconds_median']:.2f}."]
    failure = result["mapper_failure"]
    lines += ["", "Размер Pickle — сумма сериализованных payload успешных Mapper, без служебных "
              "полей и обрамления IPC. Дополнительная сериализация для метрики включена во время. "
              "При уменьшении чанков combiner видит меньше записей за задачу; "
              "одна и та же комбинация может передаваться в Reducer из нескольких чанков.", "",
              "При чанке 1 МиБ здесь всего две задачи: четыре процесса не получают больше двух "
              "задач. Эти данные не позволяют делать вывод о масштабировании на большом наборе. "
              "На время влияют запуск процессов Windows, сериализация и запись результатов; "
              "меньшее число элементов само по себе не гарантирует ускорения.", "",
              f"Искусственный отказ второго чанка: {failure['retries']} повтор, "
              f"{failure['vacancies']} записей, результат совпал с DuckDB.", "",
              "Для каждого запуска baseline и optimized проверены полные JSON документов "
              "skills, pairs и triples. Совпали также SHA-256 результатов из исходного "
              "`evidence/verification.json`: 1 541 навык, 25 462 пары, 195 366 троек, 6 215 вакансий.", "",
              "DuckDB (локальный файл, 4 потока): медиана "
              f"{result['duckdb_local_file']['wall_seconds_median']:.3f} с, "
              f"{result['duckdb_local_file']['peak_rss_mib_median']:.1f} МиБ RSS. "
              "Сравнивать это время с S3/httpfs как равные условия нельзя.", "",
              "Воспроизведение: `python -X utf8 tests/run_experiments.py --attempts 3`. "
              "Подробные измерения: `evidence/local_experiments.json`."]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--duckdb", type=Path)
    parser.add_argument("--out-dir", type=Path)
    parser.add_argument("--attempts", type=int, default=3)
    args = parser.parse_args()
    if args.duckdb:
        duckdb_local(args.duckdb, args.out_dir)
    else:
        if args.attempts < 1:
            parser.error("attempts must be positive")
        experiments(ROOT / "data/prepared/devops_vacancies.ndjson",
                    ROOT / "evidence/local_experiments.json", args.attempts)
