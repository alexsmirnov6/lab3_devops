"""Inspect the supplied JSON archive without extracting untrusted paths."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path, PurePosixPath
from zipfile import ZipFile


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("archive", type=Path)
    parser.add_argument("--sample", type=int, default=3)
    args = parser.parse_args()

    with ZipFile(args.archive) as archive:
        infos = archive.infolist()
        unsafe = [
            item.filename
            for item in infos
            if PurePosixPath(item.filename).is_absolute() or ".." in PurePosixPath(item.filename).parts
        ]
        json_names = sorted(item.filename for item in infos if item.filename.lower().endswith(".json"))
        result = {
            "archive": args.archive.name,
            "entries": len(infos),
            "json_files": len(json_names),
            "compressed_bytes": sum(item.compress_size for item in infos),
            "uncompressed_bytes": sum(item.file_size for item in infos),
            "unsafe_paths": unsafe,
            "first_names": json_names[:10],
            "samples": [],
        }
        key_sets: Counter[tuple[str, ...]] = Counter()
        for name in json_names:
            payload = json.loads(archive.read(name))
            if isinstance(payload, dict):
                key_sets[tuple(sorted(payload))] += 1
            if len(result["samples"]) < args.sample:
                result["samples"].append(
                    {
                        "name": name,
                        "type": type(payload).__name__,
                        "keys": sorted(payload) if isinstance(payload, dict) else None,
                        "preview": payload,
                    }
                )
        result["schema_counts"] = [
            {"keys": list(keys), "files": count} for keys, count in key_sets.most_common()
        ]
        print(json.dumps(result, ensure_ascii=False, indent=2)[:50000])


if __name__ == "__main__":
    main()

