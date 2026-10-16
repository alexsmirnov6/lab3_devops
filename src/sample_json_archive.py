"""Create a reproducible 1,000-file sample from the supplied HH JSON archive."""

from __future__ import annotations

import argparse
import hashlib
import json
import random
from pathlib import Path, PurePosixPath
from zipfile import ZipFile


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("archive", type=Path)
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--size", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=3310)
    args = parser.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)
    with ZipFile(args.archive) as archive:
        names = sorted(name for name in archive.namelist() if name.lower().endswith(".json"))
        if args.size > len(names):
            raise ValueError(f"Requested {args.size} files from an archive containing {len(names)} JSON files")
        selected = sorted(random.Random(args.seed).sample(names, args.size))
        manifest_items = []
        for name in selected:
            posix = PurePosixPath(name)
            if posix.is_absolute() or ".." in posix.parts:
                raise ValueError(f"Unsafe archive path: {name}")
            payload = archive.read(name)
            target = args.out_dir / posix.name
            target.write_bytes(payload)
            parsed = json.loads(payload)
            manifest_items.append(
                {
                    "archive_path": name,
                    "file": target.name,
                    "vacancy_id": parsed.get("vacancy_id"),
                    "bytes": len(payload),
                    "sha256": hashlib.sha256(payload).hexdigest(),
                }
            )
    manifest = {
        "source_archive": args.archive.name,
        "population": len(names),
        "sample_size": args.size,
        "seed": args.seed,
        "selection": "random.sample over the lexicographically sorted JSON path list",
        "files": manifest_items,
    }
    manifest_path = args.out_dir.parent / "json_sample_manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(
        json.dumps(
            {
                "population": len(names),
                "sample_size": args.size,
                "seed": args.seed,
                "output": str(args.out_dir),
                "manifest": str(manifest_path),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()

