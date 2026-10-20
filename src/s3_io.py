"""Upload, list, and verify objects in the RustFS hh-lake bucket."""

from __future__ import annotations

import argparse
import hashlib
import json
import mimetypes
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.client import Config
from botocore.exceptions import ClientError

from common import S3Config


def client(config: S3Config):
    return boto3.client(
        "s3",
        endpoint_url=config.endpoint,
        aws_access_key_id=config.access_key,
        aws_secret_access_key=config.secret_key,
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def checksum(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def ensure_bucket(s3, bucket: str) -> None:
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") not in ("404", "NoSuchBucket"):
            raise
        s3.create_bucket(Bucket=bucket)


def upload(s3, bucket: str, path: Path, key: str) -> dict:
    sha = checksum(path)
    s3.upload_file(
        str(path),
        bucket,
        key,
        ExtraArgs={"Metadata": {"sha256": sha}},
        Config=TransferConfig(multipart_threshold=16 * 1024 * 1024, multipart_chunksize=16 * 1024 * 1024),
    )
    head = s3.head_object(Bucket=bucket, Key=key)
    return {
        "key": key,
        "bytes": head["ContentLength"],
        "etag": head["ETag"].strip('"'),
        "sha256_metadata": head.get("Metadata", {}).get("sha256"),
    }


def upload_tree(s3, bucket: str, root: Path, prefix: str, workers: int) -> list[dict]:
    files = sorted(path for path in root.rglob("*") if path.is_file())

    def put(path: Path) -> dict:
        key = prefix.rstrip("/") + "/" + path.relative_to(root).as_posix()
        sha = checksum(path)
        content_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
        s3.upload_file(
            str(path),
            bucket,
            key,
            ExtraArgs={"Metadata": {"sha256": sha}, "ContentType": content_type},
        )
        return {"key": key, "bytes": path.stat().st_size, "sha256": sha}

    results = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(put, path) for path in files]
        for future in as_completed(futures):
            results.append(future.result())
    return sorted(results, key=lambda item: item["key"])


def list_objects(s3, bucket: str, prefix: str = "") -> list[dict]:
    items = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        items.extend({"key": item["Key"], "bytes": item["Size"], "etag": item["ETag"].strip('"')}
                     for item in page.get("Contents", []))
    return sorted(items, key=lambda item: item["key"])


def publish_results(s3, bucket: str, root: Path, prefix: str) -> dict:
    """Upload precisely three results, then verify actual downloaded bytes."""
    inputs = []
    rows = set()
    for width, name in enumerate(("skills", "pairs", "triples"), 1):
        path = root / f"{name}.json"
        document = json.loads(path.read_text(encoding="utf-8"))
        if document["stack_size"] != width or len(document["counts"]) != document["unique_combinations"]:
            raise ValueError(f"Invalid result document: {path}")
        rows.add(document["vacancies"])
        inputs.append((path, prefix.strip("/") + "/" + path.name))
    if len(rows) != 1:
        raise ValueError("Result documents disagree on vacancy count")
    verified = []
    for path, key in inputs:
        uploaded = upload(s3, bucket, path, key)
        digest = hashlib.sha256()
        downloaded_bytes = 0
        response = s3.get_object(Bucket=bucket, Key=key)
        with response["Body"] as stream:
            for block in iter(lambda: stream.read(8 * 1024 * 1024), b""):
                digest.update(block)
                downloaded_bytes += len(block)
        expected = checksum(path)
        if digest.hexdigest() != expected or uploaded["sha256_metadata"] != expected:
            raise RuntimeError(f"S3 checksum mismatch: s3://{bucket}/{key}")
        if downloaded_bytes != path.stat().st_size or uploaded["bytes"] != downloaded_bytes:
            raise RuntimeError(f"S3 size mismatch: s3://{bucket}/{key}")
        verified.append({**uploaded, "sha256_downloaded": digest.hexdigest(), "verified": True})
    return {"bucket": bucket, "prefix": prefix, "all_verified": True, "objects": verified,
            "list_objects_v2": list_objects(s3, bucket, prefix.rstrip("/") + "/")}


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    put = sub.add_parser("upload")
    put.add_argument("path", type=Path)
    put.add_argument("key")
    tree = sub.add_parser("upload-tree")
    tree.add_argument("root", type=Path)
    tree.add_argument("prefix")
    tree.add_argument("--workers", type=int, default=8)
    publish = sub.add_parser("publish-results")
    publish.add_argument("root", type=Path)
    publish.add_argument("--prefix", default="results/mapreduce")
    publish.add_argument("--out", type=Path, required=True)
    listing = sub.add_parser("list")
    listing.add_argument("--prefix", default="")
    listing.add_argument("--out", type=Path)
    args = parser.parse_args()

    config = S3Config.from_env()
    s3 = client(config)
    if args.command != "list":
        ensure_bucket(s3, config.bucket)
    if args.command == "upload":
        print(json.dumps(upload(s3, config.bucket, args.path, args.key), ensure_ascii=False, indent=2))
    elif args.command == "upload-tree":
        results = upload_tree(s3, config.bucket, args.root, args.prefix, args.workers)
        print(
            json.dumps(
                {
                    "prefix": args.prefix,
                    "objects": len(results),
                    "bytes": sum(item["bytes"] for item in results),
                    "first": results[:3],
                    "last": results[-3:],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    elif args.command == "publish-results":
        result = publish_results(s3, config.bucket, args.root, args.prefix)
        args.out.parent.mkdir(parents=True, exist_ok=True)
        rendered = json.dumps(result, ensure_ascii=False, indent=2) + "\n"
        args.out.write_text(rendered, encoding="utf-8")
        print(rendered, end="")
    else:
        items = list_objects(s3, config.bucket, args.prefix)
        text = json.dumps(items, ensure_ascii=False, indent=2) + "\n"
        if args.out:
            args.out.parent.mkdir(parents=True, exist_ok=True)
            args.out.write_text(text, encoding="utf-8")
        print(text, end="")


if __name__ == "__main__":
    main()

