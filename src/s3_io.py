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
    listing = sub.add_parser("list")
    listing.add_argument("--out", type=Path)
    args = parser.parse_args()

    config = S3Config.from_env()
    s3 = client(config)
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
    else:
        items = []
        token = None
        while True:
            kwargs = {"Bucket": config.bucket}
            if token:
                kwargs["ContinuationToken"] = token
            response = s3.list_objects_v2(**kwargs)
            items.extend(
                {"key": item["Key"], "bytes": item["Size"], "etag": item["ETag"].strip('"')}
                for item in response.get("Contents", [])
            )
            if not response.get("IsTruncated"):
                break
            token = response["NextContinuationToken"]
        text = json.dumps(items, ensure_ascii=False, indent=2) + "\n"
        if args.out:
            args.out.parent.mkdir(parents=True, exist_ok=True)
            args.out.write_text(text, encoding="utf-8")
        print(text, end="")


if __name__ == "__main__":
    main()

