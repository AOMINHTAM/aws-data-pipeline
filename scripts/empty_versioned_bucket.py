#!/usr/bin/env python3
"""
scripts/empty_versioned_bucket.py
Xóa toàn bộ Object Versions và DeleteMarkers trong S3 bucket có bật Versioning
trước khi gọi `aws cloudformation delete-stack` để tránh lỗi BucketNotEmpty.
"""

import json
import subprocess
import sys


def empty_versioned_bucket(bucket_name: str, region: str = "us-east-1") -> int:
    """Xóa toàn bộ Versions và DeleteMarkers theo từng lô 500 objects."""
    list_cmd = [
        "aws",
        "s3api",
        "list-object-versions",
        "--bucket",
        bucket_name,
        "--region",
        region,
        "--output",
        "json",
    ]
    res = subprocess.run(list_cmd, capture_output=True, text=True, check=False)
    if res.returncode != 0 or not res.stdout.strip():
        return 0

    data = json.loads(res.stdout)
    objects_to_delete = [
        {"Key": item["Key"], "VersionId": item["VersionId"]}
        for section in ("Versions", "DeleteMarkers")
        for item in data.get(section, [])
        if "Key" in item and "VersionId" in item
    ]

    deleted_total = 0
    for idx in range(0, len(objects_to_delete), 500):
        batch = objects_to_delete[idx : idx + 500]
        payload = json.dumps({"Objects": batch, "Quiet": True})
        del_cmd = [
            "aws",
            "s3api",
            "delete-objects",
            "--bucket",
            bucket_name,
            "--region",
            region,
            "--delete",
            payload,
        ]
        subprocess.run(del_cmd, capture_output=True, text=True, check=False)
        deleted_total += len(batch)

    return deleted_total


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 scripts/empty_versioned_bucket.py <bucket_name> [region]")
        sys.exit(1)
    target_bucket = sys.argv[1]
    target_region = sys.argv[2] if len(sys.argv) > 2 else "us-east-1"
    removed = empty_versioned_bucket(target_bucket, target_region)
    print(f"🧹 Đã xóa {removed} object version(s) / delete marker(s) khỏi s3://{target_bucket}.")
