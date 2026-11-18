#!/usr/bin/env python3
"""
scripts/empty_versioned_bucket.py
Xóa toàn bộ Object Versions và DeleteMarkers trong S3 bucket có bật Versioning
(hỗ trợ lặp phân trang không giới hạn số lượng > 1.000 versions)
trước khi gọi `aws cloudformation delete-stack` để tránh lỗi BucketNotEmpty.
"""

import json
import subprocess
import sys


def empty_versioned_bucket(bucket_name: str, region: str = "us-east-1") -> int:
    """Xóa toàn bộ Versions và DeleteMarkers theo từng lô 500 objects cho đến khi bucket rỗng."""
    deleted_total = 0

    while True:
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
            break

        data = json.loads(res.stdout)
        objects_to_delete = [
            {"Key": item["Key"], "VersionId": item["VersionId"]}
            for section in ("Versions", "DeleteMarkers")
            for item in data.get(section, [])
            if "Key" in item and "VersionId" in item
        ]
        if not objects_to_delete:
            break

        batch_progress = 0
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
            del_res = subprocess.run(
                del_cmd, capture_output=True, text=True, check=False
            )
            if del_res.returncode != 0:
                err_detail = (del_res.stderr or del_res.stdout or "").strip()
                raise RuntimeError(
                    f"aws s3api delete-objects thất bại trên s3://{bucket_name}: {err_detail}"
                )
            batch_progress += len(batch)

        if batch_progress == 0:
            break
        deleted_total += batch_progress

    return deleted_total


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 scripts/empty_versioned_bucket.py <bucket_name> [region]")
        sys.exit(1)
    target_bucket = sys.argv[1]
    target_region = sys.argv[2] if len(sys.argv) > 2 else "us-east-1"
    removed = empty_versioned_bucket(target_bucket, target_region)
    print(f"🧹 Đã xóa {removed} object version(s) / delete marker(s) khỏi s3://{target_bucket}.")
