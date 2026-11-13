"""
include/sqs_parser.py
Module tiện ích phân tích và xử lý các sự kiện AWS S3 Event Notification đẩy vào SQS Queue.
"""

import json
from typing import Any
import urllib.parse

# Mapping tên file CSV trên S3 (raw/) -> tên bảng tương ứng trong schema raw_data
CSV_TO_RAW_TABLE = {
    "olist_orders_dataset.csv": "orders",
    "olist_order_items_dataset.csv": "order_items",
    "olist_order_payments_dataset.csv": "order_payments",
    "olist_order_reviews_dataset.csv": "order_reviews",
    "olist_customers_dataset.csv": "customers",
    "olist_products_dataset.csv": "products",
    "olist_sellers_dataset.csv": "sellers",
    "product_category_name_translation.csv": "product_category_name_translation",
    "olist_geolocation_dataset.csv": "geolocation",
}


def parse_sqs_s3_events(
    messages: list[dict[str, Any]],
    table_mapping: dict[str, str] | None = None,
) -> tuple[list[dict[str, Any]], list[str]]:
    """
    Phân tích danh sách message nhận từ SQS:
    - Lọc bỏ các message lỗi cú pháp hoặc không liên quan.
    - Nhận diện `s3:TestEvent` để xóa ngay.
    - Bóc tách `s3.object.key` cho các sự kiện `ObjectCreated:*`.
    - Decode URL-encoded key (ví dụ khoảng trắng, ký tự đặc biệt).
    - Gom nhóm theo filename và lưu toàn bộ `ReceiptHandle` tương ứng để ACK sau.

    Returns:
        tuple (valid_files, test_event_handles)
        - valid_files: Danh sách dict [{'s3_key', 'filename', 'table_name', 'receipt_handles'}]
        - test_event_handles: Danh sách ReceiptHandle của s3:TestEvent cần xóa ngay
    """
    if table_mapping is None:
        table_mapping = CSV_TO_RAW_TABLE

    files_by_name: dict[str, dict[str, Any]] = {}
    test_event_handles: list[str] = []

    for msg in messages:
        if not isinstance(msg, dict):
            continue

        receipt_handle = msg.get("ReceiptHandle")
        body_raw = msg.get("Body")

        if not body_raw:
            continue

        try:
            body = json.loads(body_raw) if isinstance(body_raw, str) else body_raw
        except Exception:
            continue

        if not isinstance(body, dict):
            continue

        # 1. Phát hiện s3:TestEvent (S3 tự gửi khi bật notification để test kết nối)
        if body.get("Event") == "s3:TestEvent":
            if receipt_handle:
                test_event_handles.append(receipt_handle)
            continue

        # 2. Xử lý danh sách Records của S3 Event
        records = body.get("Records")
        if not isinstance(records, list):
            continue

        for record in records:
            if not isinstance(record, dict):
                continue

            if record.get("eventSource") != "aws:s3":
                continue

            event_name = str(record.get("eventName", ""))
            if not event_name.startswith("ObjectCreated"):
                continue

            s3_data = record.get("s3", {})
            object_data = s3_data.get("object", {})
            raw_key = object_data.get("key", "")
            if not raw_key:
                continue

            # Decode URL (S3 mã hóa dấu cách thành +, %20, v.v.)
            s3_key = urllib.parse.unquote_plus(raw_key)

            if not s3_key.endswith(".csv"):
                continue

            filename = s3_key.rsplit("/", 1)[-1]
            if filename in table_mapping:
                table_name = table_mapping[filename]
                if filename not in files_by_name:
                    files_by_name[filename] = {
                        "s3_key": s3_key,
                        "filename": filename,
                        "table_name": table_name,
                        "receipt_handles": [],
                    }

                if (
                    receipt_handle
                    and receipt_handle not in files_by_name[filename]["receipt_handles"]
                ):
                    files_by_name[filename]["receipt_handles"].append(receipt_handle)

    return list(files_by_name.values()), test_event_handles
