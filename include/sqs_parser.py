"""
include/sqs_parser.py
Module tiện ích phân tích và xử lý các sự kiện AWS S3 Event Notification đẩy vào SQS Queue.
"""

from collections.abc import Mapping
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


def _is_newer_s3_event(candidate_sequencer: str, current_sequencer: str) -> bool:
    """
    So sánh thứ tự ghi của 2 sự kiện S3 trên cùng một key dựa vào chuỗi hex `sequencer`
    (AWS S3 đảm bảo thứ tự từ điển của `sequencer` phản ánh đúng thứ tự ghi theo thời gian).
    Giúp tránh trường hợp SQS Standard giao lệch thứ tự khiến event cũ ghi đè metadata event mới.
    """
    cand = (candidate_sequencer or "").strip()
    curr = (current_sequencer or "").strip()
    if cand and curr:
        return cand >= curr
    if curr and not cand:
        return False
    return True


def _extract_event_token(object_data: Mapping[str, Any], message_id: str = "") -> str:
    """
    Trích xuất định danh sự kiện S3 bất biến (ưu tiên versionId -> eTag -> sequencer -> MessageId)
    để giữ audit_id và deterministic archive key ổn định qua mọi lần SQS redelivery.
    """
    version_id = str(object_data.get("versionId") or "").strip()
    if version_id and version_id.lower() != "null":
        return f"ver_{version_id}"

    etag = str(object_data.get("eTag") or "").strip().strip('"')
    if etag:
        return f"etag_{etag}"

    sequencer = str(object_data.get("sequencer") or "").strip()
    if sequencer:
        return f"seq_{sequencer}"

    clean_msg_id = (message_id or "").strip()
    if clean_msg_id:
        return f"msg_{clean_msg_id}"

    return ""


def parse_sqs_s3_events(
    messages: list[dict[str, Any]],
    table_mapping: dict[str, str] | None = None,
    expected_bucket: str | None = None,
) -> tuple[list[dict[str, Any]], list[str]]:
    """
    Phân tích danh sách message nhận từ SQS:
    - Lọc bỏ các message lỗi cú pháp hoặc không liên quan.
    - Nhận diện `s3:TestEvent` để xóa ngay.
    - Bóc tách `s3.object.key` cho các sự kiện `ObjectCreated:*`.
    - Kiểm tra `expected_bucket` (nếu cấu hình) và khóa cứng flat prefix `raw/<filename>`
      để chặn gộp nhầm subfolder và loại bỏ hoàn toàn nguy cơ SQL injection qua S3 key.
    - Trích xuất `event_token` (`versionId`/`eTag`/`sequencer`) phục vụ cross-run Safe ACK.

    Returns:
        tuple (valid_files, test_event_handles)
        - valid_files: Danh sách dict [{'s3_key', 'filename', 'table_name',
          'receipt_handles', 'event_token', 'version_id', 'etag', 'sequencer'}]
        - test_event_handles: Danh sách ReceiptHandle của s3:TestEvent cần xóa ngay
    """
    if table_mapping is None:
        table_mapping = CSV_TO_RAW_TABLE

    files_by_key: dict[str, dict[str, Any]] = {}
    test_event_handles: list[str] = []

    for msg in messages:
        if not isinstance(msg, dict):
            continue

        receipt_handle = msg.get("ReceiptHandle")
        extra_handles = msg.get("ReceiptHandles")
        msg_handles: list[str] = []
        if isinstance(extra_handles, list):
            msg_handles.extend(h for h in extra_handles if isinstance(h, str) and h)
        if isinstance(receipt_handle, str) and receipt_handle and receipt_handle not in msg_handles:
            msg_handles.append(receipt_handle)

        message_id = str(msg.get("MessageId") or "").strip()
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
            for handle in msg_handles:
                if handle not in test_event_handles:
                    test_event_handles.append(handle)
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
            if not isinstance(s3_data, Mapping):
                continue

            bucket_info = s3_data.get("bucket", {})
            bucket_name = (
                str(bucket_info.get("name") or "").strip()
                if isinstance(bucket_info, Mapping)
                else ""
            )
            if expected_bucket and bucket_name and bucket_name != expected_bucket:
                continue

            object_data = s3_data.get("object", {})
            if not isinstance(object_data, Mapping):
                continue

            raw_key = str(object_data.get("key") or "")
            if not raw_key:
                continue

            # Decode URL (S3 mã hóa dấu cách thành +, %20, v.v.)
            s3_key = urllib.parse.unquote_plus(raw_key)

            if not s3_key.endswith(".csv"):
                continue

            filename = s3_key.rsplit("/", 1)[-1]
            # Khóa cứng hợp đồng flat prefix `raw/<allowlisted_filename>`
            if filename not in table_mapping or s3_key != f"raw/{filename}":
                continue

            table_name = table_mapping[filename]
            event_token = _extract_event_token(object_data, message_id=message_id)
            version_id = str(object_data.get("versionId") or "").strip()
            etag = str(object_data.get("eTag") or "").strip().strip('"')
            sequencer = str(object_data.get("sequencer") or "").strip()

            if s3_key not in files_by_key:
                files_by_key[s3_key] = {
                    "s3_key": s3_key,
                    "filename": filename,
                    "table_name": table_name,
                    "receipt_handles": [],
                    "event_token": event_token,
                    "version_id": version_id,
                    "etag": etag,
                    "sequencer": sequencer,
                }
            else:
                if _is_newer_s3_event(sequencer, files_by_key[s3_key].get("sequencer", "")):
                    if event_token:
                        files_by_key[s3_key]["event_token"] = event_token
                    if version_id:
                        files_by_key[s3_key]["version_id"] = version_id
                    if etag:
                        files_by_key[s3_key]["etag"] = etag
                    if sequencer:
                        files_by_key[s3_key]["sequencer"] = sequencer

            for handle in msg_handles:
                if handle not in files_by_key[s3_key]["receipt_handles"]:
                    files_by_key[s3_key]["receipt_handles"].append(handle)

    return list(files_by_key.values()), test_event_handles


def extract_messages_from_asset_events(triggering_asset_events: Any) -> list[dict[str, Any]]:
    """
    Trích xuất danh sách SQS message dictionaries từ context['triggering_asset_events']
    do Airflow 3 AssetWatcher (SqsSensorTrigger) truyền vào DAG run.

    Hỗ trợ cả dict thông thường lẫn Airflow 3 `TriggeringAssetEventsAccessor`
    (vốn kế thừa `collections.abc.Mapping`, không phải `dict`).
    Bỏ qua các event báo lỗi (status='error') để không đánh thức Redshift ngoài ý muốn.
    """
    if not isinstance(triggering_asset_events, Mapping):
        return []

    messages: list[dict[str, Any]] = []
    for events in triggering_asset_events.values():
        if not isinstance(events, (list, tuple)):
            continue
        for asset_event in events:
            extra = getattr(asset_event, "extra", None)
            if extra is None and isinstance(asset_event, Mapping):
                extra = asset_event.get("extra")
            if not isinstance(extra, Mapping):
                continue

            payload = extra.get("payload", extra)
            if not isinstance(payload, Mapping):
                continue

            if payload.get("status") == "error":
                continue

            batch = payload.get("message_batch") or payload.get("messages") or []
            if isinstance(batch, list):
                messages.extend(msg for msg in batch if isinstance(msg, dict))

    return messages


def deduplicate_sqs_messages(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Loại bỏ các SQS message trùng lặp:
    - Ưu tiên định danh theo `MessageId` (để nhận diện cùng 1 logical message bị nhận 2 lần
      với 2 `ReceiptHandle` khác nhau), đồng thời gộp toàn bộ `ReceiptHandle` vào `ReceiptHandles`
      để bước ACK dọn sạch mọi handle đang mở.
    """
    unique_by_key: dict[str, dict[str, Any]] = {}

    for msg in messages:
        if not isinstance(msg, dict):
            continue

        dedup_key = (
            msg.get("MessageId")
            or msg.get("ReceiptHandle")
            or json.dumps(msg, sort_keys=True, default=str)
        )
        handle = msg.get("ReceiptHandle")

        if dedup_key not in unique_by_key:
            msg_copy = dict(msg)
            existing_handles = list(msg_copy.get("ReceiptHandles") or [])
            if isinstance(handle, str) and handle and handle not in existing_handles:
                existing_handles.append(handle)
            if existing_handles:
                msg_copy["ReceiptHandles"] = existing_handles
            unique_by_key[dedup_key] = msg_copy
        else:
            stored = unique_by_key[dedup_key]
            handles = list(stored.get("ReceiptHandles") or [])
            if isinstance(handle, str) and handle:
                if handle not in handles:
                    handles.append(handle)
                stored["ReceiptHandle"] = handle
            if handles:
                stored["ReceiptHandles"] = handles

    return list(unique_by_key.values())
