"""
include/ingestion_guards.py
Các hàm kiểm tra toàn vẹn dữ liệu trước khi thay thế bảng đích trong Redshift
(CSV Header Schema Guard + Empty-Stage Guard) và xác minh bằng chứng hoàn tất
(Audit ARCHIVED/COMPLETED + Deterministic S3 Archive Key) trước khi ACK SQS.
"""

import re

VALID_ARCHIVED_AUDIT_STATUSES = frozenset({"ARCHIVED", "COMPLETED"})

# Hợp đồng schema cột CSV chuẩn cho 9 bảng raw_data (khớp 100% với scripts/create_raw_tables.sql)
EXPECTED_CSV_HEADERS: dict[str, tuple[str, ...]] = {
    "orders": (
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ),
    "order_items": (
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        "shipping_limit_date",
        "price",
        "freight_value",
    ),
    "order_payments": (
        "order_id",
        "payment_sequential",
        "payment_type",
        "payment_installments",
        "payment_value",
    ),
    "order_reviews": (
        "review_id",
        "order_id",
        "review_score",
        "review_comment_title",
        "review_comment_message",
        "review_creation_date",
        "review_answer_timestamp",
    ),
    "customers": (
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
    ),
    "products": (
        "product_id",
        "product_category_name",
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
    ),
    "sellers": (
        "seller_id",
        "seller_zip_code_prefix",
        "seller_city",
        "seller_state",
    ),
    "product_category_name_translation": (
        "product_category_name",
        "product_category_name_english",
    ),
    "geolocation": (
        "geolocation_zip_code_prefix",
        "geolocation_lat",
        "geolocation_lng",
        "geolocation_city",
        "geolocation_state",
    ),
}


def validate_csv_header(
    header_line: str | None,
    table_name: str,
    expected_headers_map: dict[str, tuple[str, ...]] | None = None,
) -> tuple[str, ...]:
    """
    Kiểm tra dòng header của file CSV khớp chính xác về tên cột và thứ tự cột
    với hợp đồng schema của bảng đích trước khi gọi Redshift COPY (chặn lỗi đảo cột).

    Raises:
        ValueError: Nếu header rỗng hoặc lệch tên/thứ tự cột so với schema chuẩn.
    """
    if expected_headers_map is None:
        expected_headers_map = EXPECTED_CSV_HEADERS

    raw_line = (header_line or "").lstrip("\ufeff").strip()
    if not raw_line:
        raise ValueError(
            f"CSV header validation failed for raw_data.{table_name}: empty header line."
        )

    actual_columns = tuple(
        col.strip().strip('"').strip("'").lower() for col in raw_line.split(",")
    )
    expected_columns = expected_headers_map.get(table_name)
    if expected_columns and actual_columns != expected_columns:
        raise ValueError(
            f"CSV header schema drift detected for raw_data.{table_name}: "
            f"expected {expected_columns}, got {actual_columns}."
        )
    return actual_columns


def ensure_non_empty_stage(staged_rows: int | None, s3_uri: str, table_name: str) -> int:
    """
    Ngăn chặn thao tác DELETE + INSERT xóa trắng bảng đích khi file CSV rỗng (0 dòng).

    Raises:
        ValueError: Nếu staged_rows là None hoặc <= 0.
    """
    if staged_rows is None or int(staged_rows) <= 0:
        row_count = 0 if staged_rows is None else staged_rows
        raise ValueError(
            f"Empty-stage guard triggered: '{s3_uri}' staged {row_count} rows "
            f"into staging table. Aborting swap to protect raw_data.{table_name}."
        )
    return int(staged_rows)


def build_audit_id(
    dag_run_id: str | None,
    table_name: str,
    event_token: str | None = None,
) -> str:
    """
    Tạo mã định danh audit_id giới hạn <= 256 ký tự.
    Nếu có event_token từ S3 Event (versionId / eTag / sequencer / MessageId),
    ưu tiên dùng event_token để audit_id bất biến qua mọi lần SQS redelivery
    sang các DAG Run khác nhau.
    """
    clean_table = (table_name or "unknown_table").strip()
    clean_token = (event_token or "").strip()
    base_id = clean_token if clean_token else (dag_run_id or "manual_run").strip()
    max_base_len = max(1, 256 - len(clean_table) - 2)
    return f"{base_id[:max_base_len]}__{clean_table}"


def build_deterministic_archive_key(audit_id: str, filename: str) -> str:
    """
    Tạo đường dẫn archive cố định (deterministic) theo audit_id để khi task retry
    hoặc SQS redeliver không sinh ra nhiều bản sao archive trùng lặp.
    """
    safe_audit_id = re.sub(r"[^a-zA-Z0-9_.-]+", "_", (audit_id or "manual_run").strip())
    clean_filename = filename.strip().lstrip("/")
    return f"archive/{safe_audit_id}/{clean_filename}"


def build_s3_copy_source(
    bucket: str,
    key: str,
    version_id: str | None = None,
) -> dict[str, str]:
    """
    Tạo tham số `CopySource` cho boto3 `s3.copy_object`.
    Khi S3 Bucket bật Versioning và event có `versionId` hợp lệ (khác rỗng và khác "null"),
    ghim trực tiếp `VersionId` để đảm bảo bản ghi lưu sang `archive/` đúng với phiên bản
    đối tượng của sự kiện kích hoạt, tránh lệch phiên bản nếu key bị ghi đè nhanh.
    """
    copy_source: dict[str, str] = {
        "Bucket": bucket.strip(),
        "Key": key.strip().lstrip("/"),
    }
    clean_version = (version_id or "").strip()
    if clean_version and clean_version.lower() != "null":
        copy_source["VersionId"] = clean_version
    return copy_source


def verify_safe_to_ack_when_raw_missing(
    audit_status: str | None,
    archive_exists: bool,
    s3_key: str,
    archive_key: str | None = None,
    expected_archive_key: str | None = None,
) -> bool:
    """
    Chặn ACK SQS nhầm khi file trên s3://.../raw/ không còn tồn tại.
    Chỉ cho phép ACK nếu có đủ bằng chứng:
      1. Bảng ops.pipeline_ingestion_audit đã ghi nhận trạng thái ARCHIVED hoặc COMPLETED
         (trạng thái SWAPPED chưa đủ vì chứng tỏ file biến mất trước khi copy sang archive/).
      2. Đường dẫn archive_key khớp với expected_archive_key của event/audit_id hiện tại.
      3. File bản sao thực sự tồn tại tại archive_key trên S3.

    Raises:
        RuntimeError: Nếu thiếu bằng chứng archive hoặc file không tồn tại trong archive/.
    """
    normalized_status = (audit_status or "").strip().upper()
    if normalized_status not in VALID_ARCHIVED_AUDIT_STATUSES:
        raise RuntimeError(
            f"Unsafe SQS ACK blocked for missing raw object '{s3_key}': "
            f"audit status is '{audit_status}' (expected ARCHIVED or COMPLETED)."
        )
    if expected_archive_key and archive_key != expected_archive_key:
        raise RuntimeError(
            f"Unsafe SQS ACK blocked for missing raw object '{s3_key}': "
            f"recorded archive_key '{archive_key}' does not match "
            f"expected deterministic key '{expected_archive_key}'."
        )
    if not archive_key or not archive_exists:
        raise RuntimeError(
            f"Unsafe SQS ACK blocked for missing raw object '{s3_key}': "
            f"verified archive object '{archive_key or 'N/A'}' not found in S3 archive/."
        )
    return True


def filter_ackable_missing_raw_handles(
    missing_handles: list[str],
    active_load_items: list[dict],
) -> list[str]:
    """
    Chặn ACK sớm ở Task 1 khi một SQS message chứa nhiều S3 Records
    (1 file đã missing/archived nhưng file khác cùng ReceiptHandle vẫn đang nằm
    trong `files_to_load` chờ stage/dbt/archive ở Task 3).
    Chỉ trả về các ReceiptHandle không còn gắn với bất kỳ file nào trong `active_load_items`.
    """
    in_use_handles = {
        handle
        for item in (active_load_items or [])
        for handle in (item.get("receipt_handles") or [])
        if isinstance(handle, str) and handle
    }
    return [
        handle
        for handle in (missing_handles or [])
        if isinstance(handle, str) and handle and handle not in in_use_handles
    ]

