"""
include/ingestion_guards.py
Các hàm kiểm tra toàn vẹn dữ liệu trước khi thay thế bảng đích trong Redshift
và xác minh bằng chứng hoàn tất (Audit ARCHIVED/COMPLETED + Deterministic S3 Archive Key)
trước khi ACK SQS.
"""

import re

VALID_ARCHIVED_AUDIT_STATUSES = frozenset({"ARCHIVED", "COMPLETED"})


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


def build_audit_id(dag_run_id: str | None, table_name: str) -> str:
    """Tạo mã định danh audit_id cho cặp (dag_run_id, table_name)."""
    clean_run_id = (dag_run_id or "manual_run").strip()
    return f"{clean_run_id}__{table_name}"


def build_deterministic_archive_key(audit_id: str, filename: str) -> str:
    """
    Tạo đường dẫn archive cố định (deterministic) theo audit_id để khi task retry
    không sinh ra nhiều bản sao archive trùng lặp ở các timestamp khác nhau.
    """
    safe_audit_id = re.sub(r"[^a-zA-Z0-9_.-]+", "_", (audit_id or "manual_run").strip())
    clean_filename = filename.strip().lstrip("/")
    return f"archive/{safe_audit_id}/{clean_filename}"


def verify_safe_to_ack_when_raw_missing(
    audit_status: str | None,
    archive_exists: bool,
    s3_key: str,
    archive_key: str | None = None,
    expected_archive_key: str | None = None,
) -> bool:
    """
    Chặn ACK SQS nhầm khi file trên s3://.../raw/ không còn tồn tại.
    Chỉ cho phép ACK nếu có đủ bằng chứng của ĐÚNG lượt chạy hiện tại:
      1. Bảng ops.pipeline_ingestion_audit đã ghi nhận trạng thái ARCHIVED hoặc COMPLETED
         (trạng thái SWAPPED chưa đủ vì chứng tỏ file biến mất trước khi copy sang archive/).
      2. Đường dẫn archive_key khớp với expected_archive_key của audit_id hiện tại.
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
