"""
include/ingestion_guards.py
Các hàm kiểm tra toàn vẹn dữ liệu trước khi thay thế bảng đích trong Redshift.
"""


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
