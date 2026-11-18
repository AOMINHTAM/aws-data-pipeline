-- =============================================================================
-- AWS E-Commerce Data Pipeline V5 - Operations & Ingestion Audit Table (Redshift)
-- =============================================================================
-- Lưu vết vòng đời xử lý từng file CSV (SWAPPED -> COMPLETED / FAILED)
-- phục vụ đối soát dữ liệu, điều tra sự cố DLQ và chống ACK nhầm khi retry.
-- Lưu ý: Redshift không enforce UNIQUE/PRIMARY KEY vật lý; bảng này đóng vai trò
-- Audit & Verification Ledger (không dùng làm khóa chống concurrency).
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.pipeline_ingestion_audit (
    audit_id        VARCHAR(256),
    dag_run_id      VARCHAR(256),
    s3_bucket       VARCHAR(256),
    s3_key          VARCHAR(1024),
    archive_key     VARCHAR(1024),
    table_name      VARCHAR(128),
    rows_loaded     BIGINT,
    status          VARCHAR(32),
    error_message   VARCHAR(2000),
    recorded_at     TIMESTAMP DEFAULT SYSDATE
);
