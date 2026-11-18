"""
tests/test_ingestion_guards.py
Unit tests kiểm thử lớp bảo vệ dữ liệu trước khi thay thế bảng Redshift
(CSV Header Schema Guard + Empty-Stage Guard) và lớp kiểm tra bằng chứng hoàn tất
(Cross-Run Event Audit ID + Audit ARCHIVED/COMPLETED + Deterministic S3 Archive)
trước khi ACK SQS.
"""

import unittest
from include.ingestion_guards import (
    build_audit_id,
    build_deterministic_archive_key,
    ensure_non_empty_stage,
    filter_ackable_missing_raw_handles,
    validate_csv_header,
    verify_safe_to_ack_when_raw_missing,
)


class TestIngestionGuards(unittest.TestCase):

    def test_valid_non_empty_stage_passes(self):
        result = ensure_non_empty_stage(
            100, "s3://bucket/raw/olist_orders_dataset.csv", "orders"
        )
        self.assertEqual(result, 100)

    def test_zero_rows_raises_value_error(self):
        with self.assertRaises(ValueError) as ctx:
            ensure_non_empty_stage(
                0, "s3://bucket/raw/olist_orders_dataset.csv", "orders"
            )
        self.assertIn("Empty-stage guard triggered", str(ctx.exception))
        self.assertIn("raw_data.orders", str(ctx.exception))

    def test_none_or_negative_rows_raises_value_error(self):
        with self.assertRaises(ValueError):
            ensure_non_empty_stage(None, "s3://bucket/raw/empty.csv", "customers")
        with self.assertRaises(ValueError):
            ensure_non_empty_stage(-1, "s3://bucket/raw/corrupt.csv", "customers")

    def test_validate_csv_header_passes_for_exact_schema_and_bom(self):
        header = (
            '\ufeff"product_category_name","product_category_name_english"\r\n'
        )
        cols = validate_csv_header(header, "product_category_name_translation")
        self.assertEqual(
            cols,
            ("product_category_name", "product_category_name_english"),
        )

    def test_validate_csv_header_blocks_reordered_or_empty_columns(self):
        # Đảo thứ tự 2 cột cùng kiểu VARCHAR -> phải chặn ngay trước khi Redshift COPY
        reordered = "product_category_name_english,product_category_name"
        with self.assertRaises(ValueError) as ctx:
            validate_csv_header(reordered, "product_category_name_translation")
        self.assertIn("CSV header schema drift detected", str(ctx.exception))

        with self.assertRaises(ValueError):
            validate_csv_header("   ", "orders")

    def test_build_audit_id_and_deterministic_archive_key(self):
        audit_id = build_audit_id("manual__2026-11-17T12:00:00+00:00", "orders")
        self.assertEqual(audit_id, "manual__2026-11-17T12:00:00+00:00__orders")
        dest_key = build_deterministic_archive_key(audit_id, "olist_orders_dataset.csv")
        self.assertEqual(
            dest_key,
            "archive/manual__2026-11-17T12_00_00_00_00__orders/olist_orders_dataset.csv",
        )
        self.assertEqual(build_audit_id(None, "products"), "manual_run__products")

    def test_build_audit_id_prefers_event_token_and_caps_at_256_chars(self):
        # 2 DAG Runs khác nhau nhận cùng event_token -> sinh cùng audit_id bất biến
        id_run_a = build_audit_id(
            "asset_triggered__2026-11-18T11:00:00_RunA",
            "orders",
            event_token="ver_abc123xyz",
        )
        id_run_b = build_audit_id(
            "asset_triggered__2026-11-18T11:15:00_RunB",
            "orders",
            event_token="ver_abc123xyz",
        )
        self.assertEqual(id_run_a, "ver_abc123xyz__orders")
        self.assertEqual(id_run_a, id_run_b)

        # Kiểm tra giới hạn <= 256 ký tự ngay từ lúc tạo
        very_long_run = "r" * 300
        capped_id = build_audit_id(very_long_run, "product_category_name_translation")
        self.assertLessEqual(len(capped_id), 256)
        self.assertTrue(capped_id.endswith("__product_category_name_translation"))

    def test_verify_safe_to_ack_when_raw_missing_passes_when_archived_or_completed(self):
        expected = "archive/run_1__orders/olist_orders_dataset.csv"
        self.assertTrue(
            verify_safe_to_ack_when_raw_missing(
                audit_status="COMPLETED",
                archive_exists=True,
                s3_key="raw/olist_orders_dataset.csv",
                archive_key=expected,
                expected_archive_key=expected,
            )
        )
        self.assertTrue(
            verify_safe_to_ack_when_raw_missing(
                audit_status="ARCHIVED",
                archive_exists=True,
                s3_key="raw/olist_orders_dataset.csv",
                archive_key=expected,
                expected_archive_key=expected,
            )
        )

    def test_verify_safe_to_ack_blocks_when_status_is_swapped_failed_or_missing(self):
        expected = "archive/run_1__orders/olist_orders_dataset.csv"
        for unsafe_status in (None, "SWAPPED", "FAILED"):
            with self.assertRaises(RuntimeError) as ctx:
                verify_safe_to_ack_when_raw_missing(
                    audit_status=unsafe_status,
                    archive_exists=True,
                    s3_key="raw/olist_orders_dataset.csv",
                    archive_key=expected,
                    expected_archive_key=expected,
                )
            self.assertIn("Unsafe SQS ACK blocked", str(ctx.exception))

    def test_verify_safe_to_ack_blocks_when_archive_key_mismatches_or_missing(self):
        expected = "archive/run_2__orders/olist_orders_dataset.csv"
        with self.assertRaises(RuntimeError) as ctx:
            verify_safe_to_ack_when_raw_missing(
                audit_status="ARCHIVED",
                archive_exists=True,
                s3_key="raw/olist_orders_dataset.csv",
                archive_key="archive/old_run_from_last_week/olist_orders_dataset.csv",
                expected_archive_key=expected,
            )
        self.assertIn("does not match expected deterministic key", str(ctx.exception))

        with self.assertRaises(RuntimeError) as ctx2:
            verify_safe_to_ack_when_raw_missing(
                audit_status="COMPLETED",
                archive_exists=False,
                s3_key="raw/olist_orders_dataset.csv",
                archive_key=expected,
                expected_archive_key=expected,
            )
        self.assertIn("not found in S3 archive/", str(ctx2.exception))

    def test_filter_ackable_missing_raw_handles_defers_shared_multi_record_handle(self):
        # Handle "rh-shared" đang dùng chung bởi file khác trong files_to_load -> không ACK sớm
        active_load_items = [
            {
                "filename": "olist_customers_dataset.csv",
                "receipt_handles": ["rh-shared", "rh-customers-only"],
            }
        ]
        ackable = filter_ackable_missing_raw_handles(
            ["rh-shared", "rh-isolated-dup"],
            active_load_items,
        )
        self.assertEqual(ackable, ["rh-isolated-dup"])


if __name__ == "__main__":
    unittest.main()
