"""
tests/test_ingestion_guards.py
Unit tests kiểm thử lớp bảo vệ dữ liệu trước khi thay thế bảng Redshift
và lớp kiểm tra bằng chứng hoàn tất (Audit ARCHIVED/COMPLETED + Deterministic S3 Archive)
trước khi ACK SQS.
"""

import unittest
from include.ingestion_guards import (
    build_audit_id,
    build_deterministic_archive_key,
    ensure_non_empty_stage,
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

    def test_build_audit_id_and_deterministic_archive_key(self):
        audit_id = build_audit_id("manual__2026-11-17T12:00:00+00:00", "orders")
        self.assertEqual(audit_id, "manual__2026-11-17T12:00:00+00:00__orders")
        dest_key = build_deterministic_archive_key(audit_id, "olist_orders_dataset.csv")
        self.assertEqual(
            dest_key,
            "archive/manual__2026-11-17T12_00_00_00_00__orders/olist_orders_dataset.csv",
        )
        self.assertEqual(build_audit_id(None, "products"), "manual_run__products")

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


if __name__ == "__main__":
    unittest.main()
