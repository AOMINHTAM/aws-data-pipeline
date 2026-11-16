"""
tests/test_ingestion_guards.py
Unit tests kiểm thử lớp bảo vệ dữ liệu trước khi thay thế bảng Redshift.
"""

import unittest
from include.ingestion_guards import ensure_non_empty_stage


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


if __name__ == "__main__":
    unittest.main()
