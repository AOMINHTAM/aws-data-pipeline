"""
tests/test_sqs_parser.py
Unit tests kiểm thử logic phân tích cú pháp SQS Message và định tuyến file S3.
"""

import json
import unittest
from include.sqs_parser import parse_sqs_s3_events


class TestSqsParser(unittest.TestCase):

    def test_parse_valid_s3_object_created(self):
        msg = {
            "ReceiptHandle": "handle_orders_123",
            "Body": json.dumps({
                "Records": [
                    {
                        "eventVersion": "2.6",
                        "eventSource": "aws:s3",
                        "eventName": "ObjectCreated:Put",
                        "s3": {
                            "bucket": {"name": "olist-raw-data-amt"},
                            "object": {"key": "raw/olist_orders_dataset.csv", "size": 17654914},
                        },
                    }
                ]
            }),
        }

        valid_files, test_handles = parse_sqs_s3_events([msg])
        self.assertEqual(len(valid_files), 1)
        self.assertEqual(len(test_handles), 0)

        item = valid_files[0]
        self.assertEqual(item["filename"], "olist_orders_dataset.csv")
        self.assertEqual(item["s3_key"], "raw/olist_orders_dataset.csv")
        self.assertEqual(item["table_name"], "orders")
        self.assertEqual(item["receipt_handles"], ["handle_orders_123"])

    def test_parse_multiple_files_in_batch(self):
        msg1 = {
            "ReceiptHandle": "handle_customers_1",
            "Body": json.dumps({
                "Records": [
                    {
                        "eventSource": "aws:s3",
                        "eventName": "ObjectCreated:Put",
                        "s3": {"object": {"key": "raw/olist_customers_dataset.csv"}},
                    }
                ]
            }),
        }
        msg2 = {
            "ReceiptHandle": "handle_products_2",
            "Body": json.dumps({
                "Records": [
                    {
                        "eventSource": "aws:s3",
                        "eventName": "ObjectCreated:Copy",
                        "s3": {"object": {"key": "raw/olist_products_dataset.csv"}},
                    }
                ]
            }),
        }

        valid_files, _ = parse_sqs_s3_events([msg1, msg2])
        self.assertEqual(len(valid_files), 2)
        filenames = {f["filename"] for f in valid_files}
        self.assertEqual(filenames, {"olist_customers_dataset.csv", "olist_products_dataset.csv"})

    def test_parse_duplicate_messages_aggregates_receipt_handles(self):
        msg1 = {
            "ReceiptHandle": "handle_dup_1",
            "Body": json.dumps({
                "Records": [
                    {
                        "eventSource": "aws:s3",
                        "eventName": "ObjectCreated:Put",
                        "s3": {"object": {"key": "raw/olist_orders_dataset.csv"}},
                    }
                ]
            }),
        }
        msg2 = {
            "ReceiptHandle": "handle_dup_2",
            "Body": json.dumps({
                "Records": [
                    {
                        "eventSource": "aws:s3",
                        "eventName": "ObjectCreated:Put",
                        "s3": {"object": {"key": "raw/olist_orders_dataset.csv"}},
                    }
                ]
            }),
        }

        valid_files, _ = parse_sqs_s3_events([msg1, msg2])
        self.assertEqual(len(valid_files), 1)
        self.assertEqual(valid_files[0]["filename"], "olist_orders_dataset.csv")
        self.assertEqual(valid_files[0]["receipt_handles"], ["handle_dup_1", "handle_dup_2"])

    def test_parse_s3_test_event(self):
        test_msg = {
            "ReceiptHandle": "handle_test_event_999",
            "Body": json.dumps({
                "Service": "Amazon S3",
                "Event": "s3:TestEvent",
                "Time": "2026-11-13T07:04:13.110Z",
                "Bucket": "olist-raw-data-amt",
            }),
        }

        valid_files, test_handles = parse_sqs_s3_events([test_msg])
        self.assertEqual(len(valid_files), 0)
        self.assertEqual(test_handles, ["handle_test_event_999"])

    def test_parse_url_encoded_keys(self):
        msg = {
            "ReceiptHandle": "handle_encoded_1",
            "Body": json.dumps({
                "Records": [
                    {
                        "eventSource": "aws:s3",
                        "eventName": "ObjectCreated:Put",
                        "s3": {"object": {"key": "raw/product%5Fcategory%5Fname%5Ftranslation.csv"}},
                    }
                ]
            }),
        }

        valid_files, _ = parse_sqs_s3_events([msg])
        self.assertEqual(len(valid_files), 1)
        self.assertEqual(valid_files[0]["filename"], "product_category_name_translation.csv")
        self.assertEqual(valid_files[0]["table_name"], "product_category_name_translation")

    def test_ignore_unrelated_or_invalid_events(self):
        invalid_json = {"ReceiptHandle": "h1", "Body": "not-a-json"}
        non_s3 = {
            "ReceiptHandle": "h2",
            "Body": json.dumps({"Records": [{"eventSource": "aws:sns", "eventName": "notify"}]}),
        }
        unknown_csv = {
            "ReceiptHandle": "h3",
            "Body": json.dumps({
                "Records": [
                    {
                        "eventSource": "aws:s3",
                        "eventName": "ObjectCreated:Put",
                        "s3": {"object": {"key": "raw/unknown_table_data.csv"}},
                    }
                ]
            }),
        }
        non_csv = {
            "ReceiptHandle": "h4",
            "Body": json.dumps({
                "Records": [
                    {
                        "eventSource": "aws:s3",
                        "eventName": "ObjectCreated:Put",
                        "s3": {"object": {"key": "raw/notes.txt"}},
                    }
                ]
            }),
        }

        valid_files, test_handles = parse_sqs_s3_events(
            [invalid_json, non_s3, unknown_csv, non_csv]
        )
        self.assertEqual(len(valid_files), 0)
        self.assertEqual(len(test_handles), 0)


if __name__ == "__main__":
    unittest.main()
