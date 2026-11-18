"""
tests/test_sqs_parser.py
Unit tests kiểm thử logic phân tích cú pháp SQS Message, định tuyến file S3,
kiểm tra hợp đồng prefix raw/<filename>, event_token và deduplication theo MessageId.
"""

from collections.abc import Mapping
import json
import unittest
from types import SimpleNamespace
from include.sqs_parser import (
    deduplicate_sqs_messages,
    extract_messages_from_asset_events,
    parse_sqs_s3_events,
)


class TestSqsParser(unittest.TestCase):

    def test_parse_valid_s3_object_created(self):
        msg = {
            "MessageId": "m-orders-1",
            "ReceiptHandle": "handle_orders_123",
            "Body": json.dumps({
                "Records": [
                    {
                        "eventVersion": "2.6",
                        "eventSource": "aws:s3",
                        "eventName": "ObjectCreated:Put",
                        "s3": {
                            "bucket": {"name": "olist-raw-data-amt"},
                            "object": {
                                "key": "raw/olist_orders_dataset.csv",
                                "size": 17654914,
                                "versionId": "v12345",
                                "eTag": "etag999",
                                "sequencer": "0055AED6DCD90281E5",
                            },
                        },
                    }
                ]
            }),
        }

        valid_files, test_handles = parse_sqs_s3_events(
            [msg], expected_bucket="olist-raw-data-amt"
        )
        self.assertEqual(len(valid_files), 1)
        self.assertEqual(len(test_handles), 0)

        item = valid_files[0]
        self.assertEqual(item["filename"], "olist_orders_dataset.csv")
        self.assertEqual(item["s3_key"], "raw/olist_orders_dataset.csv")
        self.assertEqual(item["table_name"], "orders")
        self.assertEqual(item["receipt_handles"], ["handle_orders_123"])
        self.assertEqual(item["event_token"], "ver_v12345")
        self.assertEqual(item["version_id"], "v12345")

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
                        "s3": {
                            "object": {
                                "key": "raw/product%5Fcategory%5Fname%5Ftranslation.csv"
                            }
                        },
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

    def test_reject_subfolder_collisions_wrong_bucket_and_special_char_keys(self):
        # Chặn gộp nhầm subfolder (raw/batch_1/orders.csv), sai bucket hoặc key chứa dấu nháy đơn
        subfolder_msg = {
            "ReceiptHandle": "h-sub",
            "Body": json.dumps({
                "Records": [
                    {
                        "eventSource": "aws:s3",
                        "eventName": "ObjectCreated:Put",
                        "s3": {
                            "bucket": {"name": "expected-bucket"},
                            "object": {"key": "raw/batch_01/olist_orders_dataset.csv"},
                        },
                    }
                ]
            }),
        }
        sql_inj_msg = {
            "ReceiptHandle": "h-inj",
            "Body": json.dumps({
                "Records": [
                    {
                        "eventSource": "aws:s3",
                        "eventName": "ObjectCreated:Put",
                        "s3": {
                            "bucket": {"name": "expected-bucket"},
                            "object": {"key": "raw/bad'quote/olist_orders_dataset.csv"},
                        },
                    }
                ]
            }),
        }
        wrong_bucket_msg = {
            "ReceiptHandle": "h-wrong-bkt",
            "Body": json.dumps({
                "Records": [
                    {
                        "eventSource": "aws:s3",
                        "eventName": "ObjectCreated:Put",
                        "s3": {
                            "bucket": {"name": "other-bucket"},
                            "object": {"key": "raw/olist_orders_dataset.csv"},
                        },
                    }
                ]
            }),
        }
        valid_files, _ = parse_sqs_s3_events(
            [subfolder_msg, sql_inj_msg, wrong_bucket_msg],
            expected_bucket="expected-bucket",
        )
        self.assertEqual(len(valid_files), 0)

    def test_deduplicate_same_message_id_with_new_receipt_handle_preserves_both_handles(self):
        msg_first_delivery = {
            "MessageId": "msg-same-100",
            "ReceiptHandle": "rh-old-1",
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
        msg_redelivery = {
            "MessageId": "msg-same-100",
            "ReceiptHandle": "rh-new-2",
            "Body": msg_first_delivery["Body"],
        }

        deduped = deduplicate_sqs_messages([msg_first_delivery, msg_redelivery])
        self.assertEqual(len(deduped), 1)
        self.assertEqual(deduped[0]["ReceiptHandle"], "rh-new-2")
        self.assertEqual(deduped[0]["ReceiptHandles"], ["rh-old-1", "rh-new-2"])

        valid_files, _ = parse_sqs_s3_events(deduped)
        self.assertEqual(len(valid_files), 1)
        self.assertEqual(
            valid_files[0]["receipt_handles"],
            ["rh-old-1", "rh-new-2"],
        )

    def test_extract_messages_from_asset_events_and_deduplicate(self):
        sqs_msg = {
            "MessageId": "msg-1",
            "ReceiptHandle": "rh-1",
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
        error_event = SimpleNamespace(
            extra={"payload": {"status": "error", "message": "transient network error"}}
        )
        valid_event = SimpleNamespace(
            extra={"payload": {"status": "success", "message_batch": [sqs_msg]}}
        )

        extracted = extract_messages_from_asset_events({"asset": [error_event, valid_event]})
        self.assertEqual(len(extracted), 1)
        self.assertEqual(extracted[0]["ReceiptHandle"], "rh-1")

        # Gộp cùng message nhận trực tiếp từ SQS -> không bị nhân đôi
        deduped = deduplicate_sqs_messages(extracted + [dict(sqs_msg)])
        self.assertEqual(len(deduped), 1)

    def test_extract_messages_supports_airflow3_mapping_accessor(self):
        class FakeAssetEventsAccessor(Mapping):
            def __init__(self, data):
                self._data = data

            def __getitem__(self, key):
                return self._data[key]

            def __iter__(self):
                return iter(self._data)

            def __len__(self):
                return len(self._data)

        sqs_msg = {"MessageId": "m-accessor", "ReceiptHandle": "rh-accessor", "Body": "{}"}
        accessor = FakeAssetEventsAccessor(
            {
                "sqs://olist-v5-s3-raw-arrival-queue-dev/": [
                    SimpleNamespace(
                        extra={
                            "from_trigger": True,
                            "payload": {"status": "success", "message_batch": [sqs_msg]},
                        }
                    )
                ]
            }
        )
        extracted = extract_messages_from_asset_events(accessor)
        self.assertEqual(len(extracted), 1)
        self.assertEqual(extracted[0]["ReceiptHandle"], "rh-accessor")


if __name__ == "__main__":
    unittest.main()
