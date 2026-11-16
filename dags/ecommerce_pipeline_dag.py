from datetime import datetime, timedelta, timezone
import os
import sys

from airflow.exceptions import AirflowSkipException
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.sqs import SqsHook
from airflow.providers.amazon.aws.triggers.sqs import SqsSensorTrigger
from airflow.sdk import DAG, Asset, AssetWatcher, task
from cosmos import DbtTaskGroup, ProfileConfig, ProjectConfig

# Đảm bảo import được module từ include/
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from include.sqs_parser import (
    CSV_TO_RAW_TABLE,
    deduplicate_sqs_messages,
    extract_messages_from_asset_events,
    parse_sqs_s3_events,
)

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

# Cấu hình đường dẫn dbt bên trong Docker container của Airflow
DBT_PROJECT_PATH = "/usr/local/airflow/dbt"
DBT_PROFILES_PATH = "/usr/local/airflow/dbt/profiles.yml"

# Cấu hình AWS S3, SQS & Redshift Serverless (đọc từ .env)
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
S3_BUCKET = os.getenv(
    "S3_BUCKET_NAME",
    "olist-raw-data-amt-us-east-1-205506434601-us-east-1-an",
)
SQS_QUEUE_NAME = os.getenv("SQS_QUEUE_NAME", "olist-s3-raw-arrival-queue")
SQS_QUEUE_URL = os.getenv(
    "SQS_QUEUE_URL",
    f"https://sqs.{AWS_REGION}.amazonaws.com/205506434601/{SQS_QUEUE_NAME}",
)
# 15 phút VisibilityTimeout: đủ dư cho Redshift staging COPY + dbt incremental + 9 tests
SQS_VISIBILITY_TIMEOUT = int(os.getenv("SQS_VISIBILITY_TIMEOUT", "900"))

REDSHIFT_IAM_ROLE = os.getenv(
    "REDSHIFT_IAM_ROLE",
    "arn:aws:iam::205506434601:role/RedshiftS3ReadOnlyRole",
)
REDSHIFT_HOST = os.getenv(
    "REDSHIFT_HOST",
    "ecommerce-workgroup.205506434601.us-east-1.redshift-serverless.amazonaws.com",
)
REDSHIFT_PORT = int(os.getenv("REDSHIFT_PORT", "5439"))
REDSHIFT_DB = os.getenv("REDSHIFT_DB", "dev")
REDSHIFT_USER = os.getenv("REDSHIFT_USER", "admin")

profile_config = ProfileConfig(
    profile_name="ecommerce_dbt",
    target_name="dev",
    profiles_yml_filepath=DBT_PROFILES_PATH,
)

project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
)

# Airflow 3 Event-Driven Triggerer: Lắng nghe SQS qua AWS Long Polling (20s),
# KHÔNG cần cron schedule="*/5 * * * *" và KHÔNG tạo các lượt chạy rỗng (SKIPPED).
# Khi bạn tắt Astro (`astro dev stop`), Triggerer ngừng hoàn toàn -> $0.00 chi phí AWS.
sqs_arrival_trigger = SqsSensorTrigger(
    sqs_queue=SQS_QUEUE_URL,
    aws_conn_id="aws_default",
    max_messages=10,
    num_batches=1,
    wait_time_seconds=20,
    visibility_timeout=SQS_VISIBILITY_TIMEOUT,
    delete_message_on_reception=False,
    waiter_delay=5,
)

sqs_raw_arrival_asset = Asset(
    name="olist_s3_raw_arrival_queue",
    uri=f"sqs://{SQS_QUEUE_NAME}",
    watchers=[
        AssetWatcher(
            name="s3_sqs_arrival_watcher",
            trigger=sqs_arrival_trigger,
        )
    ],
)


def _delete_sqs_handles(sqs_client, receipt_handles: list[str], context_label: str) -> None:
    """Xóa danh sách SQS messages theo ReceiptHandle (ACK) sau khi xử lý xong hoặc lọc bỏ."""
    for handle in receipt_handles:
        try:
            sqs_client.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=handle)
            print(f"✉️ SQS ACK ({context_label}): Đã xóa message {handle[:18]}... khỏi queue.")
        except Exception as err:
            print(f"Cảnh báo khi xóa SQS message ({context_label}): {err}")


def _collect_sqs_messages(context: dict, sqs_client) -> list[dict]:
    """Gom messages từ Triggering AssetEvent và hút cạn các messages đến cùng lô trong SQS."""
    asset_messages = extract_messages_from_asset_events(
        context.get("triggering_asset_events")
    )

    polled_messages: list[dict] = []
    try:
        for _ in range(5):
            resp = sqs_client.receive_message(
                QueueUrl=SQS_QUEUE_URL,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=1,
                VisibilityTimeout=SQS_VISIBILITY_TIMEOUT,
            )
            extra = resp.get("Messages", [])
            if not extra:
                break
            polled_messages.extend(extra)
    except Exception as err:
        print(f"Cảnh báo khi kiểm tra thêm message từ SQS: {err}")

    return deduplicate_sqs_messages(asset_messages + polled_messages)


with DAG(
    dag_id="ecommerce_aws_data_pipeline",
    default_args=default_args,
    description=(
        "True Event-Driven AWS ELT Pipeline V4 "
        "(S3 -> SQS AssetWatcher -> Atomic Redshift Staging Swap -> dbt Incremental -> Archive & ACK)"
    ),
    schedule=[sqs_raw_arrival_asset],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,  # Tránh 2 lượt chạy đụng nhau trên cùng bảng raw
    tags=[
        "ecommerce",
        "aws",
        "dbt",
        "redshift",
        "sqs-event-driven",
        "asset-watcher",
        "v4",
    ],
) as dag:

    start_pipeline = EmptyOperator(task_id="start_pipeline")

    # 1. Tiêu thụ SQS messages từ AssetWatcher (hoặc manual trigger), kiểm tra S3 tồn tại
    #    và nạp vào Redshift qua TEMP TABLE + Atomic Transaction (DELETE + INSERT, không dùng TRUNCATE)
    @task
    def load_raw_from_sqs(**context) -> list[dict]:
        import redshift_connector

        sqs_hook = SqsHook(aws_conn_id="aws_default")
        sqs_client = sqs_hook.get_conn()

        all_messages = _collect_sqs_messages(context, sqs_client)
        if not all_messages:
            raise AirflowSkipException(
                "Không có message nào trong AssetEvent hoặc SQS queue -> SKIP pipeline ($0 Redshift RPU)."
            )

        print(f"Đã thu thập {len(all_messages)} SQS message(s) (đã deduplicate).")
        valid_files, test_handles = parse_sqs_s3_events(all_messages, CSV_TO_RAW_TABLE)

        if test_handles:
            _delete_sqs_handles(sqs_client, test_handles, "s3:TestEvent")

        if not valid_files:
            raise AirflowSkipException(
                "Không có file CSV Olist hợp lệ nào trong batch -> SKIP pipeline ($0 Redshift RPU)."
            )

        # Kiểm tra file còn trên S3 trước khi đánh thức Redshift (Application-level Idempotent Guard)
        s3_hook = S3Hook(aws_conn_id="aws_default")
        files_to_load: list[dict] = []
        for file_info in valid_files:
            s3_key = file_info["s3_key"]
            if s3_hook.check_for_key(s3_key, bucket_name=S3_BUCKET):
                files_to_load.append(file_info)
            else:
                print(
                    f"ℹ️ Bỏ qua '{s3_key}' (không còn trên raw/ — đã được xử lý trước đó / duplicate SQS delivery)."
                )
                _delete_sqs_handles(
                    sqs_client,
                    file_info.get("receipt_handles", []),
                    f"duplicate-{file_info['filename']}",
                )

        if not files_to_load:
            raise AirflowSkipException(
                "Toàn bộ file trong SQS batch đã được xử lý trước đó -> SKIP ($0 Redshift RPU)."
            )

        print(
            f"Chuẩn bị nạp atomic {len(files_to_load)} file CSV Olist vào Redshift: "
            f"{[f['filename'] for f in files_to_load]}"
        )

        redshift_password = os.getenv("DBT_REDSHIFT_PASSWORD")
        if not redshift_password:
            raise ValueError("Thiếu biến môi trường DBT_REDSHIFT_PASSWORD trong .env!")

        conn = redshift_connector.connect(
            host=REDSHIFT_HOST,
            port=REDSHIFT_PORT,
            database=REDSHIFT_DB,
            user=REDSHIFT_USER,
            password=redshift_password,
            ssl=True,
        )
        # Bắt buộc autocommit=False để DELETE + INSERT nằm trọn trong 1 transaction có thể ROLLBACK
        conn.autocommit = False
        cursor = conn.cursor()

        loaded_items: list[dict] = []
        try:
            for file_info in files_to_load:
                s3_key = file_info["s3_key"]
                table_name = file_info["table_name"]
                s3_uri = f"s3://{S3_BUCKET}/{s3_key}"
                tmp_table = f"tmp_stage_{table_name}"

                try:
                    # Bước A: Nạp vào TEMP TABLE trước — nếu CSV lỗi/mất mạng, bảng đích raw_data không bị ảnh hưởng
                    print(f"Staging {s3_uri} -> {tmp_table} ...")
                    cursor.execute(f"DROP TABLE IF EXISTS {tmp_table};")
                    cursor.execute(f"CREATE TEMP TABLE {tmp_table} (LIKE raw_data.{table_name});")
                    copy_sql = f"""
                        COPY {tmp_table}
                        FROM '{s3_uri}'
                        IAM_ROLE '{REDSHIFT_IAM_ROLE}'
                        FORMAT AS CSV
                        DELIMITER ','
                        IGNOREHEADER 1
                        REGION '{AWS_REGION}'
                        TIMEFORMAT 'auto';
                    """
                    cursor.execute(copy_sql)
                    cursor.execute(f"SELECT COUNT(*) FROM {tmp_table};")
                    staged_rows = cursor.fetchone()[0]
                    conn.commit()
                    print(f"Đã stage {staged_rows} dòng từ '{file_info['filename']}'.")

                    # Bước B: Hoán đổi nguyên tử (Atomic Swap) — KHÔNG dùng TRUNCATE vì TRUNCATE tự commit ở Redshift!
                    cursor.execute(f"DELETE FROM raw_data.{table_name};")
                    cursor.execute(
                        f"INSERT INTO raw_data.{table_name} SELECT * FROM {tmp_table};"
                    )
                    conn.commit()

                    cursor.execute(f"DROP TABLE IF EXISTS {tmp_table};")
                    conn.commit()
                except Exception:
                    conn.rollback()
                    raise

                print(f"Nạp atomic thành công raw_data.{table_name} ({staged_rows} dòng)!")
                loaded_items.append({**file_info, "rows_loaded": staged_rows})
        finally:
            cursor.close()
            conn.close()

        return loaded_items

    # 2. Cosmos parse dbt project thành TaskGroup (chạy staging views, incremental marts & 9 tests)
    dbt_transform = DbtTaskGroup(
        group_id="dbt_transformation_and_tests",
        project_config=project_config,
        profile_config=profile_config,
    )

    # 3. Chỉ archive S3 -> xóa khỏi raw/ -> ACK (delete) SQS KHI VÀ CHỈ KHI dbt & 9/9 tests PASS
    @task
    def archive_and_ack_sqs_messages(loaded_items: list[dict]) -> None:
        if not loaded_items:
            print("Không có item nào cần archive và ACK.")
            return

        s3_hook = S3Hook(aws_conn_id="aws_default")
        sqs_hook = SqsHook(aws_conn_id="aws_default")
        sqs_client = sqs_hook.get_conn()
        run_stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

        archived_count = 0
        for item in loaded_items:
            src_key = item["s3_key"]
            filename = item["filename"]
            dest_key = f"archive/{run_stamp}/{filename}"

            if s3_hook.check_for_key(src_key, bucket_name=S3_BUCKET):
                # 1. Copy S3: raw/ -> archive/<run_stamp>/
                s3_hook.copy_object(
                    source_bucket_key=src_key,
                    dest_bucket_key=dest_key,
                    source_bucket_name=S3_BUCKET,
                    dest_bucket_name=S3_BUCKET,
                )
                print(f"📦 S3 Archive: {src_key} -> {dest_key}")

                # 2. Dọn file khỏi raw/ trước khi ACK SQS để tránh file mồ côi trong raw/ nếu delete lỗi
                s3_hook.delete_objects(bucket=S3_BUCKET, keys=[src_key])
                print(f"🧹 S3 Cleanup: Đã xóa {src_key} khỏi raw/.")
                archived_count += 1
            else:
                print(f"ℹ️ File {src_key} đã được archive trước đó, tiếp tục ACK SQS.")

            # 3. Xóa message tương ứng khỏi SQS (At-Least-Once Delivery -> Idempotent Consumer)
            _delete_sqs_handles(
                sqs_client,
                item.get("receipt_handles", []),
                filename,
            )

        print(
            f"🎉 Hoàn tất chu trình Pure Event-Driven: {archived_count} file "
            f"đã được nạp atomic vào Redshift, chạy dbt Incremental, archive trên S3 và ACK khỏi SQS."
        )

    end_pipeline = EmptyOperator(task_id="end_pipeline")

    # Orchestration Data Flow (không cần polling task trung gian)
    loaded_files = load_raw_from_sqs()
    archive_task = archive_and_ack_sqs_messages(loaded_files)

    (
        start_pipeline
        >> loaded_files
        >> dbt_transform
        >> archive_task
        >> end_pipeline
    )
