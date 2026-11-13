from datetime import datetime, timedelta, timezone
import os
import sys

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.sqs import SqsHook
from airflow.providers.amazon.aws.sensors.sqs import SqsSensor
from cosmos import DbtTaskGroup, ProfileConfig, ProjectConfig

# Đảm bảo import được module từ include/
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from include.sqs_parser import CSV_TO_RAW_TABLE, parse_sqs_s3_events

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

with DAG(
    dag_id="ecommerce_aws_data_pipeline",
    default_args=default_args,
    description="Event-Driven AWS ELT Pipeline V4 (S3 -> SQS -> Redshift COPY -> dbt -> Archive & ACK)",
    schedule="*/5 * * * *",  # Kiểm tra SQS queue mỗi 5 phút (Zero Polling Latency)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,  # Tránh 2 lượt chạy đụng nhau trên cùng bảng raw
    tags=["ecommerce", "aws", "dbt", "redshift", "sqs-event-driven", "v4"],
) as dag:

    start_pipeline = EmptyOperator(task_id="start_pipeline")

    # 1. Sensor lắng nghe AWS SQS Queue (s3:ObjectCreated events)
    # Hàng đợi trống -> timeout 45s -> soft_fail chuyển SKIPPED ($0 Redshift RPU)
    # Giữ lại message (delete_message_on_reception=False), chỉ ACK sau khi dbt pass
    wait_for_sqs_messages = SqsSensor(
        task_id="wait_for_sqs_messages",
        sqs_queue=SQS_QUEUE_URL,
        aws_conn_id="aws_default",
        max_messages=10,
        num_batches=1,
        wait_time_seconds=5,
        visibility_timeout=300,
        delete_message_on_reception=False,
        mode="reschedule",
        poke_interval=15,
        timeout=45,
        soft_fail=True,
    )

    # 2. Tiêu thụ message từ SQS, bóc tách s3_key, nạp vào Redshift (Idempotent TRUNCATE + COPY)
    @task
    def load_raw_from_sqs(**context) -> list[dict]:
        import redshift_connector

        # SqsSensor đẩy danh sách message vào XCom dưới key="messages"
        ti = context["ti"]
        sensor_messages = ti.xcom_pull(
            task_ids="wait_for_sqs_messages",
            key="messages",
        ) or []

        sqs_hook = SqsHook(aws_conn_id="aws_default")
        sqs_client = sqs_hook.get_conn()

        # Gom message nhận từ sensor và hút cạn các message còn tồn đọng trong queue
        all_messages = list(sensor_messages)
        try:
            for _ in range(5):
                resp = sqs_client.receive_message(
                    QueueUrl=SQS_QUEUE_URL,
                    MaxNumberOfMessages=10,
                    WaitTimeSeconds=1,
                    VisibilityTimeout=300,
                )
                extra = resp.get("Messages", [])
                if not extra:
                    break
                all_messages.extend(extra)
        except Exception as err:
            print(f"Cảnh báo khi kiểm tra thêm message từ SQS: {err}")

        if not all_messages:
            raise AirflowSkipException("Không có message nào trong SQS queue -> SKIP pipeline.")

        print(f"Đã nhận tổng cộng {len(all_messages)} messages từ SQS.")

        valid_files, test_handles = parse_sqs_s3_events(all_messages, CSV_TO_RAW_TABLE)

        # Xóa ngay các s3:TestEvent khỏi queue nếu có
        for handle in test_handles:
            try:
                sqs_client.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=handle)
                print(f"Đã dọn dẹp s3:TestEvent khỏi SQS: {handle[:20]}...")
            except Exception as err:
                print(f"Lỗi khi xóa test event: {err}")

        if not valid_files:
            raise AirflowSkipException(
                "Không có file CSV Olist hợp lệ nào cần nạp từ SQS batch -> SKIP pipeline."
            )

        print(
            f"Tìm thấy {len(valid_files)} file CSV Olist hợp lệ cần nạp: "
            f"{[f['filename'] for f in valid_files]}"
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
        conn.autocommit = True
        cursor = conn.cursor()

        loaded_items = []
        try:
            for file_info in valid_files:
                s3_key = file_info["s3_key"]
                table_name = file_info["table_name"]
                s3_uri = f"s3://{S3_BUCKET}/{s3_key}"

                print(f"Đang làm mới raw_data.{table_name} từ {s3_uri} ...")
                cursor.execute(f"TRUNCATE TABLE raw_data.{table_name};")
                copy_sql = f"""
                    COPY raw_data.{table_name}
                    FROM '{s3_uri}'
                    IAM_ROLE '{REDSHIFT_IAM_ROLE}'
                    FORMAT AS CSV
                    DELIMITER ','
                    IGNOREHEADER 1
                    REGION '{AWS_REGION}'
                    TIMEFORMAT 'auto';
                """
                cursor.execute(copy_sql)
                print(f"Nạp thành công raw_data.{table_name}!")
                loaded_items.append(file_info)
        finally:
            cursor.close()
            conn.close()

        return loaded_items

    # 3. Cosmos parse dbt project thành TaskGroup (chạy staging views, marts tables & 9 tests)
    dbt_transform = DbtTaskGroup(
        group_id="dbt_transformation_and_tests",
        project_config=project_config,
        profile_config=profile_config,
    )

    # 4. Chỉ archive S3 và ACK (delete) message khỏi SQS KHI VÀ CHỈ KHI dbt & test PASS 100%
    @task
    def archive_and_ack_sqs_messages(loaded_items: list[dict]) -> None:
        if not loaded_items:
            print("Không có item nào cần archive và ACK.")
            return

        s3_hook = S3Hook(aws_conn_id="aws_default")
        sqs_hook = SqsHook(aws_conn_id="aws_default")
        sqs_client = sqs_hook.get_conn()
        run_stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

        keys_to_delete_from_raw = []
        for item in loaded_items:
            src_key = item["s3_key"]
            filename = item["filename"]
            dest_key = f"archive/{run_stamp}/{filename}"

            # 1. Copy S3: raw/ -> archive/<run_stamp>/
            s3_hook.copy_object(
                source_bucket_key=src_key,
                dest_bucket_key=dest_key,
                source_bucket_name=S3_BUCKET,
                dest_bucket_name=S3_BUCKET,
            )
            print(f"📦 S3 Archive: {src_key} -> {dest_key}")
            keys_to_delete_from_raw.append(src_key)

            # 2. Xóa message tương ứng khỏi SQS (At-Least-Once Delivery -> Exactly-Once effect)
            for receipt_handle in item.get("receipt_handles", []):
                try:
                    sqs_client.delete_message(
                        QueueUrl=SQS_QUEUE_URL,
                        ReceiptHandle=receipt_handle,
                    )
                    print(f"✉️ SQS ACK: Đã xóa message cho '{filename}' khỏi SQS queue.")
                except Exception as err:
                    print(f"Cảnh báo khi xóa SQS message cho {filename}: {err}")

        # 3. Dọn dẹp file trong raw/ trên S3
        if keys_to_delete_from_raw:
            s3_hook.delete_objects(bucket=S3_BUCKET, keys=keys_to_delete_from_raw)
            print(f"🧹 S3 Cleanup: Đã dọn {len(keys_to_delete_from_raw)} file khỏi raw/ sau khi xử lý xong.")

        print(
            f"🎉 Hoàn tất chu trình Event-Driven: {len(keys_to_delete_from_raw)} file "
            f"đã được nạp vào Redshift, chạy dbt, archive trên S3 và ACK khỏi SQS."
        )

    end_pipeline = EmptyOperator(task_id="end_pipeline")

    # Orchestration Data Flow
    loaded_files = load_raw_from_sqs()
    archive_task = archive_and_ack_sqs_messages(loaded_files)

    (
        start_pipeline
        >> wait_for_sqs_messages
        >> loaded_files
        >> dbt_transform
        >> archive_task
        >> end_pipeline
    )
