from datetime import datetime, timedelta, timezone
import os

from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from cosmos import DbtTaskGroup, ProfileConfig, ProjectConfig

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

# Cấu hình đường dẫn dbt bên trong Docker container của Airflow
DBT_PROJECT_PATH = "/usr/local/airflow/dbt"
DBT_PROFILES_PATH = "/usr/local/airflow/dbt/profiles.yml"

# Cấu hình AWS S3 & Redshift Serverless (đọc từ .env)
S3_BUCKET = os.getenv(
    "S3_BUCKET_NAME",
    "olist-raw-data-amt-us-east-1-205506434601-us-east-1-an",
)
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
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

# Mapping tên file CSV trên S3 (raw/) -> tên bảng tương ứng trong schema raw_data
CSV_TO_RAW_TABLE = {
    "olist_orders_dataset.csv": "orders",
    "olist_order_items_dataset.csv": "order_items",
    "olist_order_payments_dataset.csv": "order_payments",
    "olist_order_reviews_dataset.csv": "order_reviews",
    "olist_customers_dataset.csv": "customers",
    "olist_products_dataset.csv": "products",
    "olist_sellers_dataset.csv": "sellers",
    "product_category_name_translation.csv": "product_category_name_translation",
    "olist_geolocation_dataset.csv": "geolocation",
}

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
    description="Event-Driven AWS ELT Pipeline (S3 Sensor -> Redshift COPY -> dbt -> Archive)",
    schedule="*/15 * * * *",  # Tự động kiểm tra S3 mỗi 15 phút
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,  # Tránh 2 lượt chạy đụng nhau trên cùng bảng raw
    tags=["ecommerce", "aws", "dbt", "redshift", "s3-sensor"],
) as dag:

    start_pipeline = EmptyOperator(task_id="start_pipeline")

    # 1. Sensor lắng nghe folder raw/ trên S3 (không gọi Redshift nếu chưa có file mới)
    wait_for_s3_files = S3KeySensor(
        task_id="wait_for_s3_files",
        bucket_name=S3_BUCKET,
        bucket_key="raw/*.csv",
        wildcard_match=True,
        aws_conn_id="aws_default",
        poke_interval=20,
        timeout=60,
        mode="reschedule",
        soft_fail=True,  # Không thấy file -> chuyển SKIPPED nhẹ nhàng, không báo lỗi đỏ
    )

    # 2. Chỉ nạp những bảng có file mới trong raw/, đảm bảo Idempotent (TRUNCATE + COPY)
    @task
    def load_raw_from_s3() -> list[str]:
        import redshift_connector

        s3_hook = S3Hook(aws_conn_id="aws_default")
        keys = s3_hook.list_keys(bucket_name=S3_BUCKET, prefix="raw/") or []
        csv_keys = [k for k in keys if k.endswith(".csv")]

        matched_files = []
        for key in csv_keys:
            filename = key.rsplit("/", 1)[-1]
            if filename in CSV_TO_RAW_TABLE:
                matched_files.append((key, filename, CSV_TO_RAW_TABLE[filename]))

        if not matched_files:
            print("Không tìm thấy file CSV hợp lệ nào trong raw/ khớp với schema raw_data.")
            return []

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

        loaded_keys = []
        try:
            for s3_key, filename, table_name in matched_files:
                s3_uri = f"s3://{S3_BUCKET}/{s3_key}"
                print(f"Đang làm mới bảng raw_data.{table_name} từ {s3_uri} ...")

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
                loaded_keys.append(s3_key)
        finally:
            cursor.close()
            conn.close()

        return loaded_keys

    # 3. Cosmos tự động dịch toàn bộ staging, marts và 9 tests thành các Airflow task
    dbt_transform = DbtTaskGroup(
        group_id="dbt_transformation_and_tests",
        project_config=project_config,
        profile_config=profile_config,
    )

    # 4. Chỉ chuyển đúng các file đã xử lý từ raw/ sang archive/ sau khi dbt & test PASS
    @task
    def archive_s3_files(loaded_keys: list[str]) -> None:
        if not loaded_keys:
            print("Không có file nào cần archive.")
            return

        s3_hook = S3Hook(aws_conn_id="aws_default")
        run_stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

        for src_key in loaded_keys:
            filename = src_key.rsplit("/", 1)[-1]
            dest_key = f"archive/{run_stamp}/{filename}"
            s3_hook.copy_object(
                source_bucket_key=src_key,
                dest_bucket_key=dest_key,
                source_bucket_name=S3_BUCKET,
                dest_bucket_name=S3_BUCKET,
            )
            print(f"Đã copy {src_key} -> {dest_key}")

        s3_hook.delete_objects(bucket=S3_BUCKET, keys=loaded_keys)
        print(f"Đã dọn {len(loaded_keys)} file khỏi raw/ sau khi xử lý xong.")

    end_pipeline = EmptyOperator(task_id="end_pipeline")

    loaded_s3_keys = load_raw_from_s3()
    archive_task = archive_s3_files(loaded_s3_keys)

    (
        start_pipeline
        >> wait_for_s3_files
        >> loaded_s3_keys
        >> dbt_transform
        >> archive_task
        >> end_pipeline
    )
