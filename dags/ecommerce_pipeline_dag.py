from datetime import datetime, timedelta
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
from include.ingestion_guards import (  # noqa: E402
    build_audit_id,
    build_deterministic_archive_key,
    ensure_non_empty_stage,
    validate_csv_header,
    verify_safe_to_ack_when_raw_missing,
)
from include.sqs_parser import (  # noqa: E402
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

# Đường dẫn dbt linh hoạt: tự resolve theo repo root (/usr/local/airflow/dbt trong Astro Docker)
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DBT_PROJECT_PATH = os.getenv("DBT_PROJECT_PATH", os.path.join(PROJECT_ROOT, "dbt"))
DBT_PROFILES_PATH = os.getenv(
    "DBT_PROFILES_PATH", os.path.join(DBT_PROJECT_PATH, "profiles.yml")
)

# Cấu hình AWS S3, SQS & Redshift Serverless (đọc từ .env / CloudFormation Outputs)
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
# 15 phút VisibilityTimeout + cơ chế gia hạn (ChangeMessageVisibility heartbeat) qua từng chặng
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


def _open_redshift_conn():
    """Khởi tạo kết nối Redshift Serverless với autocommit=False."""
    import redshift_connector

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
    conn.autocommit = False
    return conn


def _ensure_ops_audit_table(cursor, conn) -> None:
    """Đảm bảo schema ops và bảng ops.pipeline_ingestion_audit tồn tại trên Redshift."""
    cursor.execute("CREATE SCHEMA IF NOT EXISTS ops;")
    cursor.execute(
        """
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
        """
    )
    conn.commit()


def _record_audit_event(
    cursor,
    conn,
    *,
    audit_id: str,
    dag_run_id: str,
    s3_key: str,
    archive_key: str,
    table_name: str,
    rows_loaded: int,
    status: str,
    error_message: str = "",
    raise_on_error: bool = True,
    auto_commit: bool = True,
) -> None:
    """
    Ghi trạng thái (SWAPPED / ARCHIVED / COMPLETED / FAILED) vào ops.pipeline_ingestion_audit.
    - Fail-closed (`raise_on_error=True`) cho SWAPPED / ARCHIVED / COMPLETED để không bao giờ
      xóa raw/ hoặc ACK SQS khi thiếu bằng chứng audit.
    - Best-effort (`raise_on_error=False`) cho FAILED trong exception handler
      để không che mất lỗi gốc.
    """
    try:
        cursor.execute(
            """
            INSERT INTO ops.pipeline_ingestion_audit (
                audit_id, dag_run_id, s3_bucket, s3_key, archive_key,
                table_name, rows_loaded, status, error_message
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
            """,
            (
                audit_id[:256],
                dag_run_id[:256],
                S3_BUCKET[:256],
                s3_key[:1024],
                archive_key[:1024],
                table_name[:128],
                int(rows_loaded),
                status[:32],
                error_message[:1900],
            ),
        )
        if auto_commit:
            conn.commit()
    except Exception as audit_err:
        if auto_commit:
            conn.rollback()
        print(f"Lỗi khi ghi ops.pipeline_ingestion_audit ({status}): {audit_err}")
        if raise_on_error:
            raise


def _fetch_latest_audit_status(
    cursor,
    audit_id: str,
    s3_key: str | None = None,
) -> tuple[str | None, str | None]:
    """
    Lấy (status, archive_key) mới nhất của audit_id từ ops.pipeline_ingestion_audit.
    Sử dụng tie-breaker theo bậc trạng thái (COMPLETED > ARCHIVED > SWAPPED > FAILED)
    để đảm bảo deterministic ngay cả khi 2 bản ghi trùng giây recorded_at.
    """
    order_clause = """
        ORDER BY recorded_at DESC,
                 CASE status
                     WHEN 'COMPLETED' THEN 4
                     WHEN 'ARCHIVED'  THEN 3
                     WHEN 'SWAPPED'   THEN 2
                     WHEN 'FAILED'    THEN 1
                     ELSE 0
                 END DESC
        LIMIT 1;
    """
    cursor.execute(
        f"""
        SELECT status, archive_key
        FROM ops.pipeline_ingestion_audit
        WHERE audit_id = %s
        {order_clause}
        """,
        (audit_id,),
    )
    row = cursor.fetchone()
    if not row and s3_key:
        cursor.execute(
            f"""
            SELECT status, archive_key
            FROM ops.pipeline_ingestion_audit
            WHERE s3_key = %s
            {order_clause}
            """,
            (s3_key,),
        )
        row = cursor.fetchone()
    if not row:
        return None, None
    return row[0], row[1]


def _read_s3_csv_header_line(s3_hook: S3Hook, bucket_name: str, s3_key: str) -> str:
    """Đọc 4KB đầu tiên của file CSV trên S3 để kiểm tra dòng header trước khi COPY."""
    s3_client = s3_hook.get_conn()
    resp = s3_client.get_object(
        Bucket=bucket_name,
        Key=s3_key,
        Range="bytes=0-4095",
    )
    raw_bytes = resp["Body"].read()
    text = raw_bytes.decode("utf-8-sig", errors="replace")
    lines = text.splitlines()
    return lines[0] if lines else ""


def _refresh_sqs_visibility(
    sqs_client,
    receipt_handles: list[str],
    timeout_sec: int,
    stage_label: str,
    *,
    fail_on_error: bool = False,
) -> None:
    """
    Gia hạn VisibilityTimeout (Heartbeat) cho các ReceiptHandle đang xử lý.
    Khi `fail_on_error=True` (trước khi xóa raw/ ở Task 3), nếu mất lease phải fail-closed ngay.
    """
    failed_handles: list[str] = []
    for handle in receipt_handles:
        try:
            sqs_client.change_message_visibility(
                QueueUrl=SQS_QUEUE_URL,
                ReceiptHandle=handle,
                VisibilityTimeout=int(timeout_sec),
            )
        except Exception as err:
            failed_handles.append(handle)
            print(f"Cảnh báo khi gia hạn VisibilityTimeout ({stage_label}): {err}")

    if fail_on_error and failed_handles:
        raise RuntimeError(
            f"SQS visibility heartbeat failed at '{stage_label}' for "
            f"{len(failed_handles)} handle(s). Aborting archive/delete to prevent lost-lease race."
        )


def _delete_sqs_handles(
    sqs_client, receipt_handles: list[str], context_label: str
) -> list[str]:
    """Xóa SQS messages theo ReceiptHandle (ACK) và trả về danh sách handle lỗi (nếu có)."""
    failed_handles: list[str] = []
    for handle in receipt_handles:
        try:
            sqs_client.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=handle)
            print(f"✉️ SQS ACK ({context_label}): Đã xóa message {handle[:18]}... khỏi queue.")
        except Exception as err:
            failed_handles.append(handle)
            print(f"Cảnh báo khi xóa SQS message ({context_label}): {err}")
    return failed_handles


def _collect_sqs_messages(context: dict, sqs_client) -> list[dict]:
    """
    Ưu tiên tiêu thụ chính xác batch message từ Triggering AssetEvent (giữ nguyên ReceiptHandle
    và tránh race condition giữa Triggerer và direct polling).
    Chỉ gọi `receive_message()` trực tiếp làm fallback khi chạy manual (không có AssetEvent).
    """
    asset_messages = extract_messages_from_asset_events(
        context.get("triggering_asset_events")
    )
    if asset_messages:
        return deduplicate_sqs_messages(asset_messages)

    polled_messages: list[dict] = []
    try:
        resp = sqs_client.receive_message(
            QueueUrl=SQS_QUEUE_URL,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=1,
            VisibilityTimeout=SQS_VISIBILITY_TIMEOUT,
        )
        polled_messages.extend(resp.get("Messages", []))
    except Exception as err:
        print(f"Cảnh báo khi kiểm tra message từ SQS (manual fallback): {err}")

    return deduplicate_sqs_messages(polled_messages)


with DAG(
    dag_id="ecommerce_aws_data_pipeline",
    default_args=default_args,
    description=(
        "True Event-Driven AWS ELT Pipeline V5 "
        "(S3 -> SQS/DLQ AssetWatcher -> Two-Phase Validated Atomic Redshift Swap & Audit -> "
        "dbt Incremental -> Verified Deterministic Archive & Multi-Record Safe ACK)"
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
        "v5",
    ],
):

    start_pipeline = EmptyOperator(task_id="start_pipeline")

    # 1. Tiêu thụ SQS messages từ AssetWatcher, kiểm tra S3 + Safe ACK cho file missing,
    #    stage & validate toàn bộ batch trước khi swap (DELETE + INSERT + Audit SWAPPED)
    @task
    def load_raw_from_sqs(**context) -> list[dict]:
        sqs_hook = SqsHook(aws_conn_id="aws_default")
        sqs_client = sqs_hook.get_conn()
        dag_run_id = str(context.get("run_id") or "manual_run")

        all_messages = _collect_sqs_messages(context, sqs_client)
        if not all_messages:
            raise AirflowSkipException(
                "Không có message nào trong AssetEvent hoặc SQS queue -> "
                "SKIP pipeline ($0 Redshift RPU)."
            )

        print(f"Đã thu thập {len(all_messages)} SQS message(s) (đã deduplicate).")
        valid_files, test_handles = parse_sqs_s3_events(
            all_messages,
            CSV_TO_RAW_TABLE,
            expected_bucket=S3_BUCKET,
        )

        if test_handles:
            _delete_sqs_handles(sqs_client, test_handles, "s3:TestEvent")

        if not valid_files:
            raise AirflowSkipException(
                "Không có file CSV Olist hợp lệ nào trong batch -> SKIP pipeline ($0 Redshift RPU)."
            )

        s3_hook = S3Hook(aws_conn_id="aws_default")
        files_to_load: list[dict] = []
        missing_raw_files: list[dict] = []

        for file_info in valid_files:
            s3_key = file_info["s3_key"]
            table_name = file_info["table_name"]
            event_token = file_info.get("event_token") or ""
            audit_id = build_audit_id(dag_run_id, table_name, event_token=event_token)
            expected_archive_key = build_deterministic_archive_key(
                audit_id, file_info["filename"]
            )
            enriched_info = {
                **file_info,
                "audit_id": audit_id,
                "dag_run_id": dag_run_id,
                "expected_archive_key": expected_archive_key,
            }
            if s3_hook.check_for_key(s3_key, bucket_name=S3_BUCKET):
                files_to_load.append(enriched_info)
            else:
                missing_raw_files.append(enriched_info)

        conn = _open_redshift_conn()
        cursor = conn.cursor()

        loaded_items: list[dict] = []
        try:
            _ensure_ops_audit_table(cursor, conn)

            # Xác minh Safe ACK bắt buộc cho mọi file không còn trên raw/ ở Task 1 (Fix F10 & F15)
            for missing_info in missing_raw_files:
                s3_key = missing_info["s3_key"]
                audit_id = missing_info["audit_id"]
                expected_archive_key = (
                    missing_info["expected_archive_key"]
                    if missing_info.get("event_token")
                    else None
                )
                audit_status, recorded_archive_key = _fetch_latest_audit_status(
                    cursor, audit_id, s3_key=s3_key
                )
                archive_exists = bool(
                    recorded_archive_key
                    and s3_hook.check_for_key(
                        recorded_archive_key, bucket_name=S3_BUCKET
                    )
                )
                verify_safe_to_ack_when_raw_missing(
                    audit_status=audit_status,
                    archive_exists=archive_exists,
                    s3_key=s3_key,
                    archive_key=recorded_archive_key,
                    expected_archive_key=expected_archive_key,
                )
                print(
                    f"ℹ️ Xác minh duplicate an toàn cho '{s3_key}' "
                    f"(audit={audit_status}, archive='{recorded_archive_key}')."
                )
                failed_dup_handles = _delete_sqs_handles(
                    sqs_client,
                    missing_info.get("receipt_handles", []),
                    f"verified-duplicate-{missing_info['filename']}",
                )
                if failed_dup_handles:
                    raise RuntimeError(
                        f"Không thể ACK duplicate message cho '{s3_key}' "
                        f"({len(failed_dup_handles)} handles)."
                    )

            if not files_to_load:
                raise AirflowSkipException(
                    "Toàn bộ file trong SQS batch đã được xác minh archive & ACK -> SKIP."
                )

            print(
                f"Chuẩn bị stage & nạp atomic {len(files_to_load)} file CSV Olist vào Redshift: "
                f"{[f['filename'] for f in files_to_load]}"
            )

            # PHA A: Kiểm tra CSV Header + Stage vào TEMP TABLE + Empty-Stage Guard cho cả batch
            # Nếu có bất kỳ file nào lỗi header hoặc rỗng 0 dòng, chưa có bảng đích nào bị sửa!
            staged_batch: list[dict] = []
            for file_info in files_to_load:
                s3_key = file_info["s3_key"]
                table_name = file_info["table_name"]
                s3_uri = f"s3://{S3_BUCKET}/{s3_key}"
                tmp_table = f"tmp_stage_{table_name}"
                audit_id = file_info["audit_id"]

                try:
                    header_line = _read_s3_csv_header_line(s3_hook, S3_BUCKET, s3_key)
                    validate_csv_header(header_line, table_name)

                    print(f"Staging {s3_uri} -> {tmp_table} ...")
                    cursor.execute(f"DROP TABLE IF EXISTS {tmp_table};")
                    cursor.execute(
                        f"CREATE TEMP TABLE {tmp_table} (LIKE raw_data.{table_name});"
                    )
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
                    staged_rows = ensure_non_empty_stage(
                        cursor.fetchone()[0], s3_uri, table_name
                    )
                    conn.commit()
                    print(
                        f"Đã stage & kiểm tra hợp lệ {staged_rows} dòng "
                        f"từ '{file_info['filename']}'."
                    )
                except Exception as err:
                    conn.rollback()
                    _record_audit_event(
                        cursor,
                        conn,
                        audit_id=audit_id,
                        dag_run_id=dag_run_id,
                        s3_key=s3_key,
                        archive_key="",
                        table_name=table_name,
                        rows_loaded=0,
                        status="FAILED",
                        error_message=str(err),
                        raise_on_error=False,
                    )
                    raise

                _refresh_sqs_visibility(
                    sqs_client,
                    file_info.get("receipt_handles", []),
                    SQS_VISIBILITY_TIMEOUT,
                    f"post-stage-{file_info['filename']}",
                )
                staged_batch.append(
                    {
                        **file_info,
                        "tmp_table": tmp_table,
                        "rows_loaded": staged_rows,
                    }
                )

            # PHA B: Hoán đổi nguyên tử (DELETE + INSERT) + ghi Audit SWAPPED trong CÙNG transaction
            for staged_info in staged_batch:
                s3_key = staged_info["s3_key"]
                table_name = staged_info["table_name"]
                tmp_table = staged_info["tmp_table"]
                staged_rows = staged_info["rows_loaded"]
                audit_id = staged_info["audit_id"]

                try:
                    cursor.execute(f"DELETE FROM raw_data.{table_name};")
                    cursor.execute(
                        f"INSERT INTO raw_data.{table_name} SELECT * FROM {tmp_table};"
                    )
                    _record_audit_event(
                        cursor,
                        conn,
                        audit_id=audit_id,
                        dag_run_id=dag_run_id,
                        s3_key=s3_key,
                        archive_key="",
                        table_name=table_name,
                        rows_loaded=staged_rows,
                        status="SWAPPED",
                        raise_on_error=True,
                        auto_commit=False,
                    )
                    conn.commit()
                except Exception as err:
                    conn.rollback()
                    _record_audit_event(
                        cursor,
                        conn,
                        audit_id=audit_id,
                        dag_run_id=dag_run_id,
                        s3_key=s3_key,
                        archive_key="",
                        table_name=table_name,
                        rows_loaded=0,
                        status="FAILED",
                        error_message=str(err),
                        raise_on_error=False,
                    )
                    raise

                # Dọn TEMP TABLE ở khối best-effort riêng để không ghi nhầm FAILED sau khi swap
                try:
                    cursor.execute(f"DROP TABLE IF EXISTS {tmp_table};")
                    conn.commit()
                except Exception as cleanup_err:
                    conn.rollback()
                    print(f"Cảnh báo khi dọn bảng tạm {tmp_table}: {cleanup_err}")

                _refresh_sqs_visibility(
                    sqs_client,
                    staged_info.get("receipt_handles", []),
                    SQS_VISIBILITY_TIMEOUT,
                    f"post-swap-{staged_info['filename']}",
                )
                print(f"Nạp atomic thành công raw_data.{table_name} ({staged_rows} dòng)!")
                loaded_items.append(
                    {
                        k: v
                        for k, v in staged_info.items()
                        if k != "tmp_table"
                    }
                )
        finally:
            cursor.close()
            conn.close()

        return loaded_items

    # 2. Cosmos parse dbt project thành TaskGroup (staging views, incremental marts & tests)
    dbt_transform = DbtTaskGroup(
        group_id="dbt_transformation_and_tests",
        project_config=project_config,
        profile_config=profile_config,
    )

    # 3. Xác minh S3 Deterministic Archive + Audit hoàn tất cho TOÀN BỘ batch trước khi ACK SQS
    @task
    def archive_and_ack_sqs_messages(loaded_items: list[dict]) -> None:
        if not loaded_items:
            print("Không có item nào cần archive và ACK.")
            return

        s3_hook = S3Hook(aws_conn_id="aws_default")
        sqs_hook = SqsHook(aws_conn_id="aws_default")
        sqs_client = sqs_hook.get_conn()

        conn = _open_redshift_conn()
        cursor = conn.cursor()

        archived_count = 0
        handles_to_ack: list[str] = []
        try:
            _ensure_ops_audit_table(cursor, conn)

            # PHA 1: Hoàn tất Archive S3 & ghi Audit ARCHIVED -> COMPLETED cho tất cả các file
            for item in loaded_items:
                src_key = item["s3_key"]
                filename = item["filename"]
                table_name = item["table_name"]
                rows_loaded = int(item.get("rows_loaded", 0))
                dag_run_id = str(item.get("dag_run_id") or "manual_run")
                event_token = str(item.get("event_token") or "")
                audit_id = str(
                    item.get("audit_id")
                    or build_audit_id(dag_run_id, table_name, event_token=event_token)
                )
                dest_key = build_deterministic_archive_key(audit_id, filename)
                handles = item.get("receipt_handles", [])

                # Gia hạn VisibilityTimeout fail-closed trước khi sửa/xóa S3 (Fix F12)
                _refresh_sqs_visibility(
                    sqs_client,
                    handles,
                    SQS_VISIBILITY_TIMEOUT,
                    f"pre-archive-{filename}",
                    fail_on_error=True,
                )

                if s3_hook.check_for_key(src_key, bucket_name=S3_BUCKET):
                    # 1. Copy S3: raw/ -> archive/<safe_audit_id>/<filename> (deterministic)
                    s3_hook.copy_object(
                        source_bucket_key=src_key,
                        dest_bucket_key=dest_key,
                        source_bucket_name=S3_BUCKET,
                        dest_bucket_name=S3_BUCKET,
                    )
                    if not s3_hook.check_for_key(dest_key, bucket_name=S3_BUCKET):
                        raise RuntimeError(
                            f"S3 Archive verification failed: '{dest_key}' not found after copy."
                        )
                    print(f"📦 S3 Archive verified: {src_key} -> {dest_key}")

                    # 2. Ghi nhận ARCHIVED (fail-closed) TRƯỚC KHI xóa raw/
                    _record_audit_event(
                        cursor,
                        conn,
                        audit_id=audit_id,
                        dag_run_id=dag_run_id,
                        s3_key=src_key,
                        archive_key=dest_key,
                        table_name=table_name,
                        rows_loaded=rows_loaded,
                        status="ARCHIVED",
                        raise_on_error=True,
                    )

                    # 3. Dọn file khỏi raw/ và ghi nhận COMPLETED vào ops.pipeline_ingestion_audit
                    s3_hook.delete_objects(bucket=S3_BUCKET, keys=[src_key])
                    print(f"🧹 S3 Cleanup: Đã xóa {src_key} khỏi raw/.")
                    _record_audit_event(
                        cursor,
                        conn,
                        audit_id=audit_id,
                        dag_run_id=dag_run_id,
                        s3_key=src_key,
                        archive_key=dest_key,
                        table_name=table_name,
                        rows_loaded=rows_loaded,
                        status="COMPLETED",
                        raise_on_error=True,
                    )
                    archived_count += 1
                else:
                    # Nhánh Retry: raw/ không còn file -> đối chiếu đúng dest_key của lượt này
                    audit_status, recorded_archive_key = _fetch_latest_audit_status(
                        cursor, audit_id, s3_key=src_key
                    )
                    archive_exists = bool(
                        recorded_archive_key
                        and s3_hook.check_for_key(
                            recorded_archive_key, bucket_name=S3_BUCKET
                        )
                    )
                    verify_safe_to_ack_when_raw_missing(
                        audit_status=audit_status,
                        archive_exists=archive_exists,
                        s3_key=src_key,
                        archive_key=recorded_archive_key,
                        expected_archive_key=dest_key,
                    )
                    print(
                        f"ℹ️ Xác minh an toàn cho '{src_key}' (audit={audit_status}, "
                        f"archive='{recorded_archive_key}'), sẵn sàng ACK SQS."
                    )
                    if audit_status != "COMPLETED":
                        _record_audit_event(
                            cursor,
                            conn,
                            audit_id=audit_id,
                            dag_run_id=dag_run_id,
                            s3_key=src_key,
                            archive_key=dest_key,
                            table_name=table_name,
                            rows_loaded=rows_loaded,
                            status="COMPLETED",
                            raise_on_error=True,
                        )

                for handle in handles:
                    if handle not in handles_to_ack:
                        handles_to_ack.append(handle)
        finally:
            cursor.close()
            conn.close()

        # PHA 2: Chỉ ACK SQS sau khi TẤT CẢ các file trong batch đã archive & ghi audit (Fix F13)
        failed_handles = _delete_sqs_handles(
            sqs_client, handles_to_ack, "batch-verified-ack"
        )
        if failed_handles:
            raise RuntimeError(
                f"SQS ACK thất bại cho {len(failed_handles)} handle(s) sau khi archive. "
                "Cho phép Airflow retry task để hoàn tất ACK."
            )

        print(
            f"🎉 Hoàn tất chu trình Pure Event-Driven V5: {archived_count} file "
            "đã được nạp atomic vào Redshift, chạy dbt Incremental, verify archive và ACK SQS."
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
