from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.sdk import DAG

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

# DAG bảo trì tùy chọn: đối soát toàn bộ dữ liệu lịch sử (--full-refresh)
# nhằm bắt các đơn hàng/review đến trễ vượt quá lookback window 3 ngày.
# Mặc định PAUSED (`is_paused_upon_creation=True`) để đảm bảo $0.00 chi phí AWS ngoài ý muốn.
with DAG(
    dag_id="ecommerce_dbt_full_refresh_maintenance",
    default_args=default_args,
    description=(
        "Optional weekly dbt --full-refresh reconciliation for deep late-arriving data "
        "(paused by default for $0 FinOps safety)"
    ),
    schedule="@weekly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    tags=["ecommerce", "dbt", "maintenance", "full-refresh", "finops", "v4"],
):

    start_maintenance = EmptyOperator(task_id="start_maintenance")

    dbt_full_refresh_marts = BashOperator(
        task_id="dbt_full_refresh_incremental_marts",
        bash_command=(
            "dbt build --full-refresh "
            "--select fct_orders dim_customers "
            "--project-dir /usr/local/airflow/dbt "
            "--profiles-dir /usr/local/airflow/dbt"
        ),
    )

    end_maintenance = EmptyOperator(task_id="end_maintenance")

    start_maintenance >> dbt_full_refresh_marts >> end_maintenance
