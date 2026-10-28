from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig
import os

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Đường dẫn của dbt bên trong container Docker của Airflow
DBT_PROJECT_PATH = "/usr/local/airflow/dbt"
DBT_PROFILES_PATH = "/usr/local/airflow/dbt/profiles.yml"

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
    description="End-to-End AWS E-Commerce Data Pipeline (S3 -> Redshift -> dbt -> Marts)",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ecommerce", "aws", "dbt", "redshift"],
) as dag:

    start_pipeline = EmptyOperator(
        task_id="start_pipeline"
    )

    # Cosmos tự động dịch toàn bộ staging, marts và test thành các task con trực quan
    dbt_transform = DbtTaskGroup(
        group_id="dbt_transformation_and_tests",
        project_config=project_config,
        profile_config=profile_config,
    )

    end_pipeline = EmptyOperator(
        task_id="end_pipeline"
    )

    start_pipeline >> dbt_transform >> end_pipeline
