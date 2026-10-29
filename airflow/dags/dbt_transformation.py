from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag

from common.assets import RAW_AIS, RAW_PORTS_ACTIVITY

DBT_BIN = "/opt/dbt-venv/bin/dbt"          # own virtualenv, see airflow/Dockerfile
DBT_PROJECT_DIR = "/opt/airflow/dbt/ais"
DBT_PROFILES_DIR = "/opt/airflow/config/dbt"
DBT_FLAGS = (
    f"--target prod --profiles-dir {DBT_PROFILES_DIR} "
    "--target-path /tmp/dbt_target --log-path /tmp/dbt_logs"
)


@dag(
    dag_id="dbt_transform",
    schedule=[RAW_PORTS_ACTIVITY, RAW_AIS],   # runs once both raw sources were refreshed
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "snowflake", "transform"],
    doc_md="Builds the dbt project (staging -> intermediate -> marts) and runs its tests "
           "whenever activity_weekly has loaded and checked new raw data. Can also be triggered manually.",
)
def dbt_transform_dag():

    freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"cd {DBT_PROJECT_DIR} && {DBT_BIN} source freshness {DBT_FLAGS}",
    )

    deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_PROJECT_DIR} && {DBT_BIN} deps --profiles-dir {DBT_PROFILES_DIR}",
    )

    build_staging = BashOperator(
        task_id="dbt_build_staging",
        bash_command=f"cd {DBT_PROJECT_DIR} && {DBT_BIN} build --select staging intermediate {DBT_FLAGS}",
        retries=0,    # a failed dbt test is deterministic
    )

    build_marts = BashOperator(
        task_id="dbt_build_marts",
        bash_command=f"cd {DBT_PROJECT_DIR} && {DBT_BIN} build --select marts {DBT_FLAGS}",
        retries=0,
    )

    docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=f"cd {DBT_PROJECT_DIR} && {DBT_BIN} docs generate {DBT_FLAGS}",
        retries=1,    # unlike a test, this can fail on a transient connection blip
    )

    deps >> freshness >> build_staging >> build_marts >> docs


dbt_transform_dag()