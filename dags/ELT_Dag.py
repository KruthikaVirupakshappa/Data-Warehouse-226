from pendulum import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook

DBT_PROJECT_DIR = "/opt/airflow/dbt/stocks_elt"
DBT_CMD = "dbt"

conn = BaseHook.get_connection("snowflake_conn")

dbt_env = {
    "DBT_USER": conn.login,
    "DBT_PASSWORD": conn.password,
    "DBT_ACCOUNT": conn.extra_dejson.get("account"),
    "DBT_SCHEMA": conn.schema,
    "DBT_DATABASE": conn.extra_dejson.get("database"),
    "DBT_ROLE": conn.extra_dejson.get("role"),
    "DBT_WAREHOUSE": conn.extra_dejson.get("warehouse"),
    "DBT_TYPE": "snowflake",
    "PATH": "/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin",
}

default_args = {"retries": 1, "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id="BuildELT_dbt",
    start_date=datetime(2025, 11, 23),
    description="Airflow DAG that runs dbt",
    schedule="45 2 * * *",
    catchup=False,
    is_paused_upon_creation=False,
    default_args=default_args,
) as dag:

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=(
            "set -euo pipefail; "
            f"cd {DBT_PROJECT_DIR}; "
            f"{DBT_CMD} snapshot --project-dir . --profiles-dir ."
        ),
        env=dbt_env,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "set -euo pipefail; "
            f"cd {DBT_PROJECT_DIR}; "
            f"{DBT_CMD} run --project-dir . --profiles-dir ."
        ),
        env=dbt_env,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "set -euo pipefail; "
            f"cd {DBT_PROJECT_DIR}; "
            f"{DBT_CMD} test --project-dir . --profiles-dir ."
        ),
        env=dbt_env,
    )

    dbt_snapshot >> dbt_run >> dbt_test
