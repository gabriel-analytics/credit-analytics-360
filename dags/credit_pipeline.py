"""
DAG: credit_analytics_360
Pipeline diário: validação → dbt staging → intermediate → marts → relatório
"""

from __future__ import annotations

import os
import json
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

DATA_DIR = Path(os.getenv("AIRFLOW_DATA_DIR", "/opt/airflow/data"))
DBT_DIR  = Path(os.getenv("AIRFLOW_DBT_DIR",  "/opt/airflow/dbt_credit"))
PARQUET_FILES = [
    "customers.parquet",
    "contracts.parquet",
    "payments.parquet",
    "app_events.parquet",
    "proposals.parquet",
    "collections.parquet",
]

default_args = {
    "owner": "gabriel.pacheco",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def validate_sources(**ctx):
    missing = [f for f in PARQUET_FILES if not (DATA_DIR / f).exists()]
    if missing:
        raise FileNotFoundError(f"Parquet files missing: {missing}")
    sizes = {f: round((DATA_DIR / f).stat().st_size / 1_048_576, 1) for f in PARQUET_FILES}
    print("Source validation PASSED")
    for name, mb in sizes.items():
        print(f"  {name:<30} {mb:>7.1f} MB")
    ctx["ti"].xcom_push(key="source_sizes", value=sizes)


def generate_quality_report(**ctx):
    import duckdb

    db_path = DATA_DIR / "financeflow.duckdb"
    con = duckdb.connect(str(db_path), read_only=True)

    report = {}

    report["customers"] = con.execute(
        "SELECT COUNT(*) as n, SUM(CASE WHEN income_declared=0 THEN 1 ELSE 0 END) as zero_income "
        "FROM stg_customers"
    ).fetchdf().to_dict(orient="records")[0]

    report["payments"] = con.execute(
        "SELECT COUNT(*) as n, "
        "  SUM(CASE WHEN is_defaulted THEN 1 ELSE 0 END) as defaults, "
        "  ROUND(AVG(days_late),1) as avg_days_late "
        "FROM stg_payments"
    ).fetchdf().to_dict(orient="records")[0]

    report["contracts"] = con.execute(
        "SELECT COUNT(*) as n, "
        "  SUM(CASE WHEN is_defaulted THEN 1 ELSE 0 END) as defaults "
        "FROM stg_contracts"
    ).fetchdf().to_dict(orient="records")[0]

    con.close()
    ctx["ti"].xcom_push(key="quality_report", value=report)
    print("Quality report generated:", json.dumps(report, indent=2))


def notify_success(**ctx):
    ti = ctx["ti"]
    sizes  = ti.xcom_pull(task_ids="validate_sources", key="source_sizes") or {}
    report = ti.xcom_pull(task_ids="generate_quality_report", key="quality_report") or {}

    print()
    print("=" * 52)
    print("  CREDIT ANALYTICS 360 — PIPELINE CONCLUIDO")
    print("=" * 52)
    print(f"  Run date : {ctx['ds']}")
    for name, mb in sizes.items():
        print(f"  {name:<30} {mb:>6.1f} MB")
    if report:
        pay = report.get("payments", {})
        n   = pay.get("n", 0)
        d   = pay.get("defaults", 0)
        pct = round(d / n * 100, 1) if n else 0
        print(f"  Inadimplencia: {pct}% ({d:,}/{n:,} pagamentos)")
    print("=" * 52)


with DAG(
    dag_id="credit_analytics_360",
    default_args=default_args,
    description="FinanceFlow Bank — pipeline dbt completo",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["credit", "financeflow", "analytics"],
) as dag:

    t_validate = PythonOperator(
        task_id="validate_sources",
        python_callable=validate_sources,
    )

    t_stg_run = BashOperator(
        task_id="run_dbt_staging",
        bash_command=f"cd {DBT_DIR} && dbt run --select staging.*",
    )

    t_stg_test = BashOperator(
        task_id="test_dbt_staging",
        bash_command=f"cd {DBT_DIR} && dbt test --select staging.*",
    )

    t_int_run = BashOperator(
        task_id="run_dbt_intermediate",
        bash_command=f"cd {DBT_DIR} && dbt run --select intermediate.*",
    )

    t_int_test = BashOperator(
        task_id="test_dbt_intermediate",
        bash_command=f"cd {DBT_DIR} && dbt test --select intermediate.*",
    )

    t_mrt_run = BashOperator(
        task_id="run_dbt_marts",
        bash_command=f"cd {DBT_DIR} && dbt run --select marts.*",
    )

    t_mrt_test = BashOperator(
        task_id="test_dbt_marts",
        bash_command=f"cd {DBT_DIR} && dbt test --select marts.*",
    )

    t_quality = PythonOperator(
        task_id="generate_quality_report",
        python_callable=generate_quality_report,
    )

    t_notify = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
    )

    (
        t_validate
        >> t_stg_run
        >> t_stg_test
        >> t_int_run
        >> t_int_test
        >> t_mrt_run
        >> t_mrt_test
        >> t_quality
        >> t_notify
    )
