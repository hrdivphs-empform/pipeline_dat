from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

with DAG(
    dag_id="employee_full_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["orchestration"]
) as dag:

    run_employee_pipeline = TriggerDagRunOperator(
        task_id="trigger_employee_pipeline",
        trigger_dag_id="etl_employee_pipeline",
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=30
    )

    run_benchmark_pipeline = TriggerDagRunOperator(
        task_id="trigger_benchmark_pipeline",
        trigger_dag_id="etl_benchmark_pipeline",
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=30
    )

    run_employee_pipeline >> run_benchmark_pipeline