from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd

CSV_PATH = "/opt/airflow/data/benchmark.csv"

def load_raw_benchmark():
    df = pd.read_csv(CSV_PATH)
    if df.empty:
        raise ValueError("Benchmark CSV is empty")

    pg = PostgresHook(postgres_conn_id="postgres_dwh")
    engine = pg.get_sqlalchemy_engine()
    conn = pg.get_conn()
    conn.autocommit = True
    cursor = conn.cursor()

    cursor.execute("TRUNCATE raw.benchmark RESTART IDENTITY CASCADE;")
    df.to_sql("benchmark", engine, schema="raw", if_exists="append", index=False)
    cursor.close()
    conn.close()

def load_dim_benchmark_group():
    sql = """
    TRUNCATE dwh.dim_benchmark_group RESTART IDENTITY CASCADE;
    INSERT INTO dwh.dim_benchmark_group (
        job_nature_key, division_key, department_key, title_key
    )
    SELECT DISTINCT
        j.job_nature_key,
        dv.division_key,
        dp.department_key,
        dt.title_key
    FROM raw.benchmark b
    JOIN dwh.dim_job_nature j ON b.group_function = j.group_function
    JOIN dwh.dim_division dv ON b.division = dv.division_name
    JOIN dwh.dim_department dp ON b.department = dp.department_name
    JOIN dwh.dim_title dt ON b.title = dt.title;
    """
    PostgresHook(postgres_conn_id="postgres_dwh").run(sql)

def load_fact_benchmark():
    sql = """
    TRUNCATE dwh.fact_competency_benchmark RESTART IDENTITY CASCADE;
    INSERT INTO dwh.fact_competency_benchmark (
        benchmark_group_key, competency_key, benchmark_value
    )
    SELECT
        bg.benchmark_group_key,
        c.competency_key,
        v.val
    FROM raw.benchmark b
    JOIN dwh.dim_job_nature j ON b.group_function = j.group_function
    JOIN dwh.dim_division dv ON b.division = dv.division_name
    JOIN dwh.dim_department dp ON b.department = dp.department_name
    JOIN dwh.dim_title dt ON b.title = dt.title
    JOIN dwh.dim_benchmark_group bg
      ON bg.job_nature_key = j.job_nature_key
     AND bg.division_key = dv.division_key
     AND bg.department_key = dp.department_key
     AND bg.title_key = dt.title_key
    CROSS JOIN LATERAL (
        VALUES
        ('communication', b.communication),
        ('digital_literacy', b.digital_literacy),
        ('data_analysis', b.data_analysis),
        ('problem_solving', b.problem_solving),
        ('critical_thinking', b.critical_thinking),
        ('continuous_learning', b.continuous_learning),
        ('teamwork_leadership', b.teamwork_leadership),
        ('strategic_thinking', b.strategic_thinking),
        ('talent_management', b.talent_management),
        ('creative_thinking', b.creative_thinking),
        ('resilience', b.resilience),
        ('ai_bigdata', b.ai_bigdata),
        ('analytical_thinking', b.analytical_thinking)
    ) v(name, val)
    JOIN dwh.dim_competency_type c ON LOWER(TRIM(c.competency_name)) = LOWER(TRIM(v.name))
    WHERE v.val IS NOT NULL;
    """
    PostgresHook(postgres_conn_id="postgres_dwh").run(sql)

with DAG(
    dag_id="etl_benchmark_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["benchmark"]
) as dag:

    t_raw = PythonOperator(
        task_id="load_raw_benchmark",
        python_callable=load_raw_benchmark
    )

    t_dim = PythonOperator(
        task_id="load_dim_benchmark_group",
        python_callable=load_dim_benchmark_group
    )

    t_fact = PythonOperator(
        task_id="load_fact_benchmark",
        python_callable=load_fact_benchmark
    )

    t_raw >> t_dim >> t_fact
