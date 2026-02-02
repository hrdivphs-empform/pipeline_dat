### etl_employee_pipeline.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import requests

SOURCE_API = "https://emp-form-project.onrender.com/api/employees"

def extract_api():
    response = requests.get(SOURCE_API)
    response.raise_for_status()
    data = response.json()

    pg = PostgresHook(postgres_conn_id="postgres_dwh")
    conn = pg.get_conn()
    cur = conn.cursor()

    cur.execute("TRUNCATE raw.employee;")

    insert_sql = """
        INSERT INTO raw.employee (
            id, year, code, full_name, title,
            department, division,
            communication, communication_req,
            continuous_learning, continuous_learning_req,
            creative_thinking, creative_thinking_req,
            critical_thinking, critical_thinking_req,
            data_analysis, data_analysis_req,
            digital_literacy, digital_literacy_req,
            problem_solving, problem_solving_req,
            resilience, resilience_req,
            strategic_thinking, strategic_thinking_req,
            talent_management, talent_management_req,
            teamwork_leadership, teamwork_leadership_req,
            ai_bigdata, ai_bigdata_req,
            analytical_thinking, analytical_thinking_req,
            classification_core, classification_new, created_at
        )
        VALUES (
            %(id)s, %(year)s, %(code)s, %(full_name)s, %(title)s,
            %(department)s, %(division)s,
            %(communication)s, %(communication_req)s,
            %(continuous_learning)s, %(continuous_learning_req)s,
            %(creative_thinking)s, %(creative_thinking_req)s,
            %(critical_thinking)s, %(critical_thinking_req)s,
            %(data_analysis)s, %(data_analysis_req)s,
            %(digital_literacy)s, %(digital_literacy_req)s,
            %(problem_solving)s, %(problem_solving_req)s,
            %(resilience)s, %(resilience_req)s,
            %(strategic_thinking)s, %(strategic_thinking_req)s,
            %(talent_management)s, %(talent_management_req)s,
            %(teamwork_leadership)s, %(teamwork_leadership_req)s,
            %(ai_bigdata)s, %(ai_bigdata_req)s,
            %(analytical_thinking)s, %(analytical_thinking_req)s,
            %(classification_core)s, %(classification_new)s, %(created_at)s
        )
    """

    for row in data:
        cur.execute(insert_sql, row)

    conn.commit()
    cur.close()
    conn.close()

def exec_sql(sql: str):
    pg = PostgresHook(postgres_conn_id="postgres_dwh")
    conn = pg.get_conn()
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()

def load_dim_tables():
    exec_sql("TRUNCATE dwh.dim_department RESTART IDENTITY CASCADE;")
    exec_sql("""
        INSERT INTO dwh.dim_department (department_name)
        VALUES
        ('No Department'),
        ('Customer Care Department'),
        ('Business Analysis (Function)'),
        ('Workplace (Receptionist)'),
        ('Consultant (Function)'),
        ('Brokerage Management Department'),
        ('Account Management Department'),
        ('Institutional Business Department'),
        ('Margin & Settlement (Financial Product)'),
        ('Margin & Settlement (Operation)'),
        ('Network Management Department'),
        ('Software Department'),
        ('System Department'),
        ('Data Engineer (Function)'),
        ('Talent Acquisition Department'),
        ('Total Rewards Department'),
        ('Workplace (Purchasing)'),
        ('Workplace (General)'),
        ('Learning & Development Department'),
        ('Employee Engagement & Employer Branding Department'),
        ('Settlement & Margin Department'),
        ('Depository Trading Department'),
        ('Business Support Department'),
        ('Operation (Function)'),
        ('Customer Care Center & Business Support Department');
    """)

    exec_sql("TRUNCATE dwh.dim_division RESTART IDENTITY CASCADE;")
    exec_sql("""
        INSERT INTO dwh.dim_division (division_name)
        VALUES
        ('Office Assistant Division'),
        ('Company Secretariat Division'),
        ('Product Department'),
        ('Marketing Division'),
        ('Finance Division'),
        ('Accounting Division'),
        ('Risk Management Division'),
        ('Securities Service Division'),
        ('Internal Control Division'),
        ('Dealing Division'),
        ('Advisory Division'),
        ('Research Division'),
        ('Legal Affairs Division'),
        ('IT Division'),
        ('People & Workplace Division'),
        ('Brokerage Division'),
        ('Covered Warrant Division'),
        ('Internal Audit Division');
    """)

    exec_sql("TRUNCATE dwh.dim_title RESTART IDENTITY CASCADE;")
    exec_sql("""
        INSERT INTO dwh.dim_title (title)
        VALUES
        ('Director'),
        ('Deputy Director'),
        ('Manager'),
        ('Deputy Manager'),
        ('Supervisor'),
        ('Senior'),
        ('Officer');
    """)

    exec_sql("TRUNCATE dwh.dim_employee RESTART IDENTITY CASCADE;")
    exec_sql("""
        INSERT INTO dwh.dim_employee (
            employee_id, full_name, title_key,
            department_key, division_key
        )
        SELECT DISTINCT
            e.code,
            e.full_name,
            t.title_key,
            d.department_key,
            v.division_key
        FROM raw.employee e
        LEFT JOIN dwh.dim_department d ON e.department = d.department_name
        LEFT JOIN dwh.dim_division v ON e.division = v.division_name
        LEFT JOIN dwh.dim_title t ON e.title = t.title;
    """)

    exec_sql("TRUNCATE dwh.dim_competency_type RESTART IDENTITY CASCADE;")
    exec_sql("""
        INSERT INTO dwh.dim_competency_type (competency_name)
        VALUES
        ('communication'),
        ('continuous_learning'),
        ('creative_thinking'),
        ('critical_thinking'),
        ('data_analysis'),
        ('digital_literacy'),
        ('problem_solving'),
        ('resilience'),
        ('strategic_thinking'),
        ('talent_management'),
        ('teamwork_leadership'),
        ('ai_bigdata'),
        ('analytical_thinking');
    """)

    exec_sql("TRUNCATE dwh.dim_metric_type RESTART IDENTITY CASCADE;")
    exec_sql("INSERT INTO dwh.dim_metric_type (metric_type_name) VALUES ('actual'), ('required');")

def load_fact_competency():
    exec_sql("TRUNCATE dwh.fact_competency RESTART IDENTITY CASCADE;")

    fact_sql = """
    WITH wide AS (
        SELECT 
            de.employee_key,
            cls_core.classification_key AS classification_core_key,
            cls_new.classification_key AS classification_new_key,
            e.created_at,
            e.year,

            ARRAY[
                'communication','continuous_learning','creative_thinking',
                'critical_thinking','data_analysis','digital_literacy',
                'problem_solving','resilience','strategic_thinking',
                'talent_management','teamwork_leadership','ai_bigdata',
                'analytical_thinking'
            ] AS competency_names,

            ARRAY[
                e.communication, e.continuous_learning, e.creative_thinking,
                e.critical_thinking, e.data_analysis, e.digital_literacy,
                e.problem_solving, e.resilience, e.strategic_thinking,
                e.talent_management, e.teamwork_leadership, e.ai_bigdata,
                e.analytical_thinking
            ] AS actual_values,

            ARRAY[
                e.communication_req, e.continuous_learning_req, e.creative_thinking_req,
                e.critical_thinking_req, e.data_analysis_req, e.digital_literacy_req,
                e.problem_solving_req, e.resilience_req, e.strategic_thinking_req,
                e.talent_management_req, e.teamwork_leadership_req, e.ai_bigdata_req,
                e.analytical_thinking_req
            ] AS required_values

        FROM raw.employee e
        JOIN dwh.dim_employee de 
            ON e.code = de.employee_id
        LEFT JOIN dwh.dim_classification cls_core 
            ON e.classification_core = cls_core.classification_value
        LEFT JOIN dwh.dim_classification cls_new 
            ON e.classification_new = cls_new.classification_value
    ),

    unpivot AS (
        SELECT
            w.employee_key,
            w.classification_core_key,
            w.classification_new_key,
            w.created_at,
            w.year,
            u.comp_name AS competency_name,
            u.metric_type AS metric_type_name,
            u.val AS value
        FROM wide w,
        LATERAL (
            SELECT 
                w.competency_names[i] AS comp_name,
                'actual' AS metric_type,
                w.actual_values[i] AS val
            FROM generate_subscripts(w.competency_names, 1) g(i)

            UNION ALL

            SELECT 
                w.competency_names[i] AS comp_name,
                'required' AS metric_type,
                w.required_values[i] AS val
            FROM generate_subscripts(w.competency_names, 1) g(i)
        ) u
    )

    INSERT INTO dwh.fact_competency (
        employee_key,
        competency_key,
        metric_type_key,
        value,
        created_at,
        year,
        classification_core_key,
        classification_new_key
    )
    SELECT 
        u.employee_key,
        dc.competency_key,
        mt.metric_type_key,
        u.value,
        u.created_at,
        CAST(u.year AS SMALLINT),
        u.classification_core_key,
        u.classification_new_key
    FROM unpivot u
    JOIN dwh.dim_competency_type dc
        ON LOWER(dc.competency_name) = LOWER(u.competency_name)
    JOIN dwh.dim_metric_type mt
        ON LOWER(mt.metric_type_name) = LOWER(u.metric_type_name);
    """

    exec_sql(fact_sql)

with DAG(
    dag_id="etl_employee_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["etl", "competency"]
) as dag:

    t_extract = PythonOperator(
        task_id="extract_api",
        python_callable=extract_api
    )

    with TaskGroup("load_dims") as tg_dims:
        t_dims = PythonOperator(
            task_id="load_dim_tables",
            python_callable=load_dim_tables
        )

    with TaskGroup("load_fact") as tg_fact:
        t_fact = PythonOperator(
            task_id="load_fact_competency",
            python_callable=load_fact_competency
        )

    t_extract >> tg_dims >> tg_fact
