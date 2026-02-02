CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS dwh;

CREATE TABLE IF NOT EXISTS raw.employee (
    id BIGINT,
    year smallint,
    code TEXT,
    full_name TEXT,
    title TEXT,
    department TEXT,
    division TEXT,

    communication REAL,
    communication_req REAL,

    continuous_learning REAL,
    continuous_learning_req REAL,

    creative_thinking REAL,
    creative_thinking_req REAL,

    critical_thinking REAL,
    critical_thinking_req REAL,

    data_analysis REAL,
    data_analysis_req REAL,

    digital_literacy REAL,
    digital_literacy_req REAL,

    problem_solving REAL,
    problem_solving_req REAL,

    resilience REAL,
    resilience_req REAL,

    strategic_thinking REAL,
    strategic_thinking_req REAL,

    talent_management REAL,
    talent_management_req REAL,

    teamwork_leadership REAL,
    teamwork_leadership_req REAL,

    ai_bigdata REAL,
    ai_bigdata_req REAL,

    analytical_thinking REAL,
    analytical_thinking_req REAL,

    classification_core TEXT,
    classification_new TEXT,
    created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw.benchmark (
    job_nature TEXT,
    group_function TEXT,
    division TEXT,
    department TEXT,
    title TEXT,
    communication REAL,
    digital_literacy REAL,
    data_analysis REAL,
    problem_solving REAL,
    critical_thinking REAL,
    continuous_learning REAL,
    teamwork_leadership REAL,
    strategic_thinking REAL,
    talent_management REAL,
    creative_thinking REAL,
    resilience REAL,
    ai_bigdata REAL,
    analytical_thinking REAL
);


-- DIMENSION TABLES
CREATE TABLE IF NOT EXISTS dwh.dim_department (
    department_key SERIAL PRIMARY KEY,
    department_name TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS dwh.dim_title (
    title_key SERIAL PRIMARY KEY, 
    title TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS dwh.dim_division (
    division_key SERIAL PRIMARY KEY,
    division_name TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS dwh.dim_employee (
    employee_key SERIAL PRIMARY KEY,
    employee_id TEXT, 
    full_name TEXT,
    title_key INT REFERENCES dwh.dim_title(title_key),
    department_key INT REFERENCES dwh.dim_department(department_key),
    division_key INT REFERENCES dwh.dim_division(division_key)
);

CREATE TABLE IF NOT EXISTS dwh.dim_classification (
    classification_key SERIAL PRIMARY KEY,
    classification_value TEXT UNIQUE
);

INSERT INTO dwh.dim_classification (classification_key, classification_value) VALUES
(1, 'High'),
(2, 'Medium'),
(3, 'Low')
ON CONFLICT (classification_key) DO NOTHING;

CREATE TABLE IF NOT EXISTS dwh.dim_job_nature (
    job_nature_key SERIAL primary key,
    job_nature text,
    group_function text UNIQUE
);

INSERT INTO dwh.dim_job_nature(job_nature,group_function) values
('Front-Office','Business & Client Group'),
('Middle-Office','Analytics & Control Group'),
('Back-Office','Operations & Transaction Support'),
('Back-Office','Technology & Digital Systems'),
('Back-Office','Internal Operations & Governance'),
('Back-Office','Financial Management')
ON CONFLICT (group_function) DO NOTHING;


CREATE TABLE IF NOT EXISTS dwh.dim_competency_type (
    competency_key SERIAL PRIMARY KEY,
    competency_name TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS dwh.dim_metric_type (
    metric_type_key SERIAL PRIMARY KEY,
    metric_type_name TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS dwh.dim_benchmark_group (
    benchmark_group_key SERIAL PRIMARY KEY,
    job_nature_key INT REFERENCES dwh.dim_job_nature(job_nature_key),
    division_key INT REFERENCES dwh.dim_division(division_key),
    department_key INT REFERENCES dwh.dim_department(department_key),
    title_key INT REFERENCES dwh.dim_title(title_key),
    UNIQUE (job_nature_key, division_key, department_key, title_key)
);

CREATE TABLE IF NOT EXISTS dwh.fact_competency_benchmark (
    benchmark_key SERIAL PRIMARY KEY,
    benchmark_group_key INT REFERENCES dwh.dim_benchmark_group(benchmark_group_key),
    competency_key INT REFERENCES dwh.dim_competency_type(competency_key),
    benchmark_value REAL
);

CREATE TABLE IF NOT EXISTS dwh.fact_competency (
    fact_id BIGSERIAL PRIMARY KEY,

    employee_key INT REFERENCES dwh.dim_employee(employee_key),

    competency_key INT REFERENCES dwh.dim_competency_type(competency_key),
    metric_type_key INT REFERENCES dwh.dim_metric_type(metric_type_key),

    value REAL, 

    created_at TIMESTAMP,
    year smallint,

    classification_core_key INT REFERENCES dwh.dim_classification(classification_key),
    classification_new_key INT REFERENCES dwh.dim_classification(classification_key)
);