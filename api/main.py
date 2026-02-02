from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
import os

app = FastAPI(
    title="Data Mart Employee API",
    description="Public API for Data Mart",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_conn():
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "postgres"),
        dbname=os.getenv("PG_DB", "dwh"),
        user=os.getenv("PG_USER", "postgres"),
        password=os.getenv("PG_PASSWORD", "postgres"),
        port=int(os.getenv("PG_PORT", "5432"))
    )

def fetch_table(query: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    cols = [desc[0] for desc in cur.description]
    cur.close()
    conn.close()
    return [dict(zip(cols, row)) for row in rows]

@app.get("/ping")
def ping():
    return {"message": "pong"}

@app.get("/health")
def health_check():
    try:
        conn = get_conn()
        conn.close()
        return {"status": "ok", "database": "connected"}
    except:
        return {"status": "error", "database": "connection_failed"}

@app.get("/export/all")
def export_all():
    dim_tables = [
        "employee", "department", "division", "classification",
        "competency_type", "title", "job_nature", "benchmark_group", "metric_type"
    ]
    fact_tables = [
        "competency", "competency_benchmark"
    ]

    result = {}
    for tbl in dim_tables:
        result[f"dim_{tbl}"] = fetch_table(f"SELECT * FROM dwh.dim_{tbl}")
    for tbl in fact_tables:
        result[f"fact_{tbl}"] = fetch_table(f"SELECT * FROM dwh.fact_{tbl}")
    return result

@app.get("/api/{table_type}/{table_name}")
def get_table_data(table_type: str, table_name: str):
    if table_type not in ["dim", "fact"]:
        raise HTTPException(status_code=400, detail="table_type must be 'dim' or 'fact'")
    
    full_table_name = f"dwh.{table_type}_{table_name}"
    try:
        return fetch_table(f"SELECT * FROM {full_table_name}")
    except Exception:
        raise HTTPException(status_code=404, detail=f"Cannot fetch table: {full_table_name}")
