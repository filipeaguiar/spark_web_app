import os
import uuid
import json
import subprocess
from string import Template
from fastapi import FastAPI, Request, UploadFile, File, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
import sqlglot
import sql_unviewer
import boto3

from . import models

# =============================================================================
#  Configuração da Aplicação e Estado
# =============================================================================

app = FastAPI(title="DataFlow Runner")
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

VALIDATED_QUERIES = {}

# =============================================================================
#  Template da DAG
# =============================================================================

DAG_TEMPLATE = """
import os
import pendulum
from airflow.decorators import dag, task
from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

@task
def get_minio_credentials(aws_conn_id: str) -> dict:
    hook = S3Hook(aws_conn_id=aws_conn_id)
    connection = hook.get_connection(aws_conn_id)
    endpoint_url = connection.extra_dejson.get('endpoint_url')
    session = hook.get_session()
    credentials = session.get_credentials()
    return {
        "MINIO_ENDPOINT_URL": endpoint_url,
        "MINIO_ACCESS_KEY": credentials.access_key,
        "MINIO_SECRET_KEY": credentials.secret_key,
    }

with DAG(
    dag_id="$dag_id",
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["generated", "spark_submit"],
) as dag:
    
    env_creds = get_minio_credentials(aws_conn_id="minio")

    spark_task = SparkSubmitOperator(
        task_id="run_spark_query",
        conn_id="Spark_Local",
        application="$spark_executor_path",
        application_args=[
            "--sql-file",
            "$sql_file_path",
            "--output-path",
            "$output_path",
        ],
        env_vars=env_creds,
    )
"""

# =============================================================================
#  Lógica de Negócio
# =============================================================================

def run_unviewer(sql_content: str) -> str:
    print("Executando sql_unviewer.unview...")
    return sql_unviewer.unview(sql_content)

def parse_sql_for_tables(sql: str) -> list[str]:
    try:
        parsed = sqlglot.parse(sql, read="postgres")
        return sorted({table.name.lower() for exp in parsed for table in exp.find_all(sqlglot.exp.Table)})
    except Exception as e:
        print(f"Erro no parse com sqlglot: {e}")
        return []

def check_tables_in_minio(tables: list[str]) -> tuple[list[str], list[str]]:
    print(f"Verificando tabelas no MinIO: {tables}")
    with open('../credentials.json') as f:
        creds = json.load(f)
    s3 = boto3.client('s3', endpoint_url='http://10.34.0.82:9000', aws_access_key_id=creds['accessKey'], aws_secret_access_key=creds['secretKey'])
    found, missing = [], []
    for table in tables:
        response = s3.list_objects_v2(Bucket='bronze', Prefix=f"aghu/{table}/", MaxKeys=1)
        if 'Contents' in response:
            found.append(table)
        else:
            missing.append(table)
    return found, missing

# =============================================================================
#  Endpoints da API
# =============================================================================

@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/upload-and-verify")
async def upload_and_verify(file: UploadFile = File(...)):
    try:
        sql_content = (await file.read()).decode('utf-8')
        expanded_sql = run_unviewer(sql_content)
        tables = parse_sql_for_tables(expanded_sql)
        
        if not tables:
            return JSONResponse(status_code=400, content={"message": "Nenhuma tabela encontrada na consulta.", "expanded_sql": expanded_sql, "tables_status": []})

        found, missing = check_tables_in_minio(tables)
        tables_status = [{"name": t, "found": t in found} for t in tables]

        if missing:
            return {"success": False, "message": f"Validação falhou. {len(missing)} tabela(s) não encontrada(s).", "tables_status": tables_status, "expanded_sql": expanded_sql}
        
        query_id = str(uuid.uuid4())
        base_filename = file.filename.lower().replace('.sql', '').replace(' ', '_').replace('.', '_')
        VALIDATED_QUERIES[query_id] = {"query": expanded_sql, "output_dir_name": base_filename}
        
        return {"success": True, "message": "Sucesso! Todas as tabelas foram encontradas.", "query_id": query_id, "tables_status": tables_status, "expanded_sql": expanded_sql}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/generate-dag")
async def generate_dag(query_id: str):
    query_data = VALIDATED_QUERIES.get(query_id)
    if not query_data:
        raise HTTPException(status_code=404, detail="ID da consulta inválido ou expirado.")

    try:
        base_filename = query_data["output_dir_name"]
        dag_id = f"spark_job_{base_filename}"
        
        spark_executor_path = "/home/adm-local/airflow/dags/scripts/spark_executor.py"
        airflow_dags_dir = "/home/adm-local/airflow/dags"
        airflow_sql_dir = os.path.join(airflow_dags_dir, "sql")
        
        sql_file_path = os.path.join(airflow_sql_dir, f"{base_filename}.sql")
        dag_file_path = os.path.join(airflow_dags_dir, f"dag_{base_filename}.py")
        output_path = f"s3a://silver/faturamento/{base_filename}/"

        os.makedirs(airflow_sql_dir, exist_ok=True)

        with open(sql_file_path, "w") as f:
            f.write(query_data["query"])
        subprocess.run(["chown", "adm-local:adm-local", sql_file_path], check=True)

        template = Template(DAG_TEMPLATE)
        dag_content = template.substitute(dag_id=dag_id, spark_executor_path=spark_executor_path, sql_file_path=sql_file_path, output_path=output_path)

        with open(dag_file_path, "w") as f:
            f.write(dag_content)
        subprocess.run(["chown", "adm-local:adm-local", dag_file_path], check=True)

        return {"message": f"DAG '{dag_id}' gerada com sucesso em {dag_file_path}"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))