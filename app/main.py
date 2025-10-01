import uuid
import json
import boto3
import sqlglot
import sql_unviewer
import os
import subprocess
from fastapi import FastAPI, Request, UploadFile, File, BackgroundTasks, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from pyspark.sql import SparkSession

from . import models

# =============================================================================
#  Configuração da Aplicação e Estado em Memória
# =============================================================================

app = FastAPI(title="SQL Runner App")

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

VALIDATED_QUERIES = {}
JOB_STATUSES = {}

# =============================================================================
#  Templates
# =============================================================================

DAG_TEMPLATE = """\
from __future__ import annotations

import pendulum

from airflow.decorators import dag, task
from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

@task
def get_minio_credentials(aws_conn_id: str) -> dict:
    \"""\
    Busca as credenciais de uma conexão S3/MinIO do Airflow e as retorna
    como um dicionário de variáveis de ambiente.
    \"""\
    hook = S3Hook(aws_conn_id=aws_conn_id)
    session = hook.get_session()
    credentials = session.get_credentials()
    
    # O endpoint_url geralmente está no campo 'extra' da conexão
    endpoint_url = hook.conn.extra_dejson.get('endpoint_url')

    return {
        "MINIO_ENDPOINT_URL": endpoint_url,
        "MINIO_ACCESS_KEY": credentials.access_key,
        "MINIO_SECRET_KEY": credentials.secret_key,
    }

with DAG(
    dag_id="{dag_id}",
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["generated", "spark_submit"],
) as dag:
    
    # Tarefa para obter as credenciais da conexão 'minio'
    env_vars_credentials = get_minio_credentials(aws_conn_id="minio")

    spark_task = SparkSubmitOperator(
        task_id="run_spark_query",
        conn_id="Spark_Local",
        application="{spark_executor_path}",
        application_args=[
            "--sql-file",
            "{sql_file_path}",
            "--output-path",
            "{output_path}",
        ],
        # Injeta as credenciais como variáveis de ambiente para o script Spark
        env_vars=env_vars_credentials,
    )
"""

# =============================================================================
#  Lógica de Negócio com Spark
# =============================================================================

def run_unviewer(sql_content: str) -> str:
    """Executa o sql-unviewer no conteúdo SQL fornecido."""
    print("Executando sql_unviewer.unview...")
    try:
        expanded_sql = sql_unviewer.unview(sql_content)
        print("SQL expandido com sucesso.")
        return expanded_sql
    except Exception as e:
        print(f"Erro durante a execução do sql_unviewer: {e}")
        raise ValueError(f"Falha ao expandir o SQL: {e}")

def parse_sql_for_tables(sql: str) -> list[str]:
    """Usa sqlglot para extrair nomes de tabelas de uma consulta SQL."""
    try:
        parsed = sqlglot.parse(sql, read="spark")
        tables = {table.name.lower() for expression in parsed for table in expression.find_all(sqlglot.exp.Table)}
        unique_tables = sorted(list(tables))
        print(f"Tabelas encontradas (com sqlglot): {unique_tables}")
        return unique_tables
    except Exception as e:
        print(f"Erro ao fazer o parse do SQL com sqlglot: {e}")
        return []

def check_tables_in_minio(tables: list[str]) -> tuple[list[str], list[str]]:
    """Verifica a existência de arquivos Parquet para as tabelas no MinIO usando boto3."""
    print(f"Verificando tabelas no MinIO com boto3: {tables}")
    
    with open('credentials.json') as f:
        credentials = json.load(f)

    s3_client = boto3.client(
        's3',
        endpoint_url=f"http://10.34.0.82:9000",
        aws_access_key_id=credentials['accessKey'],
        aws_secret_access_key=credentials['secretKey']
    )
    
    found_tables = []
    missing_tables = []

    for table_name in tables:
        prefix = f"aghu/{table_name}/"
        try:
            response = s3_client.list_objects_v2(Bucket='bronze', Prefix=prefix, MaxKeys=1)
            if 'Contents' in response and len(response['Contents']) > 0:
                found_tables.append(table_name)
                print(f"  - Tabela '{table_name}' ENCONTRADA.")
            else:
                missing_tables.append(table_name)
                print(f"  - Tabela '{table_name}' NÃO ENCONTRADA (nenhum objeto no prefixo).")
        except Exception as e:
            missing_tables.append(table_name)
            print(f"  - Tabela '{table_name}' NÃO ENCONTRADA (Erro: {e}).")
            
    print(f"Verificação concluída. Encontradas: {found_tables}, Ausentes: {missing_tables}")
    return found_tables, missing_tables

def execute_spark_job(query_id: str, query: str, tables: list[str], output_dir_name: str):
    """Executa a consulta SQL com PySpark em segundo plano."""
    JOB_STATUSES[query_id] = {"status": "running", "message": "Iniciando job Spark..."}
    print(f"\n--- INICIANDO JOB SPARK PARA CONSULTA {query_id} ---")
    output_path = f"s3a://silver/faturamento/{output_dir_name}/result"
    print(f"Caminho de saída: {output_path}")

    spark = None
    try:
        with open('credentials.json') as f:
            credentials = json.load(f)

        JOB_STATUSES[query_id]["message"] = "Configurando e iniciando a sessão Spark..."
        spark = (
            SparkSession.builder.appName(f"QueryRunner-{query_id}")
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
            .config("spark.hadoop.fs.s3a.endpoint", "http://10.34.0.82:9000")
            .config("spark.hadoop.fs.s3a.access.key", credentials['accessKey'])
            .config("spark.hadoop.fs.s3a.secret.key", credentials['secretKey'])
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60000")
            .config("spark.hadoop.fs.s3a.connection.acquisition.timeout", "60000")
            .config("spark.hadoop.fs.s3a.connection.idle.time", "60000")
            .config("spark.hadoop.fs.s3a.connection.request.timeout", "60000")
            .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
            .config("spark.hadoop.fs.s3a.connection.establish.timeout", "30000")
            .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400000")
            .getOrCreate()
        )
        JOB_STATUSES[query_id]["message"] = "Criando views temporárias para as tabelas..."
        for table_name in tables:
            path = f"s3a://bronze/aghu/{table_name}/"
            df = spark.read.parquet(path)
            df.createOrReplaceTempView(table_name)
            print(f"View '{table_name}' criada a partir de {path}")

        JOB_STATUSES[query_id]["message"] = "Executando consulta principal..."
        spark_sql = sqlglot.transpile(query, read="postgres", write="spark", normalize=True)[0]
        query_cleaned = spark_sql.replace("agh.", "").strip().rstrip(';')
        
        result_df = spark.sql(query_cleaned)

        JOB_STATUSES[query_id]["message"] = "Salvando resultado no MinIO..."
        result_df.write.mode("overwrite").parquet(output_path)
        
        success_message = f"SUCESSO! Resultado salvo em {output_path}"
        print(success_message)
        JOB_STATUSES[query_id] = {"status": "success", "message": success_message}

    except Exception as e:
        error_message = f"ERRO ao executar o job Spark: {e}"
        print(error_message)
        JOB_STATUSES[query_id] = {"status": "failed", "message": str(e)}
    finally:
        if spark:
            spark.stop()
        print(f"--- JOB SPARK {query_id} FINALIZADO ---")

# =============================================================================
#  Endpoints da API
# =============================================================================

@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/upload-and-verify", response_model=models.VerificationResult)
async def upload_and_verify(file: UploadFile = File(...)):
    try:
        if not file.filename.endswith('.sql'):
            return models.VerificationResult(success=False, message="Por favor, envie um arquivo .sql")

        sql_content = (await file.read()).decode('utf-8')
        expanded_sql = run_unviewer(sql_content)
        tables = parse_sql_for_tables(expanded_sql)
        
        if not tables:
            return models.VerificationResult(success=False, message="Nenhuma tabela encontrada na consulta.", expanded_sql=expanded_sql)

        found, missing = check_tables_in_minio(tables)
        
        tables_status = [models.TableStatus(name=t, found=t in found) for t in tables]

        if missing:
            msg = f"Validação falhou. {len(missing)} tabela(s) não encontrada(s)."
            return models.VerificationResult(success=False, message=msg, tables_status=tables_status, expanded_sql=expanded_sql)
        
        query_id = str(uuid.uuid4())
        base_filename = file.filename.lower().replace('.sql', '').replace(' ', '_').replace('.', '_')
        VALIDATED_QUERIES[query_id] = {
            "query": expanded_sql,
            "tables": found,
            "output_dir_name": base_filename
        }
        
        msg = "Sucesso! Todas as tabelas foram encontradas. Pronto para executar."
        return models.VerificationResult(success=True, message=msg, query_id=query_id, tables_status=tables_status, expanded_sql=expanded_sql)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ocorreu um erro interno: {e}")

@app.post("/execute-job", response_model=models.JobSubmissionResponse)
async def execute_job(query_id: str, background_tasks: BackgroundTasks):
    query_data = VALIDATED_QUERIES.get(query_id)
    if not query_data:
        raise HTTPException(status_code=404, detail="ID da consulta inválido ou expirado.")

    background_tasks.add_task(
        execute_spark_job,
        query_id=query_id,
        query=query_data["query"],
        tables=query_data["tables"],
        output_dir_name=query_data["output_dir_name"]
    )
    
    return models.JobSubmissionResponse(
        message="Job Spark iniciado em segundo plano.",
        query_id=query_id
    )

@app.post("/generate-dag")
async def generate_dag(query_id: str):
    query_data = VALIDATED_QUERIES.get(query_id)
    if not query_data:
        raise HTTPException(status_code=404, detail="ID da consulta inválido ou expirado.")

    try:
        # --- Definir Caminhos ---
        base_filename = query_data["output_dir_name"]
        dag_id = f"spark_job_{base_filename}"
        
        # Caminho absoluto para o diretório do projeto
        project_root = "/home/filipe/Documentos/Projetos/spark/spark_web_app"
        spark_executor_path = os.path.join(project_root, "spark_executor.py")

        # Caminhos no ambiente Airflow
        airflow_dags_dir = "/home/adm-local/airflow/dags"
        airflow_sql_dir = os.path.join(airflow_dags_dir, "sql")
        
        sql_file_path = os.path.join(airflow_sql_dir, f"{base_filename}.sql")
        dag_file_path = os.path.join(airflow_dags_dir, f"dag_{base_filename}.py")
        output_path = f"s3a://silver/faturamento/{base_filename}/result"

        # --- Criar Diretórios e Arquivos ---
        os.makedirs(airflow_sql_dir, exist_ok=True)

        # Salvar arquivo SQL
        print(f"Salvando SQL em: {sql_file_path}")
        with open(sql_file_path, "w", encoding="utf-8") as f:
            f.write(query_data["query"])
        
        # Mudar proprietário do arquivo SQL
        print(f"Alterando proprietário de {sql_file_path} para adm-local:adm-local")
        subprocess.run(["chown", "adm-local:adm-local", sql_file_path], check=True)

        # Gerar conteúdo da DAG
        dag_content = DAG_TEMPLATE.format(
            dag_id=dag_id,
            spark_executor_path=spark_executor_path,
            sql_file_path=sql_file_path,
            output_path=output_path,
        )

        # Salvar arquivo da DAG
        print(f"Salvando DAG em: {dag_file_path}")
        with open(dag_file_path, "w", encoding="utf-8") as f:
            f.write(dag_content)

        # Mudar proprietário do arquivo da DAG
        print(f"Alterando proprietário de {dag_file_path} para adm-local:adm-local")
        subprocess.run(["chown", "adm-local:adm-local", dag_file_path], check=True)

        return JSONResponse(
            status_code=200,
            content={"message": f"DAG '{dag_id}' gerada com sucesso em {dag_file_path}"}
        )

    except FileNotFoundError:
        raise HTTPException(status_code=500, detail="Erro: Diretório de DAGs do Airflow não encontrado. Verifique o caminho.")
    except subprocess.CalledProcessError as e:
        raise HTTPException(status_code=500, detail=f"Erro ao alterar permissões do arquivo: {e}. Verifique se a aplicação tem permissão para executar 'chown'.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ocorreu um erro inesperado ao gerar a DAG: {e}")

@app.get("/job-status/{query_id}", response_model=models.JobStatus)
async def get_job_status(query_id: str):
    status = JOB_STATUSES.get(query_id)
    if not status:
        return models.JobStatus(status="pending", message="Job na fila, aguardando para iniciar...")
    return models.JobStatus(**status)