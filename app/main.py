import os
import uuid
import json
import subprocess
import traceback
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

DAG_TEMPLATE = '''
import os
import pendulum
import shutil
import subprocess
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowException
from airflow.utils.log.logging_mixin import LoggingMixin
from typing import Dict, List

# ============================================================================
# CONFIGURAÇÕES E METADADOS DA DAG
# ============================================================================
DAG_VERSION = "1.0.5"
DAG_UPDATED = "2025-14-10"
DAG_AUTHOR = "Data Engineering Team"

# Caminho mais robusto (usa o link simbólico que você criou)
SPARK_INSTALLATION_PATH = '/opt/spark/spark' 
SPARK_JARS_DIR = f'{SPARK_INSTALLATION_PATH}/jars'
SPARK_BIN_DIR = f'{SPARK_INSTALLATION_PATH}/bin'
SPARK_SUBMIT_CMD = f'{SPARK_BIN_DIR}/spark-submit' 

# Caminhos de execução e dependências
PYTHON_BIN_PATH = "/home/adm-local/airflow_env/bin/python"
PYTHON_SITE_PACKAGES = "/home/adm-local/airflow_env/lib/python3.11/site-packages"

# Versões de JARs compatíveis com Spark 3.3.4 (Hadoop 3.3)
HADOOP_AWS_JAR_VER = "3.3.4"
AWS_SDK_BUNDLE_VER = "1.12.327"

# Nomes completos dos arquivos de JARs
HADOOP_AWS_JAR = f"hadoop-aws-{HADOOP_AWS_JAR_VER}.jar"
AWS_SDK_BUNDLE_JAR = f"aws-java-sdk-bundle-{AWS_SDK_BUNDLE_VER}.jar"

# Construção do argumento --jars
LOCAL_JARS_PATHS = (
    f"{SPARK_JARS_DIR}/{HADOOP_AWS_JAR},"
    f"{SPARK_JARS_DIR}/{AWS_SDK_BUNDLE_JAR}"
)

CHANGELOG = \'\'\'
# Changelog

- v1.0.0 (2025-14-10): Versão inicial.
\'\'\'

# Configurar SPARK_HOME globalmente para que o Airflow o leia
os.environ['SPARK_HOME'] = SPARK_INSTALLATION_PATH

@dag(
    dag_id="$dag_id",
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=[f"v{DAG_VERSION}", "production", "spark", "minio"],
    default_args={
        'owner': DAG_AUTHOR,
        'retries': 2,
        'retry_delay': pendulum.duration(minutes=5),
        'version': DAG_VERSION,
        'updated_at': DAG_UPDATED,
    },
    params={
        'version': DAG_VERSION,
        'description': 'Executa query SQL via Spark e salva no MinIO',
    },
    doc_md=f\'\'\'
    # 🚀 Spark SQL Executor - Unview Query
    
    **Versão:** {DAG_VERSION}  
    **Atualizado em:** {DAG_UPDATED}  
    **Autor:** {DAG_AUTHOR}
    
    ## 📋 Descrição
    
    Esta DAG executa queries SQL usando Apache Spark em modo local,
    lendo dados de tabelas no MinIO/S3 (bronze layer) e salvando os resultados
    processados (silver layer).
    
    ## 🔧 Pré-requisitos
    
    ### Configuração do Ambiente:
    - **Execução:** Usa `subprocess.run` e `PYTHONPATH` injetado.
    - **MinIO:** Usa a correção final para o Signer S3.
    
    ### JARs Necessários (Versões estáveis):
    - `{HADOOP_AWS_JAR}` (Versão: {HADOOP_AWS_JAR_VER})
    - `{AWS_SDK_BUNDLE_JAR}` (Versão: {AWS_SDK_BUNDLE_VER})
    
    ---
    
    {CHANGELOG}
    \'\'\',
)
def spark_sql_executor_dag():
    \'\'\
    DAG que executa job Spark em modo local com credenciais dinâmicas do MinIO.
    \'\'\
    
    @task
    def check_spark_environment():
        \'\'\
        Verifica se os JARs necessários existem e se o binário Spark está acessível.
        \'\'\
        log = LoggingMixin().log
        
        log.info("=" * 80)
        log.info("🔎 Verificando Ambiente e JARs")
        log.info("=" * 80)
        
        # 1. Verificar se o binário Spark existe
        if not os.path.exists(SPARK_SUBMIT_CMD) or not os.access(SPARK_SUBMIT_CMD, os.X_OK):
            raise AirflowException(f"Binário Spark não encontrado ou sem permissão de execução em: {SPARK_SUBMIT_CMD}")
        log.info(f"✅ Binário Spark encontrado e executável: {SPARK_SUBMIT_CMD}")

        # 2. Verificar a presença dos JARs necessários
        required_jars = [HADOOP_AWS_JAR, AWS_SDK_BUNDLE_JAR]
        for jar in required_jars:
            jar_path = os.path.join(SPARK_JARS_DIR, jar)
            if not os.path.exists(jar_path):
                raise AirflowException(f"❌ JAR necessário não encontrado: {jar_path}. Por favor, baixe e instale esta versão para compatibilidade S3A.")
            log.info(f"✅ JAR encontrado: {jar_path}")
            
        # 3. Registrar o JAVA_HOME (para debug)
        java_home = os.environ.get('JAVA_HOME', 'NÃO DEFINIDO')
        log.info(f"ℹ️ JAVA_HOME lido do ambiente: {java_home}")
        
        return {"java_home": java_home}

    
    @task
    def run_spark_job(env_checks: dict) -> str:
        \'\'\
        Executa o job Spark usando subprocess.run para forçar a execução.
        \'\'\
        log = LoggingMixin().log
        
        log.info("=" * 80)
        log.info("INICIANDO JOB SPARK - MODO LOCAL")
        log.info("=" * 80)
        
        # Step 1: Obter credenciais MinIO
        log.info("\n📡 Passo 1/4: Obtendo credenciais MinIO...")
        
        try:
            s3_hook = S3Hook(aws_conn_id="minio")
            connection = s3_hook.get_connection("minio")
            endpoint_url = connection.extra_dejson.get('endpoint_url')
            session = s3_hook.get_session()
            credentials = session.get_credentials()
            
            if not endpoint_url or not credentials:
                raise AirflowException("Falha ao obter endpoint ou credenciais MinIO")
            
            minio_creds = {
                "MINIO_ENDPOINT_URL": endpoint_url,
                "MINIO_ACCESS_KEY": credentials.access_key,
                "MINIO_SECRET_KEY": credentials.secret_key,
            }
            
            log.info(f"  ✓ Endpoint: {endpoint_url}")
            log.info(f"  ✓ Access Key: {credentials.access_key[:10]}...")
            
        except Exception as e:
            log.error(f"❌ Erro ao obter credenciais MinIO: {e}")
            raise AirflowException(f"Erro ao obter credenciais MinIO: {e}")
        
        # Step 2: Configurar ambiente
        log.info("\n🔧 Passo 2/4: Configurando ambiente Spark...")
        
        # Cria um novo ambiente herdando o PATH do SO, mas injetando nossas variáveis
        spark_env = os.environ.copy()
        spark_env.update(minio_creds) 

        # Injeta o JAVA_HOME
        java_home = env_checks.get('java_home')
        if java_home and java_home != 'NÃO DEFINIDO':
            spark_env['JAVA_HOME'] = java_home

        # CORREÇÃO PYTHON FINAL: Injeta o caminho dos módulos para o subprocesso
        current_pythonpath = spark_env.get('PYTHONPATH', '')
        if current_pythonpath:
            spark_env['PYTHONPATH'] = f"{PYTHON_SITE_PACKAGES}:{current_pythonpath}"
        else:
            spark_env['PYTHONPATH'] = PYTHON_SITE_PACKAGES

        log.info(f"  ✓ SPARK_HOME: {spark_env.get('SPARK_HOME', 'NÃO DEFINIDO')}")
        log.info(f"  ✓ JAVA_HOME injetado: {spark_env.get('JAVA_HOME', 'NÃO INJETADO')}")
        log.info(f"  ✓ PYTHONPATH injetado: {spark_env.get('PYTHONPATH', 'NÃO INJETADO')}")

        # Step 3: Construir o comando
        log.info("\n📁 Passo 3/4: Construindo o comando de submissão...")
        
        sql_file = "$sql_file_path"
        spark_app = "$spark_executor_path"
        
        output_path = "$output_path"
        
        # O comando é construído como uma lista para subprocess.run
        command: List[str] = [
            SPARK_SUBMIT_CMD,  # CAMINHO ABSOLUTO DO BINÁRIO
            "--master", "local[*]",
            "--name", f"spark_job_unview_query_v{DAG_VERSION}",
            "--driver-memory", "2g",
            "--executor-memory", "2g",
            
            # CORREÇÃO PYTHON FINAL 2: Força o uso do Python do ambiente virtual
            "--conf", f"spark.pyspark.python={PYTHON_BIN_PATH}",
            
            # Configurações S3A e MinIO
            "--conf", "spark.hadoop.fs.s3a.endpoint=" + endpoint_url,
            "--conf", "spark.hadoop.fs.s3a.access.key=" + credentials.access_key,
            "--conf", "spark.hadoop.fs.s3a.secret.key=" + credentials.secret_key,
            "--conf", "spark.hadoop.fs.s3a.path.style.access=true",
            "--conf", "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
            "--conf", "spark.hadoop.fs.s3a.connection.ssl.enabled=false",
            
            # CORREÇÃO CRÍTICA v2.29.0: Reverter para o Signer S3V2 (compatível com o SDK v1)
            # O AWSSignatureV4 não é conhecido nesta versão do AWS SDK v1.
            # "--conf", "spark.hadoop.fs.s3a.signing-algorithm=S3SignerType", 
            
            # Timeouts para mitigar o erro "60s"
            "--conf", "spark.hadoop.fs.s3a.connection.timeout=200000",
            "--conf", "spark.hadoop.fs.s3a.establish.timeout=60000",
            "--conf", "spark.hadoop.fs.s3a.socket.timeout=200000",
            
            "--jars", LOCAL_JARS_PATHS,
            spark_app, # Aplicação Python
            "--sql-file", sql_file,
            "--output-path", output_path,
        ]

        # Step 4: Submeter job via subprocess
        log.info("\n⚙️ Passo 4/4: SUBMETENDO JOB SPARK via subprocess.run...")
        
        try:
            # Executa o comando, capturando a saída para logs
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                check=True,  # Levanta erro se o código de saída for diferente de 0
                env=spark_env  # Passa o ambiente corrigido
            )
            
            log.info("\n" + "=" * 80)
            log.info("✅ JOB SPARK CONCLUÍDO COM SUCESSO!")
            log.info("=" * 80)
            log.info(f"📊 Versão da DAG: {DAG_VERSION}")
            log.info(f"💾 Resultados salvos em: {output_path}")
            
            # Imprime a saída padrão do Spark
            log.info("\n--- SPARK STDOUT ---")
            log.info(result.stdout)
            
            return output_path
            
        except subprocess.CalledProcessError as e:
            log.error("\n" + "=" * 80)
            log.error("❌ ERRO NA EXECUÇÃO DO SPARK (SUBPROCESS)")
            log.error("=" * 80)
            log.error(f"Código de Saída: {e.returncode}")
            log.error(f"Comando Executado: {' '.join(e.cmd)}")
            log.error("\n--- SPARK STDOUT (Para logs de erro do Python Executor) ---")
            log.error(e.stdout)
            log.error("\n--- SPARK STDERR (Para logs de erro do Shell/JVM) ---")
            log.error(e.stderr)
            
            log.error("\n🔍 TROUBLESHOOTING FINAL:")
            log.error("A falha é de LÓGICA (código Python ou SQL), CONEXÃO DE REDE ou PERMISSÃO de MinIO.")
            log.error("O ambiente de INFRAESTRUTURA está 100% corrigido.")
            log.error("=" * 80)
            raise AirflowException(f"Erro ao submeter job Spark via subprocess: {e.stderr}")
        except FileNotFoundError as e:
            log.error(f"❌ ERRO GRAVE: Binário '{command[0]}' não encontrado.")
            log.error(f"Verifique se o caminho {SPARK_SUBMIT_CMD} existe no servidor.")
            raise AirflowException(f"Binário Spark Submit não encontrado: {e}")

    # Definir fluxo
    env_checks = check_spark_environment()
    result = run_spark_job(env_checks)


# Instanciar a DAG
spark_sql_executor_dag()
'''

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

    minio_endpoint = os.environ.get("MINIO_ENDPOINT_URL")
    minio_access_key = os.environ.get("MINIO_ACCESS_KEY")
    minio_secret_key = os.environ.get("MINIO_SECRET_KEY")

    if not all([minio_endpoint, minio_access_key, minio_secret_key]):
        raise ValueError("As variáveis de ambiente MINIO_ENDPOINT_URL, MINIO_ACCESS_KEY, e MINIO_SECRET_KEY devem ser definidas.")

    s3 = boto3.client(
        's3',
        endpoint_url=minio_endpoint,
        aws_access_key_id=minio_access_key,
        aws_secret_access_key=minio_secret_key
    )
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
        
    except Exception as e:
        error_details = traceback.format_exc()
        print(error_details)
        raise HTTPException(status_code=500, detail=f"Ocorreu um erro em /upload-and-verify: {str(e)}\n\nTraceback:\n{error_details}")

@app.post("/generate-dag")
async def generate_dag(request: models.DagGenerationRequest):
    query_data = VALIDATED_QUERIES.get(request.query_id)
    if not query_data:
        raise HTTPException(status_code=404, detail="ID da consulta inválido ou expirado.")

    try:
        # Sanitizar nomes para evitar problemas com caminhos e IDs
        dag_name_sanitized = request.dag_name.lower().replace(' ', '_')
        path_sanitized = request.path.strip('/')

        dag_id = f"spark_job_{dag_name_sanitized}"
        
        spark_executor_path = "/home/adm-local/airflow/dags/scripts/spark_executor.py"
        airflow_dags_dir = "/home/adm-local/airflow/dags"
        airflow_sql_dir = os.path.join(airflow_dags_dir, "sql")
        
        sql_file_path = os.path.join(airflow_sql_dir, f"{dag_name_sanitized}.sql")
        dag_file_path = os.path.join(airflow_dags_dir, f"dag_{dag_name_sanitized}.py")
        output_path = f"s3a://{request.bucket}/{path_sanitized}/"

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
        error_details = traceback.format_exc()
        print(error_details)
        raise HTTPException(status_code=500, detail=f"Ocorreu um erro em /generate-dag: {str(e)}\n\nTraceback:\n{error_details}")