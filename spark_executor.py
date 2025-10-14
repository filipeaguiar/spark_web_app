import os
import argparse
import sqlglot
from pyspark.sql import SparkSession

def parse_sql_for_tables(sql: str) -> list[str]:
    """Usa sqlglot para extrair nomes de tabelas de uma consulta SQL."""
    try:
        parsed = sqlglot.parse(sql, read="spark")
        tables = {table.name.lower() for expression in parsed for table in expression.find_all(sqlglot.exp.Table)}
        unique_tables = sorted(list(tables))
        print(f"Tabelas encontradas na consulta: {unique_tables}")
        return unique_tables
    except Exception as e:
        print(f"Erro ao fazer o parse do SQL com sqlglot: {e}")
        raise

def main():
    """
    Script principal para executar uma consulta Spark a partir de um arquivo SQL.
    """
    parser = argparse.ArgumentParser(description="Spark SQL Executor")
    parser.add_argument("--sql-file", required=True, help="Caminho para o arquivo .sql contendo a consulta a ser executada.")
    parser.add_argument("--output-path", required=True, help="Caminho de saída no S3 (ex: s3a://silver/faturamento/nome_da_consulta/).")
    args = parser.parse_args()

    print(f"Iniciando job Spark para o arquivo: {args.sql_file}")
    print(f"Caminho de saída: {args.output_path}")

    minio_endpoint = os.environ.get("MINIO_ENDPOINT_URL")
    minio_access_key = os.environ.get("MINIO_ACCESS_KEY")
    minio_secret_key = os.environ.get("MINIO_SECRET_KEY")

    if not all([minio_endpoint, minio_access_key, minio_secret_key]):
        raise ValueError("As variáveis de ambiente MINIO_ENDPOINT_URL, MINIO_ACCESS_KEY, e MINIO_SECRET_KEY devem ser definidas.")

    spark = None
    try:
        print("Configurando a sessão Spark...")
        spark = (
            SparkSession.builder.appName(f"SparkExecutor-{os.path.basename(args.sql_file)}")
            .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
            .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
            .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            .getOrCreate()
        )
        print("Sessão Spark iniciada com sucesso.")

        with open(args.sql_file, 'r', encoding='utf-8') as f:
            sql_query = f.read()

        tables = parse_sql_for_tables(sql_query)
        if not tables:
            raise ValueError("Nenhuma tabela encontrada no arquivo SQL.")

        print("Criando views temporárias para as tabelas...")
        for table_name in tables:
            table_path = f"s3a://bronze/aghu/{table_name}/"
            print(f"Lendo tabela '{table_name}' de '{table_path}'...")
            df = spark.read.parquet(table_path)
            df.createOrReplaceTempView(table_name)
            print(f"View temporária '{table_name}' criada.")

        print("Executando a consulta principal...")
        spark_sql = sqlglot.transpile(sql_query, read="postgres", write="spark", normalize=True)[0]
        query_cleaned = spark_sql.replace("agh.", "").strip().rstrip(';')
        result_df = spark.sql(query_cleaned)

        print(f"Salvando resultado em '{args.output_path}'...")
        result_df.write.mode("overwrite").parquet(args.output_path)

        print("Job Spark concluído com SUCESSO!")

    except Exception as e:
        print(f"ERRO durante a execução do job Spark: {e}")
        raise
    finally:
        if spark:
            print("Finalizando a sessão Spark.")
            spark.stop()

if __name__ == "__main__":
    main()