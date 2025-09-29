# Aplicação Web para Execução de SQL com Spark

Esta é uma aplicação web desenvolvida com FastAPI que permite aos usuários fazer o upload de um arquivo `.sql`, validar as tabelas consultadas contra um data lake no MinIO e executar a consulta em um ambiente PySpark local.

## Funcionalidades

- **Upload de SQL**: Interface para upload de arquivos `.sql`.
- **Expansão de Views**: Utiliza a biblioteca `sql-unviewer` para expandir as views contidas no SQL.
- **Validação de Tabelas**: Verifica se todas as tabelas existem como arquivos Parquet na camada `bronze` do MinIO.
- **Tradução de Dialeto**: Usa `sqlglot` para traduzir o SQL (originalmente em dialeto PostgreSQL) para um dialeto compatível com Spark.
- **Execução com Spark**: Inicia um job PySpark em segundo plano para executar a consulta sobre os dados do MinIO.
- **Salvar Resultados**: Salva o resultado da consulta em formato Parquet na camada `silver` do MinIO.

## Tecnologias

- **Backend**: FastAPI
- **Servidor**: Uvicorn
- **Processamento**: PySpark
- **Frontend**: HTML, CSS, JavaScript
- **Interação S3**: Boto3
- **Análise e Tradução de SQL**: SQLGlot, SQL-Unviewer
