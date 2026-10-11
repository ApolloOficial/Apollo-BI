# Databricks notebook source
# MAGIC %md
# MAGIC ## Conexão com a base de dados Apollo
# MAGIC Esse notebook conecta o Databricks ao banco do Apollo no Neon, define a lista das 15 views do schema `dm` que o BI usa e cria os schemas `raw`, `cleaned` e `curated`. Os notebooks do pipeline reutilizam o que é definido aqui.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Instalar a biblioteca que lê o `.env`

# COMMAND ----------

# MAGIC %pip install python-dotenv

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Ler as credenciais do `.env`

# COMMAND ----------

import os
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv(usecwd=True))

host = os.getenv("NEON_HOST")
database = os.getenv("NEON_DB")
user = os.getenv("NEON_USER")
password = os.getenv("NEON_PASSWORD")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Conectar com o banco
# MAGIC Cria a função `ler`, que traz uma tabela ou view do Neon para o Databricks.

# COMMAND ----------

def read_data(table):
    return (spark.read.format("jdbc")
            .option("url", f"jdbc:postgresql://{host}:5432/{database}?sslmode=require")
            .option("user", user)
            .option("password", password)
            .option("driver", "org.postgresql.Driver")
            .option("dbtable", table)
            .load())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Definir o que o BI vai ler
# MAGIC Catálogo onde as tabelas ficam e as 15 views do schema `dm` (dimensões, fatos e a cadeia de manutenção).

# COMMAND ----------

CATALOG = "workspace"
NEON_SCHEMA = "dm"

VIEWS = [
    "dim_company", "dim_segment", "dim_company_unit", "dim_panel_model",
    "dim_inverter", "dim_string", "dim_panel", "dim_employee", "dim_date",
    "fact_string_daily_performance", "fact_string_health", "fact_maintenance",
    "fact_part_consumption", "fact_daily_active_user", "vw_maintenance_chain",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Criar as camadas
# MAGIC Cria os schemas `raw`, `cleaned` e `curated` no Databricks.

# COMMAND ----------

for layer in ("raw", "cleaned", "curated"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{layer}")

display(spark.sql(f"SHOW SCHEMAS IN {CATALOG}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Testar a conexão com todas as views
# MAGIC Conta as linhas de cada view. Se todas aparecerem sem erro, a conexão está funcionando.

# COMMAND ----------

for view in VIEWS:
    print(view, read_data(f"{NEON_SCHEMA}.{view}").count(), "linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Olhar uma view
# MAGIC Troque o nome para ver outra.

# COMMAND ----------

df = read_data("dm.dim_company")
display(df)

# COMMAND ----------

for s in ("bronze", "silver", "gold"):
    print(s, spark.sql(f"SHOW TABLES IN workspace.{s}").count(), "tabelas")

# COMMAND ----------

for s in ("bronze", "silver", "gold"):
    display(spark.sql(f"SHOW TABLES IN workspace.{s}"))