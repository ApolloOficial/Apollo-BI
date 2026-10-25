# Databricks notebook source
# MAGIC %md
# MAGIC ## Conexão com a base de dados Apollo
# MAGIC Esse notebook conecta o Databricks ao banco do Apollo no Neon, define a lista das 15 views do schema dm que o BI usa e cria os schemas raw, cleaned e curated. Os notebooks do pipeline reutilizam o que é definido aqui.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Ler as credenciais do `.env`

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
# MAGIC #### Conectar com o banco
# MAGIC Cria a função `read_data`, que traz uma tabela ou view do Neon para o Databricks.

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
# MAGIC #### Definir o que o BI vai ler
# MAGIC Catálogo onde as tabelas ficam e as 15 views do schema `dm` (dimensões, fatos e a cadeia de manutenção).

# COMMAND ----------

CATALOG = "workspace"
NEON_SCHEMA = "dm"

VIEWS = [
    "dim_company", "dim_segment", "dim_company_unit", "dim_panel_model",
    "dim_inverter", "dim_string", "dim_panel", "dim_employee", "dim_date",
    "fact_string_daily_performance", "fact_string_health", "fact_maintenance",
    "fact_part_consumption", "fact_daily_active_user", "vw_maintenance_chain",
    "dim_maintenance_type", "dim_part", "fact_warning", "fact_relocation", "fact_stock"
]

# COMMAND ----------

# MAGIC %md
# MAGIC #### Criar as camadas
# MAGIC Cria os schemas `raw`, `cleaned` e `curated` no Databricks.

# COMMAND ----------

for layer in ("raw", "cleaned", "curated"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{layer}")