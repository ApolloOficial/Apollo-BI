# Databricks notebook source
# MAGIC %md
# MAGIC ## Exploração: perfil dos dados da raw
# MAGIC Notebook de apoio, fora da pipeline e do Job. Mostra nulos, valores distintos e categorias de uma tabela da `raw` para decidir quais tratamentos entram em `transformations/apollo_cleaned_operacional.py`.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Imports

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# COMMAND ----------

# MAGIC %md
# MAGIC #### Funções de análise

# COMMAND ----------

def text_columns(df):
    return [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]

def profile(df):
    types = dict(df.dtypes)
    rows = []
    for column in df.columns:
        nulls = df.filter(F.col(column).isNull()).count()
        distinct = df.select(column).distinct().count()
        rows.append((column, types[column], nulls, distinct))
    return spark.createDataFrame(rows, "coluna string, tipo string, nulos long, distintos long")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Perfil de uma tabela
# MAGIC Troque o nome da tabela para analisar outra.

# COMMAND ----------

df = spark.table("workspace.raw.dim_company")
display(profile(df))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Categorias dos textos

# COMMAND ----------

for column in text_columns(df):
    if df.select(column).distinct().count() <= 15:
        display(df.groupBy(column).count().orderBy("count", ascending=False))
