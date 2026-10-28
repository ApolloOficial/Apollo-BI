# Databricks notebook source
# MAGIC %md
# MAGIC ## Limpeza do tracking mobile: raw para cleaned
# MAGIC Fonte da pipeline `Apollo_BI` (camada Silver). Lê `workspace.raw.tracking_events`, converte os tipos, aplica 10 tratamentos de qualidade e define `workspace.cleaned.tracking_events` com `@dp.materialized_view()`. A tabela `cleaning_log_mongo` resume as linhas e os nulos antes e depois da limpeza. Esta fonte não depende do Neon, então a pipeline a executa em paralelo com `apollo_cleaned_operacional`.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Imports e configuração

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

CATALOG = "workspace"
TABLE = "tracking_events"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Configurações dos tratamentos
# MAGIC Tudo que depende do domínio fica aqui. Quando aparecerem erros novos no tracking, é só ajustar esta célula.

# COMMAND ----------

FAKE_NULLS = ["", "s/n", "n/a", "na", "n.a.", "null", "none", "nan", "nil", "-", "--", "?", "undefined",
              "sem informacao", "sem informação", "nao informado", "não informado", "#n/a"]

LOWER_COLUMNS = ["event_name", "platform", "source", "properties_exit_reason", "properties_input_method"]
UPPER_COLUMNS = ["properties_method"]

TYPES = {
    "company_id": "bigint",
    "schema_version": "int",
    "timestamp": "timestamp",
    "received_at": "timestamp",
    "properties_duration_ms": "double",
    "properties_http_status": "int",
}

UUID_COLUMNS = ["event_id", "session_id", "user_id"]

RANGES = {
    "properties_duration_ms": (0, 86_400_000),
    "properties_http_status": (100, 599),
    "schema_version": (1, 1000),
}

MIN_TIMESTAMP = "2020-01-01"
TIME_COLUMNS = ["timestamp", "received_at"]

REQUIRED_KEYS = ["event_id", "event_name", "timestamp", "session_id"]
IGNORE_IN_DUPLICATES = ["mongo_id", "ingested_at"]

# COMMAND ----------

# MAGIC %md
# MAGIC #### Tratamentos de texto

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Espaços extras

# COMMAND ----------

def text_columns(df):
    return [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]

def trim_texts(df):
    for column in text_columns(df):
        df = df.withColumn(column, F.trim(F.regexp_replace(F.col(column), r"\s+", " ")))
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Nulo escrito como texto

# COMMAND ----------

def fake_null_to_null(df):
    for column in text_columns(df):
        df = df.withColumn(column, F.when(F.lower(F.col(column)).isin(FAKE_NULLS), None).otherwise(F.col(column)))
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Caixa das categorias

# COMMAND ----------

def normalize_categories(df):
    for column in LOWER_COLUMNS:
        if column in df.columns:
            df = df.withColumn(column, F.lower(F.col(column)))
    for column in UPPER_COLUMNS:
        if column in df.columns:
            df = df.withColumn(column, F.upper(F.col(column)))
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4. Identificador com formato inválido

# COMMAND ----------

UUID_REGEX = r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"

def invalid_uuid_to_null(df):
    for column in UUID_COLUMNS:
        if column in df.columns:
            df = df.withColumn(column, F.when(F.col(column).rlike(UUID_REGEX), F.col(column)))
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC #### Tratamentos de tipo, número e data

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 5. Conversão de tipos

# COMMAND ----------

def cast_types(df):
    for column, dtype in TYPES.items():
        if column in df.columns:
            if dtype in ("int", "bigint"):
                expr = f"try_cast(try_cast(`{column}` as double) as {dtype})"
            else:
                expr = f"try_cast(`{column}` as {dtype})"
            df = df.withColumn(column, F.expr(expr))
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 6. NaN e infinito

# COMMAND ----------

def nan_to_null(df):
    for field in df.schema.fields:
        if field.dataType.simpleString() == "double":
            column = field.name
            bad = F.isnan(column) | F.col(column).isin(float("inf"), float("-inf"))
            df = df.withColumn(column, F.when(bad, None).otherwise(F.col(column)))
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 7. Fora da faixa

# COMMAND ----------

def out_of_range_to_null(df):
    for column, (low, high) in RANGES.items():
        if column in df.columns:
            df = df.withColumn(column, F.when(F.col(column).between(low, high), F.col(column)))
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 8. Data impossível

# COMMAND ----------

def bad_timestamps_to_null(df):
    limit = F.current_timestamp() + F.expr("INTERVAL 1 DAY")
    for column in TIME_COLUMNS:
        if column in df.columns:
            invalid = (F.col(column) > limit) | (F.col(column) < F.lit(MIN_TIMESTAMP).cast("timestamp"))
            df = df.withColumn(column, F.when(invalid, None).otherwise(F.col(column)))
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC #### Tratamentos de chave e duplicidade

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 9. Evento sem campo obrigatório

# COMMAND ----------

def drop_null_keys(df):
    return df.dropna(subset=[c for c in REQUIRED_KEYS if c in df.columns])

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 10. Evento duplicado

# COMMAND ----------

def drop_duplicates(df):
    content = [c for c in df.columns if c not in IGNORE_IN_DUPLICATES]
    df = df.dropDuplicates(content)
    w = Window.partitionBy("event_id").orderBy(F.col("received_at").desc_nulls_last(), F.col("ingested_at").desc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Ordem dos tratamentos
# MAGIC A ordem importa: primeiro padroniza o texto, depois converte e zera o que é inválido, e só então remove linhas.

# COMMAND ----------

STEPS = [
    ("1 espacos extras",         trim_texts),
    ("2 nulo escrito como texto", fake_null_to_null),
    ("3 caixa das categorias",   normalize_categories),
    ("4 identificador invalido", invalid_uuid_to_null),
    ("5 conversao de tipos",     cast_types),
    ("6 nan e infinito",         nan_to_null),
    ("7 fora da faixa",          out_of_range_to_null),
    ("8 data impossivel",        bad_timestamps_to_null),
    ("9 campo obrigatorio nulo", drop_null_keys),
    ("10 evento duplicado",      drop_duplicates),
]

def clean_events(df):
    for _, step in STEPS:
        df = step(df)
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC #### Definir a tabela da cleaned

# COMMAND ----------

@dp.materialized_view(
    name=f"{CATALOG}.cleaned.{TABLE}",
    comment="Eventos de tracking do app mobile com tipos convertidos e tratamentos de qualidade aplicados"
)
def tracking_events():
    return clean_events(spark.read.table(f"{CATALOG}.raw.{TABLE}"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Resumo da limpeza
# MAGIC Evidência da etapa de tratamento para o relatório gerencial: as linhas e os nulos na `raw` e na `cleaned`.

# COMMAND ----------

def table_stats(df):
    nulls = sum((F.col(c).isNull().cast("int") for c in df.columns), F.lit(0))
    return df.agg(F.count("*").alias("linhas"), F.coalesce(F.sum(nulls), F.lit(0)).alias("nulos"))

@dp.materialized_view(
    name=f"{CATALOG}.cleaned.cleaning_log_mongo",
    comment="Resumo da limpeza do tracking: linhas e nulos antes e depois dos 10 tratamentos"
)
def cleaning_log_mongo():
    antes = table_stats(spark.read.table(f"{CATALOG}.raw.{TABLE}")).select(
        F.col("linhas").alias("linhas_raw"), F.col("nulos").alias("nulos_raw"))
    depois = table_stats(spark.read.table(f"{CATALOG}.cleaned.{TABLE}")).select(
        F.col("linhas").alias("linhas_cleaned"), F.col("nulos").alias("nulos_cleaned"))
    return (antes.crossJoin(depois)
                 .withColumn("tabela", F.lit(TABLE))
                 .withColumn("linhas_removidas", F.col("linhas_raw") - F.col("linhas_cleaned"))
                 .select("tabela", "linhas_raw", "linhas_cleaned", "linhas_removidas", "nulos_raw", "nulos_cleaned")
                 .withColumn("executado_em", F.current_timestamp()))
