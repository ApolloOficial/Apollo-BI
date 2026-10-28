# Databricks notebook source
# MAGIC %md
# MAGIC ## Limpeza dos dados: raw para cleaned
# MAGIC Fonte da pipeline `Apollo_BI` (camada Silver). Lê as 20 tabelas da camada `raw`, aplica 16 tratamentos de qualidade de dados (texto, número, data, chave e duplicidade) e define cada tabela da camada `cleaned` com `@dp.materialized_view()`. A tabela `cleaning_log` resume, por tabela, as linhas e os nulos antes e depois da limpeza.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Imports e catálogo
# MAGIC Dentro da pipeline não há gravação direta com `.write.saveAsTable()`: cada tabela é uma função decorada, e a pipeline controla a criação, a ordem e a atualização.

# COMMAND ----------

import re
from functools import reduce

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType, FloatType, NumericType, DateType, TimestampType
from pyspark.sql.window import Window

CATALOG = "workspace"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Configurações dos tratamentos
# MAGIC Tudo que depende do domínio do Apollo fica aqui. Quando aparecerem erros novos nos dados, é só ajustar esta célula.

# COMMAND ----------

FAKE_NULLS = ["", "s/n", "n/a", "na", "n.a.", "null", "none", "nan", "nil", "-", "--", "?", "nulo",
              "sem informacao", "sem informação", "nao informado", "não informado", "#n/a"]

CATEGORY_PATTERN = r"(status|stats|severity|scope|state|warning_type)$"

DOMAINS = {
    ("fact_maintenance", "maintenance_status"): ["ABERTA", "EM_ANDAMENTO", "CONCLUIDA", "CANCELADA"],
    ("fact_warning", "maintenance_status"): ["ABERTA", "EM_ANDAMENTO", "CONCLUIDA", "CANCELADA"],
    ("fact_warning", "status"): ["ATIVO", "RECONHECIDO", "RESOLVIDO"],
    ("fact_relocation", "status"): ["PENDENTE", "APROVADA", "REJEITADA", "CONCLUIDA"],
    ("dim_panel", "operating_stats"): ["EM_ESTOQUE", "OPERACIONAL", "MANUTENCAO", "INATIVO", "FALHA"],
}

NEGATIVE_PATTERN = r"(energy|kwh|power|cost|quantity|stock|days|hours|count|kwp|value|warranty)"

RANGES = {
    "health_score": (0, 100),
    "peak_to_nominal_ratio": (0, 1.5),
    "warranty_years": (0, 50),
}

MIN_DATE = "2000-01-01"
FUTURE_EXEMPT = r"(due|deadline|expected|scheduled|planned|expir)"
DATE_EXEMPT_TABLES = ["dim_date"]

CHRONOLOGY = [("dim_panel", "installation_dt", "deactivated_at")]

ORDER_COLS = ["updated_at", "synced_at", "calculated_at"]

FOREIGN_KEYS = {
    "dim_company_unit": [("company_key", "dim_company")],
    "dim_inverter": [("company_unit_key", "dim_company_unit")],
    "dim_string": [("inverter_key", "dim_inverter"), ("panel_model_key", "dim_panel_model")],
    "dim_panel": [("string_key", "dim_string")],
    "fact_string_daily_performance": [("string_key", "dim_string"), ("date_key", "dim_date")],
    "fact_string_health": [("string_key", "dim_string")],
    "fact_maintenance": [("company_unit_key", "dim_company_unit"), ("maintenance_type_key", "dim_maintenance_type")],
    "fact_warning": [("company_unit_key", "dim_company_unit")],
    "fact_stock": [("company_unit_key", "dim_company_unit"), ("part_key", "dim_part")],
}

# COMMAND ----------

# MAGIC %md
# MAGIC #### Chaves de cada tabela
# MAGIC As 20 tabelas da `raw` que a pipeline trata. A ordem de execução não é definida aqui: a pipeline deduz a ordem pelas leituras (por exemplo, `dim_panel` espera `dim_string` por causa da chave estrangeira).

# COMMAND ----------

TABLE_KEYS = {
    "dim_company": ["company_key"],
    "dim_segment": ["segment_key"],
    "dim_company_unit": ["company_unit_key"],
    "dim_panel_model": ["panel_model_key"],
    "dim_inverter": ["inverter_key"],
    "dim_string": ["string_key"],
    "dim_panel": ["panel_key"],
    "dim_employee": ["employee_key"],
    "dim_date": ["date_key"],
    "dim_maintenance_type": ["maintenance_type_key"],
    "dim_part": ["part_key"],
    "fact_string_daily_performance": ["string_key", "date_key"],
    "fact_string_health": ["string_key", "calculated_at"],
    "fact_maintenance": ["maintenance_key"],
    "fact_part_consumption": ["maintenance_key", "part_key"],
    "fact_daily_active_user": ["company_key", "date_key"],
    "fact_warning": ["warning_key"],
    "fact_relocation": ["relocation_scope", "relocation_key"],
    "fact_stock": ["company_unit_key", "part_key"],
    "vw_maintenance_chain": ["id"],
}

TABLES = list(TABLE_KEYS)

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

def fake_null_to_null(df, table=None):
    for column in text_columns(df):
        df = df.withColumn(column, F.when(F.lower(F.col(column)).isin(FAKE_NULLS), None).otherwise(F.col(column)))
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Caixa das categorias

# COMMAND ----------

def category_columns(df):
    return [column for column in text_columns(df) if re.search(CATEGORY_PATTERN, column)]

def upper_categories(df):
    for column in category_columns(df):
        df = df.withColumn(column, F.upper(F.col(column)))
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4. Grafia das categorias

# COMMAND ----------

ACCENTS_FROM = "ÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ"
ACCENTS_TO = "AAAAAEEEEIIIIOOOOOUUUUC"
assert len(ACCENTS_FROM) == len(ACCENTS_TO)

def fix_category_spelling(df):
    for column in category_columns(df):
        if re.search(r"state$", column):
            continue
        value = F.translate(F.col(column), ACCENTS_FROM, ACCENTS_TO)
        df = df.withColumn(column, F.regexp_replace(value, r"[\s\-]+", "_"))
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 5. Categoria fora do domínio

# COMMAND ----------

def invalid_domain_to_null(df, table):
    for (domain_table, column), valid in DOMAINS.items():
        if domain_table == table and column in df.columns:
            df = df.withColumn(column, F.when(F.col(column).isin(valid), F.col(column)))
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC #### Tratamentos numéricos

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 6. NaN e infinito

# COMMAND ----------

def nan_to_null(df):
    for field in df.schema.fields:
        if isinstance(field.dataType, (DoubleType, FloatType)):
            column = field.name
            bad = F.isnan(column) | F.col(column).isin(float("inf"), float("-inf"))
            df = df.withColumn(column, F.when(bad, None).otherwise(F.col(column)))
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 7. Negativo impossível

# COMMAND ----------

def negative_to_null(df):
    for field in df.schema.fields:
        column = field.name
        if (isinstance(field.dataType, NumericType) and re.search(NEGATIVE_PATTERN, column)
                and not re.search(r"(_key$|_id$|delta)", column)):
            df = df.withColumn(column, F.when(F.col(column) < 0, None).otherwise(F.col(column)))
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 8. Fora da faixa

# COMMAND ----------

def out_of_range_to_null(df):
    for column, (low, high) in RANGES.items():
        if column in df.columns:
            df = df.withColumn(column, F.when(F.col(column).between(low, high), F.col(column)))
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 9. Outlier extremo de geração

# COMMAND ----------

def extreme_outlier_to_null(df, table):
    if table != "fact_string_daily_performance" or "generated_energy_kwh" not in df.columns:
        return df
    q = (df.groupBy("string_key")
           .agg(F.percentile_approx("generated_energy_kwh", [0.25, 0.75]).alias("q"))
           .select("string_key", F.col("q")[0].alias("_q1"), F.col("q")[1].alias("_q3")))
    df = df.join(q, "string_key", "left")
    iqr = F.col("_q3") - F.col("_q1")
    extreme = (iqr > 0) & (F.col("generated_energy_kwh") > F.col("_q3") + 3 * iqr)
    df = df.withColumn("generated_energy_kwh", F.when(extreme, None).otherwise(F.col("generated_energy_kwh")))
    return df.drop("_q1", "_q3")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Tratamentos de data

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 10. Data no futuro

# COMMAND ----------

def date_columns(df):
    return [f.name for f in df.schema.fields if isinstance(f.dataType, (DateType, TimestampType))]

def future_dates_to_null(df, table):
    if table in DATE_EXEMPT_TABLES:
        return df
    limit = F.current_timestamp() + F.expr("INTERVAL 1 DAY")
    for column in date_columns(df):
        if not re.search(FUTURE_EXEMPT, column):
            df = df.withColumn(column, F.when(F.col(column) > limit, None).otherwise(F.col(column)))
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 11. Data de preenchimento padrão

# COMMAND ----------

def placeholder_dates_to_null(df, table):
    if table in DATE_EXEMPT_TABLES:
        return df
    for column in date_columns(df):
        df = df.withColumn(column, F.when(F.col(column) < F.lit(MIN_DATE).cast("date"), None).otherwise(F.col(column)))
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 12. Ordem cronológica

# COMMAND ----------

def invalid_chronology(df, table):
    for chrono_table, first, second in CHRONOLOGY:
        if chrono_table == table and first in df.columns and second in df.columns:
            df = df.withColumn(first, F.when(F.col(second) < F.col(first), None).otherwise(F.col(first)))
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC #### Tratamentos de chave e duplicidade

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 13. Linha duplicada

# COMMAND ----------

def drop_exact_duplicates(df):
    return df.dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 14. Chave nula

# COMMAND ----------

def drop_null_keys(df, keys):
    return df.dropna(subset=keys) if keys else df

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 15. Chave duplicada

# COMMAND ----------

def drop_key_duplicates(df, keys):
    if not keys:
        return df
    order = next((c for c in ORDER_COLS if c in df.columns and c not in keys), None)
    if order is None:
        return df.dropDuplicates(keys)
    w = Window.partitionBy(*keys).orderBy(F.col(order).desc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 16. Chave estrangeira órfã
# MAGIC Lê a tabela pai já tratada, então a pipeline executa o pai antes do filho.

# COMMAND ----------

def drop_orphans(df, table):
    for column, parent in FOREIGN_KEYS.get(table, []):
        if column in df.columns:
            parent_keys = spark.read.table(f"{CATALOG}.cleaned.{parent}").select(F.col(column).alias("_pk")).distinct()
            df = df.join(parent_keys, df[column] == parent_keys["_pk"], "left")
            df = df.filter(F.col(column).isNull() | F.col("_pk").isNotNull()).drop("_pk")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC #### Ordem dos tratamentos
# MAGIC A ordem importa: primeiro padroniza o texto, depois zera o que é inválido, e só então remove linhas (chave nula, duplicadas e órfãs), para não perder linha que ficaria boa depois da padronização.

# COMMAND ----------

STEPS = [
    ("1 espacos extras",            lambda df, t: trim_texts(df)),
    ("2 nulo escrito como texto",   lambda df, t: fake_null_to_null(df)),
    ("3 caixa das categorias",      lambda df, t: upper_categories(df)),
    ("4 grafia das categorias",     lambda df, t: fix_category_spelling(df)),
    ("5 categoria fora do dominio", lambda df, t: invalid_domain_to_null(df, t)),
    ("6 nan e infinito",            lambda df, t: nan_to_null(df)),
    ("7 negativo impossivel",       lambda df, t: negative_to_null(df)),
    ("8 fora da faixa",             lambda df, t: out_of_range_to_null(df)),
    ("9 outlier extremo",           lambda df, t: extreme_outlier_to_null(df, t)),
    ("10 data no futuro",           lambda df, t: future_dates_to_null(df, t)),
    ("11 data padrao antiga",       lambda df, t: placeholder_dates_to_null(df, t)),
    ("12 ordem cronologica",        lambda df, t: invalid_chronology(df, t)),
    ("13 linha duplicada",          lambda df, t: drop_exact_duplicates(df)),
    ("14 chave nula",               lambda df, t: drop_null_keys(df, TABLE_KEYS.get(t, []))),
    ("15 chave duplicada",          lambda df, t: drop_key_duplicates(df, TABLE_KEYS.get(t, []))),
    ("16 chave estrangeira orfa",   lambda df, t: drop_orphans(df, t)),
]

def clean_table(df, table):
    for _, step in STEPS:
        df = step(df, table)
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC #### Definir as tabelas da cleaned
# MAGIC Uma `@dp.materialized_view()` para cada uma das 20 tabelas. A função devolve o DataFrame tratado, e a pipeline grava `workspace.cleaned.<tabela>`.

# COMMAND ----------

def define_cleaned(table):
    @dp.materialized_view(
        name=f"{CATALOG}.cleaned.{table}",
        comment=f"Tabela {table} tratada a partir da camada raw"
    )
    def cleaned_table():
        return clean_table(spark.read.table(f"{CATALOG}.raw.{table}"), table)

for table in TABLES:
    define_cleaned(table)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Resumo da limpeza
# MAGIC Evidência da etapa de tratamento para o relatório gerencial: para cada tabela, as linhas e os nulos na `raw` e na `cleaned`.

# COMMAND ----------

def table_stats(df):
    nulls = sum((F.col(c).isNull().cast("int") for c in df.columns), F.lit(0))
    return df.agg(F.count("*").alias("linhas"), F.coalesce(F.sum(nulls), F.lit(0)).alias("nulos"))

def log_row(table):
    antes = table_stats(spark.read.table(f"{CATALOG}.raw.{table}")).select(
        F.col("linhas").alias("linhas_raw"), F.col("nulos").alias("nulos_raw"))
    depois = table_stats(spark.read.table(f"{CATALOG}.cleaned.{table}")).select(
        F.col("linhas").alias("linhas_cleaned"), F.col("nulos").alias("nulos_cleaned"))
    return (antes.crossJoin(depois)
                 .withColumn("tabela", F.lit(table))
                 .withColumn("linhas_removidas", F.col("linhas_raw") - F.col("linhas_cleaned")))

@dp.materialized_view(
    name=f"{CATALOG}.cleaned.cleaning_log",
    comment="Resumo da limpeza por tabela: linhas e nulos antes e depois dos 16 tratamentos"
)
def cleaning_log():
    log = reduce(lambda a, b: a.unionByName(b), [log_row(t) for t in TABLES])
    return (log.select("tabela", "linhas_raw", "linhas_cleaned", "linhas_removidas", "nulos_raw", "nulos_cleaned")
               .withColumn("executado_em", F.current_timestamp()))
