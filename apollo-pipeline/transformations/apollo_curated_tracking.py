# Databricks notebook source
# MAGIC %md
# MAGIC ## Cálculo dos KPIs do tracking mobile: cleaned para curated
# MAGIC Fonte da pipeline `Apollo_BI` (camada Gold). Lê `workspace.cleaned.tracking_events`, calcula os KPIs de uso do app (atividade diária, telas, API, ações de toque e sessões) e define cada tabela da camada `curated` com `@dp.materialized_view()`. Esta fonte não depende do Neon.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Imports e configuração

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = "workspace"
CLEANED = f"{CATALOG}.cleaned"
CURATED = f"{CATALOG}.curated"
TIMEZONE = "America/Sao_Paulo"
HTTP_ERRO_MIN = 400

# COMMAND ----------

# MAGIC %md
# MAGIC #### Leitura

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Eventos do tracking

# COMMAND ----------

def read_events():
    return (spark.read.table(f"{CLEANED}.tracking_events")
            .filter(F.col("company_id").isNotNull())
            .withColumn("event_date", F.to_date(F.from_utc_timestamp("timestamp", TIMEZONE))))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Contar linhas que atendem a uma condição

# COMMAND ----------

def count_if(condition):
    return F.sum(F.when(condition, 1).otherwise(0))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Atividade

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Atividade diária por empresa

# COMMAND ----------

def atividade_diaria_empresa(events):
    return events.groupBy("company_id", "event_date").agg(
        F.count("*").alias("eventos"),
        F.countDistinct("session_id").alias("sessoes"),
        F.countDistinct("user_id").alias("usuarios_ativos"),
        count_if(F.col("event_name") == "app_opened").alias("aberturas_app"),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Sessões

# COMMAND ----------

def sessoes(events):
    df = events.groupBy("session_id", "company_id", "user_id").agg(
        F.min("timestamp").alias("inicio"),
        F.max("timestamp").alias("fim"),
        F.count("*").alias("eventos"),
        F.countDistinct("screen").alias("telas_distintas"),
    )
    return df.withColumn("duracao_s", F.round(F.col("fim").cast("double") - F.col("inicio").cast("double"), 1))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Telas e ações

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Uso por tela

# COMMAND ----------

def uso_tela(events):
    views = (events.filter(F.col("event_name") == "screen_viewed")
                   .groupBy("company_id", "screen").agg(F.count("*").alias("visualizacoes")))
    tempo = (events.filter((F.col("event_name") == "screen_exited") & F.col("properties_duration_ms").isNotNull())
                   .groupBy("company_id", "screen").agg(
                       F.round(F.avg("properties_duration_ms"), 0).alias("tempo_medio_ms"),
                       F.percentile_approx("properties_duration_ms", 0.5).alias("tempo_mediano_ms")))
    return views.join(tempo, ["company_id", "screen"], "outer")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Ações de toque

# COMMAND ----------

def acoes(events):
    return (events.filter(F.col("event_name") == "ui_action")
                  .groupBy("company_id", "screen", "properties_target", "properties_input_method")
                  .agg(F.count("*").alias("acoes"))
                  .withColumnRenamed("properties_target", "alvo")
                  .withColumnRenamed("properties_input_method", "forma_entrada"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### API

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Chamadas de API por dia e endpoint

# COMMAND ----------

def api_diaria(events):
    api = events.filter(F.col("event_name") == "api_request_completed")
    df = api.groupBy("company_id", "event_date", "properties_endpoint").agg(
        F.count("*").alias("requisicoes"),
        count_if(F.col("properties_http_status") >= HTTP_ERRO_MIN).alias("erros"),
        F.round(F.avg("properties_duration_ms"), 0).alias("latencia_media_ms"),
        F.percentile_approx("properties_duration_ms", 0.95).alias("latencia_p95_ms"),
    ).withColumnRenamed("properties_endpoint", "endpoint")
    return df.withColumn("taxa_erro_pct", F.round(100 * F.col("erros") / F.col("requisicoes"), 2))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Definir as tabelas da curated
# MAGIC Uma `@dp.materialized_view()` para cada KPI do tracking.

# COMMAND ----------

@dp.materialized_view(
    name=f"{CURATED}.kpi_tracking_diario_empresa",
    comment="Eventos, sessões, usuários ativos e aberturas do app por empresa e dia"
)
def kpi_tracking_diario_empresa():
    return atividade_diaria_empresa(read_events())

# COMMAND ----------

@dp.materialized_view(
    name=f"{CURATED}.kpi_tracking_sessao",
    comment="Uma linha por sessão: duração, eventos e telas distintas"
)
def kpi_tracking_sessao():
    return sessoes(read_events())

# COMMAND ----------

@dp.materialized_view(
    name=f"{CURATED}.kpi_tracking_tela",
    comment="Visualizações e tempo médio e mediano por tela"
)
def kpi_tracking_tela():
    return uso_tela(read_events())

# COMMAND ----------

@dp.materialized_view(
    name=f"{CURATED}.kpi_tracking_acoes",
    comment="Toques por tela, alvo e forma de entrada"
)
def kpi_tracking_acoes():
    return acoes(read_events())

# COMMAND ----------

@dp.materialized_view(
    name=f"{CURATED}.kpi_tracking_api_diaria",
    comment="Requisições, erros, taxa de erro e latência por endpoint e dia"
)
def kpi_tracking_api_diaria():
    return api_diaria(read_events())
