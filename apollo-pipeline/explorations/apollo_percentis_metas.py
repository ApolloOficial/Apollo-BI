# Databricks notebook source
# MAGIC %md
# MAGIC ## Exploração: percentis para definir as metas dos KPIs
# MAGIC Notebook de apoio, fora da pipeline e do Job. Lê a `cleaned` e mostra os percentis usados para escolher `LIMIAR_SAUDE`, `PADRAO_MIN`, `QUEDA_BRUSCA_PTS` e `META_RESOLUCAO_DIAS` em `transformations/apollo_curated_operacional.py`.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Imports

# COMMAND ----------

from pyspark.sql import functions as F

CLEANED = "workspace.cleaned"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Tempo de resolução das OS

# COMMAND ----------

display(spark.table(f"{CLEANED}.fact_maintenance").select(
    F.percentile_approx("resolution_days", [0.25, 0.5, 0.75, 0.9]).alias("percentis_resolucao_dias")))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Nota de saúde

# COMMAND ----------

display(spark.table(f"{CLEANED}.fact_string_health").select(
    F.percentile_approx("health_score", [0.1, 0.25, 0.5, 0.75]).alias("percentis_saude")))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Relação pico/nominal

# COMMAND ----------

display(spark.table(f"{CLEANED}.fact_string_daily_performance").select(
    F.percentile_approx("peak_to_nominal_ratio", [0.1, 0.25, 0.5, 0.75]).alias("percentis_pico_nominal")))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Queda de saúde

# COMMAND ----------

display(spark.table(f"{CLEANED}.fact_string_health").select(
    F.percentile_approx("health_delta", [0.01, 0.05, 0.25, 0.5]).alias("percentis_queda_saude")))
