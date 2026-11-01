-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Limpeza: dim_maintenance_type
-- MAGIC Refaz `workspace.cleaned.dim_maintenance_type` a partir de `workspace.raw.dim_maintenance_type`: padroniza textos, zera valores inválidos e remove linhas duplicadas, sem chave ou sem pai. Fonte da pipeline `Apollo_BI`.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW workspace.cleaned.dim_maintenance_type
COMMENT 'Tipos de manutenção tratadas a partir da camada raw'
AS
WITH
lida AS (
  SELECT * FROM workspace.raw.dim_maintenance_type
),
aparada AS (
  SELECT
    maintenance_type_key,
    trim(regexp_replace(type_name, '\\s+', ' ')) AS type_name,
    default_interval_days
  FROM lida
),
sem_nulo_falso AS (
  SELECT
    maintenance_type_key,
    CASE WHEN lower(type_name) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE type_name END AS type_name,
    default_interval_days
  FROM aparada
),
sem_negativos AS (
  SELECT
    maintenance_type_key,
    type_name,
    CASE WHEN default_interval_days < 0 THEN NULL ELSE default_interval_days END AS default_interval_days
  FROM sem_nulo_falso
),
sem_duplicadas AS (
  SELECT DISTINCT * FROM sem_negativos
),
com_chave AS (
  SELECT * FROM sem_duplicadas
  WHERE maintenance_type_key IS NOT NULL
),
chave_unica AS (
  SELECT
    maintenance_type_key,
    type_name,
    default_interval_days
  FROM (
    SELECT *, row_number() OVER (PARTITION BY maintenance_type_key ORDER BY maintenance_type_key) AS _linha
    FROM com_chave
  )
  WHERE _linha = 1
)
SELECT * FROM chave_unica;
