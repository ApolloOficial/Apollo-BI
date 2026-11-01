-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Limpeza: dim_date
-- MAGIC Refaz `workspace.cleaned.dim_date` a partir de `workspace.raw.dim_date`: padroniza textos, zera valores inválidos e remove linhas duplicadas, sem chave ou sem pai. Fonte da pipeline `Apollo_BI`.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW workspace.cleaned.dim_date
COMMENT 'Calendário tratadas a partir da camada raw'
AS
WITH
lida AS (
  SELECT * FROM workspace.raw.dim_date
),
aparada AS (
  SELECT
    date_key,
    full_date,
    day_number,
    month_number,
    trim(regexp_replace(month_name, '\\s+', ' ')) AS month_name,
    quarter_number,
    year_number,
    is_weekend
  FROM lida
),
sem_nulo_falso AS (
  SELECT
    date_key,
    full_date,
    day_number,
    month_number,
    CASE WHEN lower(month_name) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE month_name END AS month_name,
    quarter_number,
    year_number,
    is_weekend
  FROM aparada
),
sem_duplicadas AS (
  SELECT DISTINCT * FROM sem_nulo_falso
),
com_chave AS (
  SELECT * FROM sem_duplicadas
  WHERE date_key IS NOT NULL
),
chave_unica AS (
  SELECT
    date_key,
    full_date,
    day_number,
    month_number,
    month_name,
    quarter_number,
    year_number,
    is_weekend
  FROM (
    SELECT *, row_number() OVER (PARTITION BY date_key ORDER BY date_key) AS _linha
    FROM com_chave
  )
  WHERE _linha = 1
)
SELECT * FROM chave_unica;
