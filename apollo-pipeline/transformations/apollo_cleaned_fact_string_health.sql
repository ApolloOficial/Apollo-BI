-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Limpeza: fact_string_health
-- MAGIC Refaz `workspace.cleaned.fact_string_health` a partir de `workspace.raw.fact_string_health`: padroniza textos, zera valores inválidos e remove linhas duplicadas, sem chave ou sem pai. Fonte da pipeline `Apollo_BI`.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW workspace.cleaned.fact_string_health
COMMENT 'Saúde das strings tratadas a partir da camada raw'
AS
WITH
lida AS (
  SELECT * FROM workspace.raw.fact_string_health
),
aparada AS (
  SELECT
    date_key,
    trim(regexp_replace(string_key, '\\s+', ' ')) AS string_key,
    trim(regexp_replace(inverter_key, '\\s+', ' ')) AS inverter_key,
    calculated_at,
    window_days,
    sibling_ratio,
    nominal_ratio,
    warning_penalty,
    health_score,
    health_moving_average_7,
    health_delta
  FROM lida
),
sem_nulo_falso AS (
  SELECT
    date_key,
    CASE WHEN lower(string_key) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE string_key END AS string_key,
    CASE WHEN lower(inverter_key) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE inverter_key END AS inverter_key,
    calculated_at,
    window_days,
    sibling_ratio,
    nominal_ratio,
    warning_penalty,
    health_score,
    health_moving_average_7,
    health_delta
  FROM aparada
),
sem_negativos AS (
  SELECT
    date_key,
    string_key,
    inverter_key,
    calculated_at,
    CASE WHEN window_days < 0 THEN NULL ELSE window_days END AS window_days,
    sibling_ratio,
    nominal_ratio,
    warning_penalty,
    health_score,
    health_moving_average_7,
    health_delta
  FROM sem_nulo_falso
),
na_faixa AS (
  SELECT
    date_key,
    string_key,
    inverter_key,
    calculated_at,
    window_days,
    sibling_ratio,
    nominal_ratio,
    warning_penalty,
    CASE WHEN health_score BETWEEN 0 AND 100 THEN health_score END AS health_score,
    health_moving_average_7,
    health_delta
  FROM sem_negativos
),
datas_validas AS (
  SELECT
    date_key,
    string_key,
    inverter_key,
    CASE WHEN calculated_at > current_timestamp() + INTERVAL 1 DAY OR calculated_at < DATE '2000-01-01' THEN NULL ELSE calculated_at END AS calculated_at,
    window_days,
    sibling_ratio,
    nominal_ratio,
    warning_penalty,
    health_score,
    health_moving_average_7,
    health_delta
  FROM na_faixa
),
sem_duplicadas AS (
  SELECT DISTINCT * FROM datas_validas
),
com_chave AS (
  SELECT * FROM sem_duplicadas
  WHERE string_key IS NOT NULL AND calculated_at IS NOT NULL
),
chave_unica AS (
  SELECT
    date_key,
    string_key,
    inverter_key,
    calculated_at,
    window_days,
    sibling_ratio,
    nominal_ratio,
    warning_penalty,
    health_score,
    health_moving_average_7,
    health_delta
  FROM (
    SELECT *, row_number() OVER (PARTITION BY string_key, calculated_at ORDER BY string_key, calculated_at) AS _linha
    FROM com_chave
  )
  WHERE _linha = 1
),
com_pai AS (
  SELECT * FROM chave_unica
  WHERE (string_key IS NULL OR string_key IN (SELECT string_key FROM workspace.cleaned.dim_string))
)
SELECT * FROM com_pai;
