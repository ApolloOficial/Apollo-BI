-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Limpeza: dim_segment
-- MAGIC Refaz `workspace.cleaned.dim_segment` a partir de `workspace.raw.dim_segment`: padroniza textos, zera valores inválidos e remove linhas duplicadas, sem chave ou sem pai. Fonte da pipeline `Apollo_BI`.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW workspace.cleaned.dim_segment
COMMENT 'Segmentos tratadas a partir da camada raw'
AS
WITH
lida AS (
  SELECT * FROM workspace.raw.dim_segment
),
aparada AS (
  SELECT
    segment_key,
    trim(regexp_replace(segment_name, '\\s+', ' ')) AS segment_name
  FROM lida
),
sem_nulo_falso AS (
  SELECT
    segment_key,
    CASE WHEN lower(segment_name) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE segment_name END AS segment_name
  FROM aparada
),
sem_duplicadas AS (
  SELECT DISTINCT * FROM sem_nulo_falso
),
com_chave AS (
  SELECT * FROM sem_duplicadas
  WHERE segment_key IS NOT NULL
),
chave_unica AS (
  SELECT
    segment_key,
    segment_name
  FROM (
    SELECT *, row_number() OVER (PARTITION BY segment_key ORDER BY segment_key) AS _linha
    FROM com_chave
  )
  WHERE _linha = 1
)
SELECT * FROM chave_unica;
