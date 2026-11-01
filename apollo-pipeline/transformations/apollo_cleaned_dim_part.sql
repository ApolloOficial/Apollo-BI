-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Limpeza: dim_part
-- MAGIC Refaz `workspace.cleaned.dim_part` a partir de `workspace.raw.dim_part`: padroniza textos, zera valores inválidos e remove linhas duplicadas, sem chave ou sem pai. Fonte da pipeline `Apollo_BI`.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW workspace.cleaned.dim_part
COMMENT 'Peças tratadas a partir da camada raw'
AS
WITH
lida AS (
  SELECT * FROM workspace.raw.dim_part
),
aparada AS (
  SELECT
    part_key,
    trim(regexp_replace(sku, '\\s+', ' ')) AS sku,
    trim(regexp_replace(part_name, '\\s+', ' ')) AS part_name,
    trim(regexp_replace(manufacturer, '\\s+', ' ')) AS manufacturer
  FROM lida
),
sem_nulo_falso AS (
  SELECT
    part_key,
    CASE WHEN lower(sku) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE sku END AS sku,
    CASE WHEN lower(part_name) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE part_name END AS part_name,
    CASE WHEN lower(manufacturer) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE manufacturer END AS manufacturer
  FROM aparada
),
sem_duplicadas AS (
  SELECT DISTINCT * FROM sem_nulo_falso
),
com_chave AS (
  SELECT * FROM sem_duplicadas
  WHERE part_key IS NOT NULL
),
chave_unica AS (
  SELECT
    part_key,
    sku,
    part_name,
    manufacturer
  FROM (
    SELECT *, row_number() OVER (PARTITION BY part_key ORDER BY part_key) AS _linha
    FROM com_chave
  )
  WHERE _linha = 1
)
SELECT * FROM chave_unica;
