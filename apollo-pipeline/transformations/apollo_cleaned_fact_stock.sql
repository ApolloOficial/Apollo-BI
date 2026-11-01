-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Limpeza: fact_stock
-- MAGIC Refaz `workspace.cleaned.fact_stock` a partir de `workspace.raw.fact_stock`: padroniza textos, zera valores inválidos e remove linhas duplicadas, sem chave ou sem pai. Fonte da pipeline `Apollo_BI`.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW workspace.cleaned.fact_stock
COMMENT 'Estoque de peças tratadas a partir da camada raw'
AS
WITH
lida AS (
  SELECT * FROM workspace.raw.fact_stock
),
aparada AS (
  SELECT
    trim(regexp_replace(company_unit_key, '\\s+', ' ')) AS company_unit_key,
    part_key,
    available_qtt,
    minimum_qtt,
    unit_cost,
    stock_value,
    below_minimum,
    updated_at
  FROM lida
),
sem_nulo_falso AS (
  SELECT
    CASE WHEN lower(company_unit_key) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE company_unit_key END AS company_unit_key,
    part_key,
    available_qtt,
    minimum_qtt,
    unit_cost,
    stock_value,
    below_minimum,
    updated_at
  FROM aparada
),
sem_negativos AS (
  SELECT
    company_unit_key,
    part_key,
    available_qtt,
    minimum_qtt,
    CASE WHEN unit_cost < 0 THEN NULL ELSE unit_cost END AS unit_cost,
    CASE WHEN stock_value < 0 THEN NULL ELSE stock_value END AS stock_value,
    below_minimum,
    updated_at
  FROM sem_nulo_falso
),
datas_validas AS (
  SELECT
    company_unit_key,
    part_key,
    available_qtt,
    minimum_qtt,
    unit_cost,
    stock_value,
    below_minimum,
    CASE WHEN updated_at > current_timestamp() + INTERVAL 1 DAY OR updated_at < DATE '2000-01-01' THEN NULL ELSE updated_at END AS updated_at
  FROM sem_negativos
),
sem_duplicadas AS (
  SELECT DISTINCT * FROM datas_validas
),
com_chave AS (
  SELECT * FROM sem_duplicadas
  WHERE company_unit_key IS NOT NULL AND part_key IS NOT NULL
),
chave_unica AS (
  SELECT
    company_unit_key,
    part_key,
    available_qtt,
    minimum_qtt,
    unit_cost,
    stock_value,
    below_minimum,
    updated_at
  FROM (
    SELECT *, row_number() OVER (PARTITION BY company_unit_key, part_key ORDER BY updated_at DESC NULLS LAST) AS _linha
    FROM com_chave
  )
  WHERE _linha = 1
),
com_pai AS (
  SELECT * FROM chave_unica
  WHERE (company_unit_key IS NULL OR company_unit_key IN (SELECT company_unit_key FROM workspace.cleaned.dim_company_unit))
    AND (part_key IS NULL OR part_key IN (SELECT part_key FROM workspace.cleaned.dim_part))
)
SELECT * FROM com_pai;
