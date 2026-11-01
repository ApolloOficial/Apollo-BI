-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Limpeza: fact_part_consumption
-- MAGIC Refaz `workspace.cleaned.fact_part_consumption` a partir de `workspace.raw.fact_part_consumption`: padroniza textos, zera valores inválidos e remove linhas duplicadas, sem chave ou sem pai. Fonte da pipeline `Apollo_BI`.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW workspace.cleaned.fact_part_consumption
COMMENT 'Consumo de peças tratadas a partir da camada raw'
AS
WITH
lida AS (
  SELECT * FROM workspace.raw.fact_part_consumption
),
aparada AS (
  SELECT
    date_key,
    maintenance_key,
    part_key,
    trim(regexp_replace(company_unit_key, '\\s+', ' ')) AS company_unit_key,
    quantity,
    unit_cost,
    total_cost,
    running_quantity
  FROM lida
),
sem_nulo_falso AS (
  SELECT
    date_key,
    maintenance_key,
    part_key,
    CASE WHEN lower(company_unit_key) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE company_unit_key END AS company_unit_key,
    quantity,
    unit_cost,
    total_cost,
    running_quantity
  FROM aparada
),
sem_negativos AS (
  SELECT
    date_key,
    maintenance_key,
    part_key,
    company_unit_key,
    CASE WHEN quantity < 0 THEN NULL ELSE quantity END AS quantity,
    CASE WHEN unit_cost < 0 THEN NULL ELSE unit_cost END AS unit_cost,
    CASE WHEN total_cost < 0 THEN NULL ELSE total_cost END AS total_cost,
    CASE WHEN running_quantity < 0 THEN NULL ELSE running_quantity END AS running_quantity
  FROM sem_nulo_falso
),
sem_duplicadas AS (
  SELECT DISTINCT * FROM sem_negativos
),
com_chave AS (
  SELECT * FROM sem_duplicadas
  WHERE maintenance_key IS NOT NULL AND part_key IS NOT NULL
),
chave_unica AS (
  SELECT
    date_key,
    maintenance_key,
    part_key,
    company_unit_key,
    quantity,
    unit_cost,
    total_cost,
    running_quantity
  FROM (
    SELECT *, row_number() OVER (PARTITION BY maintenance_key, part_key ORDER BY maintenance_key, part_key) AS _linha
    FROM com_chave
  )
  WHERE _linha = 1
)
SELECT * FROM chave_unica;
