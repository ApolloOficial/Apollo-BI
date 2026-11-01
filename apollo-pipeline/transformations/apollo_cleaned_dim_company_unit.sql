-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Limpeza: dim_company_unit
-- MAGIC Refaz `workspace.cleaned.dim_company_unit` a partir de `workspace.raw.dim_company_unit`: padroniza textos, zera valores inválidos e remove linhas duplicadas, sem chave ou sem pai. Fonte da pipeline `Apollo_BI`.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW workspace.cleaned.dim_company_unit
COMMENT 'Filiais tratadas a partir da camada raw'
AS
WITH
lida AS (
  SELECT * FROM workspace.raw.dim_company_unit
),
aparada AS (
  SELECT
    trim(regexp_replace(company_unit_key, '\\s+', ' ')) AS company_unit_key,
    company_key,
    segment_key,
    trim(regexp_replace(unit_name, '\\s+', ' ')) AS unit_name,
    trim(regexp_replace(city, '\\s+', ' ')) AS city,
    trim(regexp_replace(state, '\\s+', ' ')) AS state,
    kwp_total,
    is_active
  FROM lida
),
sem_nulo_falso AS (
  SELECT
    CASE WHEN lower(company_unit_key) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE company_unit_key END AS company_unit_key,
    company_key,
    segment_key,
    CASE WHEN lower(unit_name) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE unit_name END AS unit_name,
    CASE WHEN lower(city) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE city END AS city,
    CASE WHEN lower(state) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE state END AS state,
    kwp_total,
    is_active
  FROM aparada
),
categorias AS (
  SELECT
    company_unit_key,
    company_key,
    segment_key,
    unit_name,
    city,
    upper(state) AS state,
    kwp_total,
    is_active
  FROM sem_nulo_falso
),
formatos AS (
  SELECT
    company_unit_key,
    company_key,
    segment_key,
    unit_name,
    CASE WHEN city LIKE '%?%' THEN NULL ELSE city END AS city,
    CASE WHEN state IN ('AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO') THEN state END AS state,
    kwp_total,
    is_active
  FROM categorias
),
sem_negativos AS (
  SELECT
    company_unit_key,
    company_key,
    segment_key,
    unit_name,
    city,
    state,
    CASE WHEN kwp_total < 0 THEN NULL ELSE kwp_total END AS kwp_total,
    is_active
  FROM formatos
),
sem_duplicadas AS (
  SELECT DISTINCT * FROM sem_negativos
),
com_chave AS (
  SELECT * FROM sem_duplicadas
  WHERE company_unit_key IS NOT NULL
),
chave_unica AS (
  SELECT
    company_unit_key,
    company_key,
    segment_key,
    unit_name,
    city,
    state,
    kwp_total,
    is_active
  FROM (
    SELECT *, row_number() OVER (PARTITION BY company_unit_key ORDER BY company_unit_key) AS _linha
    FROM com_chave
  )
  WHERE _linha = 1
),
com_pai AS (
  SELECT * FROM chave_unica
  WHERE (company_key IS NULL OR company_key IN (SELECT company_key FROM workspace.cleaned.dim_company))
)
SELECT * FROM com_pai;
