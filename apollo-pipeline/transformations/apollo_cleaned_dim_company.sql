-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Limpeza: dim_company
-- MAGIC Refaz `workspace.cleaned.dim_company` a partir de `workspace.raw.dim_company`: padroniza textos, zera valores inválidos e remove linhas duplicadas, sem chave ou sem pai. Fonte da pipeline `Apollo_BI`.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW workspace.cleaned.dim_company
COMMENT 'Empresas tratadas a partir da camada raw'
AS
WITH
lida AS (
  SELECT * FROM workspace.raw.dim_company
),
aparada AS (
  SELECT
    company_key,
    trim(regexp_replace(company_name, '\\s+', ' ')) AS company_name,
    trim(regexp_replace(cnpj, '\\s+', ' ')) AS cnpj,
    is_active,
    created_at
  FROM lida
),
sem_nulo_falso AS (
  SELECT
    company_key,
    CASE WHEN lower(company_name) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE company_name END AS company_name,
    CASE WHEN lower(cnpj) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE cnpj END AS cnpj,
    is_active,
    created_at
  FROM aparada
),
formatos AS (
  SELECT
    company_key,
    company_name,
    CASE WHEN cnpj RLIKE '^[0-9]{14}$' AND NOT cnpj RLIKE '^(.)\\1{13}$' THEN cnpj END AS cnpj,
    is_active,
    created_at
  FROM sem_nulo_falso
),
datas_validas AS (
  SELECT
    company_key,
    company_name,
    cnpj,
    is_active,
    CASE WHEN created_at > current_timestamp() + INTERVAL 1 DAY OR created_at < DATE '2000-01-01' THEN NULL ELSE created_at END AS created_at
  FROM formatos
),
sem_duplicadas AS (
  SELECT DISTINCT * FROM datas_validas
),
com_chave AS (
  SELECT * FROM sem_duplicadas
  WHERE company_key IS NOT NULL
),
chave_unica AS (
  SELECT
    company_key,
    company_name,
    cnpj,
    is_active,
    created_at
  FROM (
    SELECT *, row_number() OVER (PARTITION BY company_key ORDER BY company_key) AS _linha
    FROM com_chave
  )
  WHERE _linha = 1
)
SELECT * FROM chave_unica;
