-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Limpeza: dim_employee
-- MAGIC Refaz `workspace.cleaned.dim_employee` a partir de `workspace.raw.dim_employee`: padroniza textos, zera valores inválidos e remove linhas duplicadas, sem chave ou sem pai. Fonte da pipeline `Apollo_BI`.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW workspace.cleaned.dim_employee
COMMENT 'Funcionários tratadas a partir da camada raw'
AS
WITH
lida AS (
  SELECT * FROM workspace.raw.dim_employee
),
aparada AS (
  SELECT
    trim(regexp_replace(employee_key, '\\s+', ' ')) AS employee_key,
    trim(regexp_replace(company_unit_key, '\\s+', ' ')) AS company_unit_key,
    trim(regexp_replace(job_role, '\\s+', ' ')) AS job_role,
    is_active
  FROM lida
),
sem_nulo_falso AS (
  SELECT
    CASE WHEN lower(employee_key) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE employee_key END AS employee_key,
    CASE WHEN lower(company_unit_key) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE company_unit_key END AS company_unit_key,
    CASE WHEN lower(job_role) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE job_role END AS job_role,
    is_active
  FROM aparada
),
sem_duplicadas AS (
  SELECT DISTINCT * FROM sem_nulo_falso
),
com_chave AS (
  SELECT * FROM sem_duplicadas
  WHERE employee_key IS NOT NULL
),
chave_unica AS (
  SELECT
    employee_key,
    company_unit_key,
    job_role,
    is_active
  FROM (
    SELECT *, row_number() OVER (PARTITION BY employee_key ORDER BY employee_key) AS _linha
    FROM com_chave
  )
  WHERE _linha = 1
)
SELECT * FROM chave_unica;
