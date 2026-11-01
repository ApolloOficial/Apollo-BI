-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Limpeza: fact_daily_active_user
-- MAGIC Refaz `workspace.cleaned.fact_daily_active_user` a partir de `workspace.raw.fact_daily_active_user`: padroniza textos, zera valores inválidos e remove linhas duplicadas, sem chave ou sem pai. Fonte da pipeline `Apollo_BI`.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW workspace.cleaned.fact_daily_active_user
COMMENT 'Usuários ativos por dia tratadas a partir da camada raw'
AS
WITH
lida AS (
  SELECT * FROM workspace.raw.fact_daily_active_user
),
datas_validas AS (
  SELECT
    company_key,
    date_key,
    CASE WHEN access_date > current_timestamp() + INTERVAL 1 DAY OR access_date < DATE '2000-01-01' THEN NULL ELSE access_date END AS access_date,
    daily_active_users,
    total_sessions,
    rolling_7d_active_user_sum
  FROM lida
),
sem_duplicadas AS (
  SELECT DISTINCT * FROM datas_validas
),
com_chave AS (
  SELECT * FROM sem_duplicadas
  WHERE company_key IS NOT NULL AND date_key IS NOT NULL
),
chave_unica AS (
  SELECT
    company_key,
    date_key,
    access_date,
    daily_active_users,
    total_sessions,
    rolling_7d_active_user_sum
  FROM (
    SELECT *, row_number() OVER (PARTITION BY company_key, date_key ORDER BY company_key, date_key) AS _linha
    FROM com_chave
  )
  WHERE _linha = 1
)
SELECT * FROM chave_unica;
