-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Limpeza: dim_panel_model
-- MAGIC Refaz `workspace.cleaned.dim_panel_model` a partir de `workspace.raw.dim_panel_model`: padroniza textos, zera valores inválidos e remove linhas duplicadas, sem chave ou sem pai. Fonte da pipeline `Apollo_BI`.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW workspace.cleaned.dim_panel_model
COMMENT 'Modelos de placa tratadas a partir da camada raw'
AS
WITH
lida AS (
  SELECT * FROM workspace.raw.dim_panel_model
),
aparada AS (
  SELECT
    panel_model_key,
    trim(regexp_replace(manufacturer, '\\s+', ' ')) AS manufacturer,
    trim(regexp_replace(model, '\\s+', ' ')) AS model,
    pmax_w,
    efficiency_pct,
    warranty_years
  FROM lida
),
sem_nulo_falso AS (
  SELECT
    panel_model_key,
    CASE WHEN lower(manufacturer) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE manufacturer END AS manufacturer,
    CASE WHEN lower(model) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE model END AS model,
    pmax_w,
    efficiency_pct,
    warranty_years
  FROM aparada
),
sem_negativos AS (
  SELECT
    panel_model_key,
    manufacturer,
    model,
    pmax_w,
    efficiency_pct,
    CASE WHEN warranty_years < 0 THEN NULL ELSE warranty_years END AS warranty_years
  FROM sem_nulo_falso
),
na_faixa AS (
  SELECT
    panel_model_key,
    manufacturer,
    model,
    pmax_w,
    efficiency_pct,
    CASE WHEN warranty_years BETWEEN 0 AND 50 THEN warranty_years END AS warranty_years
  FROM sem_negativos
),
sem_duplicadas AS (
  SELECT DISTINCT * FROM na_faixa
),
com_chave AS (
  SELECT * FROM sem_duplicadas
  WHERE panel_model_key IS NOT NULL
),
chave_unica AS (
  SELECT
    panel_model_key,
    manufacturer,
    model,
    pmax_w,
    efficiency_pct,
    warranty_years
  FROM (
    SELECT *, row_number() OVER (PARTITION BY panel_model_key ORDER BY panel_model_key) AS _linha
    FROM com_chave
  )
  WHERE _linha = 1
)
SELECT * FROM chave_unica;
