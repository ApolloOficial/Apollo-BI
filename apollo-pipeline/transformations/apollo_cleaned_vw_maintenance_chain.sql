-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Limpeza: vw_maintenance_chain
-- MAGIC Refaz `workspace.cleaned.vw_maintenance_chain` a partir de `workspace.raw.vw_maintenance_chain`: padroniza textos, zera valores inválidos e remove linhas duplicadas, sem chave ou sem pai. Fonte da pipeline `Apollo_BI`.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW workspace.cleaned.vw_maintenance_chain
COMMENT 'Cadeia de ordens de serviço tratadas a partir da camada raw'
AS
WITH
lida AS (
  SELECT * FROM workspace.raw.vw_maintenance_chain
),
aparada AS (
  SELECT
    id,
    parent_maintenance_id,
    warning_id,
    trim(regexp_replace(maintenance_status, '\\s+', ' ')) AS maintenance_status,
    opening_dt,
    chain_level,
    path,
    trim(regexp_replace(string_id, '\\s+', ' ')) AS string_id
  FROM lida
),
sem_nulo_falso AS (
  SELECT
    id,
    parent_maintenance_id,
    warning_id,
    CASE WHEN lower(maintenance_status) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE maintenance_status END AS maintenance_status,
    opening_dt,
    chain_level,
    path,
    CASE WHEN lower(string_id) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE string_id END AS string_id
  FROM aparada
),
categorias AS (
  SELECT
    id,
    parent_maintenance_id,
    warning_id,
    regexp_replace(translate(upper(maintenance_status), 'ÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ', 'AAAAAEEEEIIIIOOOOOUUUUC'), '[\\s\\-]+', '_') AS maintenance_status,
    opening_dt,
    chain_level,
    path,
    string_id
  FROM sem_nulo_falso
),
datas_validas AS (
  SELECT
    id,
    parent_maintenance_id,
    warning_id,
    maintenance_status,
    CASE WHEN opening_dt > current_timestamp() + INTERVAL 1 DAY OR opening_dt < DATE '2000-01-01' THEN NULL ELSE opening_dt END AS opening_dt,
    chain_level,
    path,
    string_id
  FROM categorias
),
sem_duplicadas AS (
  SELECT DISTINCT * FROM datas_validas
),
com_chave AS (
  SELECT * FROM sem_duplicadas
  WHERE id IS NOT NULL
),
chave_unica AS (
  SELECT
    id,
    parent_maintenance_id,
    warning_id,
    maintenance_status,
    opening_dt,
    chain_level,
    path,
    string_id
  FROM (
    SELECT *, row_number() OVER (PARTITION BY id ORDER BY id) AS _linha
    FROM com_chave
  )
  WHERE _linha = 1
)
SELECT * FROM chave_unica;
