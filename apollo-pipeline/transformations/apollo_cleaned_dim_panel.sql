-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Limpeza: dim_panel
-- MAGIC Refaz `workspace.cleaned.dim_panel` a partir de `workspace.raw.dim_panel`: padroniza textos, zera valores inválidos e remove linhas duplicadas, sem chave ou sem pai. Fonte da pipeline `Apollo_BI`.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW workspace.cleaned.dim_panel
COMMENT 'Placas tratadas a partir da camada raw'
AS
WITH
lida AS (
  SELECT * FROM workspace.raw.dim_panel
),
aparada AS (
  SELECT
    panel_key,
    trim(regexp_replace(string_key, '\\s+', ' ')) AS string_key,
    trim(regexp_replace(serial_number, '\\s+', ' ')) AS serial_number,
    trim(regexp_replace(barcode, '\\s+', ' ')) AS barcode,
    trim(regexp_replace(operating_stats, '\\s+', ' ')) AS operating_stats,
    installation_dt,
    deactivated_at
  FROM lida
),
sem_nulo_falso AS (
  SELECT
    panel_key,
    CASE WHEN lower(string_key) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE string_key END AS string_key,
    CASE WHEN lower(serial_number) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE serial_number END AS serial_number,
    CASE WHEN lower(barcode) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE barcode END AS barcode,
    CASE WHEN lower(operating_stats) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE operating_stats END AS operating_stats,
    installation_dt,
    deactivated_at
  FROM aparada
),
categorias AS (
  SELECT
    panel_key,
    string_key,
    serial_number,
    barcode,
    regexp_replace(translate(upper(operating_stats), 'ÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ', 'AAAAAEEEEIIIIOOOOOUUUUC'), '[\\s\\-]+', '_') AS operating_stats,
    installation_dt,
    deactivated_at
  FROM sem_nulo_falso
),
dominios AS (
  SELECT
    panel_key,
    string_key,
    serial_number,
    barcode,
    CASE WHEN operating_stats IN ('EM_ESTOQUE', 'OPERACIONAL', 'MANUTENCAO', 'INATIVO', 'FALHA') THEN operating_stats END AS operating_stats,
    installation_dt,
    deactivated_at
  FROM categorias
),
datas_validas AS (
  SELECT
    panel_key,
    string_key,
    serial_number,
    barcode,
    operating_stats,
    CASE WHEN installation_dt > current_timestamp() + INTERVAL 1 DAY OR installation_dt < DATE '2000-01-01' THEN NULL ELSE installation_dt END AS installation_dt,
    CASE WHEN deactivated_at > current_timestamp() + INTERVAL 1 DAY OR deactivated_at < DATE '2000-01-01' THEN NULL ELSE deactivated_at END AS deactivated_at
  FROM dominios
),
cronologia AS (
  SELECT
    panel_key,
    string_key,
    serial_number,
    barcode,
    operating_stats,
    CASE WHEN deactivated_at < installation_dt THEN NULL ELSE installation_dt END AS installation_dt,
    deactivated_at
  FROM datas_validas
),
sem_duplicadas AS (
  SELECT DISTINCT * FROM cronologia
),
com_chave AS (
  SELECT * FROM sem_duplicadas
  WHERE panel_key IS NOT NULL
),
chave_unica AS (
  SELECT
    panel_key,
    string_key,
    serial_number,
    barcode,
    operating_stats,
    installation_dt,
    deactivated_at
  FROM (
    SELECT *, row_number() OVER (PARTITION BY panel_key ORDER BY panel_key) AS _linha
    FROM com_chave
  )
  WHERE _linha = 1
),
com_pai AS (
  SELECT * FROM chave_unica
  WHERE (string_key IS NULL OR string_key IN (SELECT string_key FROM workspace.cleaned.dim_string))
)
SELECT * FROM com_pai;
