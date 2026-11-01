-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Limpeza: dim_string
-- MAGIC Refaz `workspace.cleaned.dim_string` a partir de `workspace.raw.dim_string`: padroniza textos, zera valores inválidos e remove linhas duplicadas, sem chave ou sem pai. Fonte da pipeline `Apollo_BI`.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW workspace.cleaned.dim_string
COMMENT 'Strings tratadas a partir da camada raw'
AS
WITH
lida AS (
  SELECT * FROM workspace.raw.dim_string
),
aparada AS (
  SELECT
    trim(regexp_replace(string_key, '\\s+', ' ')) AS string_key,
    trim(regexp_replace(inverter_key, '\\s+', ' ')) AS inverter_key,
    panel_model_key,
    trim(regexp_replace(code, '\\s+', ' ')) AS code,
    mppt_number,
    entry_number,
    trim(regexp_replace(invoice_number, '\\s+', ' ')) AS invoice_number,
    acquisition_dt,
    unit_cost,
    purchased_qty,
    installed_panels,
    nominal_power_wp,
    trim(regexp_replace(status, '\\s+', ' ')) AS status
  FROM lida
),
sem_nulo_falso AS (
  SELECT
    CASE WHEN lower(string_key) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE string_key END AS string_key,
    CASE WHEN lower(inverter_key) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE inverter_key END AS inverter_key,
    panel_model_key,
    CASE WHEN lower(code) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE code END AS code,
    mppt_number,
    entry_number,
    CASE WHEN lower(invoice_number) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE invoice_number END AS invoice_number,
    acquisition_dt,
    unit_cost,
    purchased_qty,
    installed_panels,
    nominal_power_wp,
    CASE WHEN lower(status) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE status END AS status
  FROM aparada
),
categorias AS (
  SELECT
    string_key,
    inverter_key,
    panel_model_key,
    code,
    mppt_number,
    entry_number,
    invoice_number,
    acquisition_dt,
    unit_cost,
    purchased_qty,
    installed_panels,
    nominal_power_wp,
    regexp_replace(translate(upper(status), 'ÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ', 'AAAAAEEEEIIIIOOOOOUUUUC'), '[\\s\\-]+', '_') AS status
  FROM sem_nulo_falso
),
sem_negativos AS (
  SELECT
    string_key,
    inverter_key,
    panel_model_key,
    code,
    mppt_number,
    entry_number,
    invoice_number,
    acquisition_dt,
    CASE WHEN unit_cost < 0 THEN NULL ELSE unit_cost END AS unit_cost,
    purchased_qty,
    installed_panels,
    CASE WHEN nominal_power_wp < 0 THEN NULL ELSE nominal_power_wp END AS nominal_power_wp,
    status
  FROM categorias
),
datas_validas AS (
  SELECT
    string_key,
    inverter_key,
    panel_model_key,
    code,
    mppt_number,
    entry_number,
    invoice_number,
    CASE WHEN acquisition_dt > current_timestamp() + INTERVAL 1 DAY OR acquisition_dt < DATE '2000-01-01' THEN NULL ELSE acquisition_dt END AS acquisition_dt,
    unit_cost,
    purchased_qty,
    installed_panels,
    nominal_power_wp,
    status
  FROM sem_negativos
),
sem_duplicadas AS (
  SELECT DISTINCT * FROM datas_validas
),
com_chave AS (
  SELECT * FROM sem_duplicadas
  WHERE string_key IS NOT NULL
),
chave_unica AS (
  SELECT
    string_key,
    inverter_key,
    panel_model_key,
    code,
    mppt_number,
    entry_number,
    invoice_number,
    acquisition_dt,
    unit_cost,
    purchased_qty,
    installed_panels,
    nominal_power_wp,
    status
  FROM (
    SELECT *, row_number() OVER (PARTITION BY string_key ORDER BY string_key) AS _linha
    FROM com_chave
  )
  WHERE _linha = 1
),
com_pai AS (
  SELECT * FROM chave_unica
  WHERE (inverter_key IS NULL OR inverter_key IN (SELECT inverter_key FROM workspace.cleaned.dim_inverter))
    AND (panel_model_key IS NULL OR panel_model_key IN (SELECT panel_model_key FROM workspace.cleaned.dim_panel_model))
)
SELECT * FROM com_pai;
