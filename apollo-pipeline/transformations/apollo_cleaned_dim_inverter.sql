-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Limpeza: dim_inverter
-- MAGIC Refaz `workspace.cleaned.dim_inverter` a partir de `workspace.raw.dim_inverter`: padroniza textos, zera valores inválidos e remove linhas duplicadas, sem chave ou sem pai. Fonte da pipeline `Apollo_BI`.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW workspace.cleaned.dim_inverter
COMMENT 'Inversores tratadas a partir da camada raw'
AS
WITH
lida AS (
  SELECT * FROM workspace.raw.dim_inverter
),
aparada AS (
  SELECT
    trim(regexp_replace(inverter_key, '\\s+', ' ')) AS inverter_key,
    trim(regexp_replace(company_unit_key, '\\s+', ' ')) AS company_unit_key,
    trim(regexp_replace(code, '\\s+', ' ')) AS code,
    trim(regexp_replace(brand, '\\s+', ' ')) AS brand,
    trim(regexp_replace(model, '\\s+', ' ')) AS model,
    rated_power_kw,
    mppt_count,
    strings_per_mppt,
    trim(regexp_replace(status, '\\s+', ' ')) AS status,
    installed_at
  FROM lida
),
sem_nulo_falso AS (
  SELECT
    CASE WHEN lower(inverter_key) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE inverter_key END AS inverter_key,
    CASE WHEN lower(company_unit_key) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE company_unit_key END AS company_unit_key,
    CASE WHEN lower(code) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE code END AS code,
    CASE WHEN lower(brand) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE brand END AS brand,
    CASE WHEN lower(model) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE model END AS model,
    rated_power_kw,
    mppt_count,
    strings_per_mppt,
    CASE WHEN lower(status) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE status END AS status,
    installed_at
  FROM aparada
),
categorias AS (
  SELECT
    inverter_key,
    company_unit_key,
    code,
    brand,
    model,
    rated_power_kw,
    mppt_count,
    strings_per_mppt,
    regexp_replace(translate(upper(status), 'ÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ', 'AAAAAEEEEIIIIOOOOOUUUUC'), '[\\s\\-]+', '_') AS status,
    installed_at
  FROM sem_nulo_falso
),
sem_negativos AS (
  SELECT
    inverter_key,
    company_unit_key,
    code,
    brand,
    model,
    CASE WHEN rated_power_kw < 0 THEN NULL ELSE rated_power_kw END AS rated_power_kw,
    CASE WHEN mppt_count < 0 THEN NULL ELSE mppt_count END AS mppt_count,
    strings_per_mppt,
    status,
    installed_at
  FROM categorias
),
datas_validas AS (
  SELECT
    inverter_key,
    company_unit_key,
    code,
    brand,
    model,
    rated_power_kw,
    mppt_count,
    strings_per_mppt,
    status,
    CASE WHEN installed_at > current_timestamp() + INTERVAL 1 DAY OR installed_at < DATE '2000-01-01' THEN NULL ELSE installed_at END AS installed_at
  FROM sem_negativos
),
sem_duplicadas AS (
  SELECT DISTINCT * FROM datas_validas
),
com_chave AS (
  SELECT * FROM sem_duplicadas
  WHERE inverter_key IS NOT NULL
),
chave_unica AS (
  SELECT
    inverter_key,
    company_unit_key,
    code,
    brand,
    model,
    rated_power_kw,
    mppt_count,
    strings_per_mppt,
    status,
    installed_at
  FROM (
    SELECT *, row_number() OVER (PARTITION BY inverter_key ORDER BY inverter_key) AS _linha
    FROM com_chave
  )
  WHERE _linha = 1
),
com_pai AS (
  SELECT * FROM chave_unica
  WHERE (company_unit_key IS NULL OR company_unit_key IN (SELECT company_unit_key FROM workspace.cleaned.dim_company_unit))
)
SELECT * FROM com_pai;
