-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Limpeza: fact_warning
-- MAGIC Refaz `workspace.cleaned.fact_warning` a partir de `workspace.raw.fact_warning`: padroniza textos, zera valores inválidos e remove linhas duplicadas, sem chave ou sem pai. Fonte da pipeline `Apollo_BI`.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW workspace.cleaned.fact_warning
COMMENT 'Alertas tratadas a partir da camada raw'
AS
WITH
lida AS (
  SELECT * FROM workspace.raw.fact_warning
),
aparada AS (
  SELECT
    date_key,
    warning_key,
    trim(regexp_replace(company_unit_key, '\\s+', ' ')) AS company_unit_key,
    trim(regexp_replace(inverter_key, '\\s+', ' ')) AS inverter_key,
    trim(regexp_replace(string_key, '\\s+', ' ')) AS string_key,
    panel_key,
    trim(regexp_replace(warning_type, '\\s+', ' ')) AS warning_type,
    trim(regexp_replace(severity, '\\s+', ' ')) AS severity,
    trim(regexp_replace(status, '\\s+', ' ')) AS status,
    generation_dt,
    resolved_at,
    maintenance_key,
    trim(regexp_replace(maintenance_status, '\\s+', ' ')) AS maintenance_status,
    maintenance_opening_dt,
    hours_to_maintenance,
    resolution_days
  FROM lida
),
sem_nulo_falso AS (
  SELECT
    date_key,
    warning_key,
    CASE WHEN lower(company_unit_key) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE company_unit_key END AS company_unit_key,
    CASE WHEN lower(inverter_key) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE inverter_key END AS inverter_key,
    CASE WHEN lower(string_key) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE string_key END AS string_key,
    panel_key,
    CASE WHEN lower(warning_type) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE warning_type END AS warning_type,
    CASE WHEN lower(severity) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE severity END AS severity,
    CASE WHEN lower(status) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE status END AS status,
    generation_dt,
    resolved_at,
    maintenance_key,
    CASE WHEN lower(maintenance_status) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE maintenance_status END AS maintenance_status,
    maintenance_opening_dt,
    hours_to_maintenance,
    resolution_days
  FROM aparada
),
categorias AS (
  SELECT
    date_key,
    warning_key,
    company_unit_key,
    inverter_key,
    string_key,
    panel_key,
    regexp_replace(translate(upper(warning_type), 'ÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ', 'AAAAAEEEEIIIIOOOOOUUUUC'), '[\\s\\-]+', '_') AS warning_type,
    regexp_replace(translate(upper(severity), 'ÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ', 'AAAAAEEEEIIIIOOOOOUUUUC'), '[\\s\\-]+', '_') AS severity,
    regexp_replace(translate(upper(status), 'ÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ', 'AAAAAEEEEIIIIOOOOOUUUUC'), '[\\s\\-]+', '_') AS status,
    generation_dt,
    resolved_at,
    maintenance_key,
    regexp_replace(translate(upper(maintenance_status), 'ÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ', 'AAAAAEEEEIIIIOOOOOUUUUC'), '[\\s\\-]+', '_') AS maintenance_status,
    maintenance_opening_dt,
    hours_to_maintenance,
    resolution_days
  FROM sem_nulo_falso
),
dominios AS (
  SELECT
    date_key,
    warning_key,
    company_unit_key,
    inverter_key,
    string_key,
    panel_key,
    warning_type,
    severity,
    CASE WHEN status IN ('ATIVO', 'RECONHECIDO', 'RESOLVIDO') THEN status END AS status,
    generation_dt,
    resolved_at,
    maintenance_key,
    CASE WHEN maintenance_status IN ('ABERTA', 'EM_ANDAMENTO', 'CONCLUIDA', 'CANCELADA') THEN maintenance_status END AS maintenance_status,
    maintenance_opening_dt,
    hours_to_maintenance,
    resolution_days
  FROM categorias
),
sem_negativos AS (
  SELECT
    date_key,
    warning_key,
    company_unit_key,
    inverter_key,
    string_key,
    panel_key,
    warning_type,
    severity,
    status,
    generation_dt,
    resolved_at,
    maintenance_key,
    maintenance_status,
    maintenance_opening_dt,
    CASE WHEN hours_to_maintenance < 0 THEN NULL ELSE hours_to_maintenance END AS hours_to_maintenance,
    CASE WHEN resolution_days < 0 THEN NULL ELSE resolution_days END AS resolution_days
  FROM dominios
),
datas_validas AS (
  SELECT
    date_key,
    warning_key,
    company_unit_key,
    inverter_key,
    string_key,
    panel_key,
    warning_type,
    severity,
    status,
    CASE WHEN generation_dt > current_timestamp() + INTERVAL 1 DAY OR generation_dt < DATE '2000-01-01' THEN NULL ELSE generation_dt END AS generation_dt,
    CASE WHEN resolved_at > current_timestamp() + INTERVAL 1 DAY OR resolved_at < DATE '2000-01-01' THEN NULL ELSE resolved_at END AS resolved_at,
    maintenance_key,
    maintenance_status,
    CASE WHEN maintenance_opening_dt > current_timestamp() + INTERVAL 1 DAY OR maintenance_opening_dt < DATE '2000-01-01' THEN NULL ELSE maintenance_opening_dt END AS maintenance_opening_dt,
    hours_to_maintenance,
    resolution_days
  FROM sem_negativos
),
sem_duplicadas AS (
  SELECT DISTINCT * FROM datas_validas
),
com_chave AS (
  SELECT * FROM sem_duplicadas
  WHERE warning_key IS NOT NULL
),
chave_unica AS (
  SELECT
    date_key,
    warning_key,
    company_unit_key,
    inverter_key,
    string_key,
    panel_key,
    warning_type,
    severity,
    status,
    generation_dt,
    resolved_at,
    maintenance_key,
    maintenance_status,
    maintenance_opening_dt,
    hours_to_maintenance,
    resolution_days
  FROM (
    SELECT *, row_number() OVER (PARTITION BY warning_key ORDER BY warning_key) AS _linha
    FROM com_chave
  )
  WHERE _linha = 1
),
com_pai AS (
  SELECT * FROM chave_unica
  WHERE (company_unit_key IS NULL OR company_unit_key IN (SELECT company_unit_key FROM workspace.cleaned.dim_company_unit))
)
SELECT * FROM com_pai;
