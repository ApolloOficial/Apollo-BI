-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Limpeza: fact_maintenance
-- MAGIC Refaz `workspace.cleaned.fact_maintenance` a partir de `workspace.raw.fact_maintenance`: padroniza textos, zera valores inválidos e remove linhas duplicadas, sem chave ou sem pai. Fonte da pipeline `Apollo_BI`.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW workspace.cleaned.fact_maintenance
COMMENT 'Ordens de serviço tratadas a partir da camada raw'
AS
WITH
lida AS (
  SELECT * FROM workspace.raw.fact_maintenance
),
aparada AS (
  SELECT
    opening_date_key,
    maintenance_key,
    trim(regexp_replace(company_unit_key, '\\s+', ' ')) AS company_unit_key,
    trim(regexp_replace(string_key, '\\s+', ' ')) AS string_key,
    warning_key,
    trim(regexp_replace(warning_type, '\\s+', ' ')) AS warning_type,
    trim(regexp_replace(priority, '\\s+', ' ')) AS priority,
    maintenance_type_key,
    trim(regexp_replace(technician_key, '\\s+', ' ')) AS technician_key,
    trim(regexp_replace(created_by_key, '\\s+', ' ')) AS created_by_key,
    trim(regexp_replace(cancelled_by_key, '\\s+', ' ')) AS cancelled_by_key,
    opening_dt,
    due_date,
    concluded_at,
    cancelled_at,
    trim(regexp_replace(maintenance_status, '\\s+', ' ')) AS maintenance_status,
    trim(regexp_replace(calculated_status, '\\s+', ' ')) AS calculated_status,
    estimated_cost,
    labor_cost,
    parts_cost,
    total_cost,
    resolution_days,
    unit_avg_resolution_days,
    unit_resolution_rank,
    unit_running_maintenance_cost
  FROM lida
),
sem_nulo_falso AS (
  SELECT
    opening_date_key,
    maintenance_key,
    CASE WHEN lower(company_unit_key) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE company_unit_key END AS company_unit_key,
    CASE WHEN lower(string_key) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE string_key END AS string_key,
    warning_key,
    CASE WHEN lower(warning_type) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE warning_type END AS warning_type,
    CASE WHEN lower(priority) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE priority END AS priority,
    maintenance_type_key,
    CASE WHEN lower(technician_key) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE technician_key END AS technician_key,
    CASE WHEN lower(created_by_key) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE created_by_key END AS created_by_key,
    CASE WHEN lower(cancelled_by_key) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE cancelled_by_key END AS cancelled_by_key,
    opening_dt,
    due_date,
    concluded_at,
    cancelled_at,
    CASE WHEN lower(maintenance_status) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE maintenance_status END AS maintenance_status,
    CASE WHEN lower(calculated_status) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE calculated_status END AS calculated_status,
    estimated_cost,
    labor_cost,
    parts_cost,
    total_cost,
    resolution_days,
    unit_avg_resolution_days,
    unit_resolution_rank,
    unit_running_maintenance_cost
  FROM aparada
),
categorias AS (
  SELECT
    opening_date_key,
    maintenance_key,
    company_unit_key,
    string_key,
    warning_key,
    regexp_replace(translate(upper(warning_type), 'ÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ', 'AAAAAEEEEIIIIOOOOOUUUUC'), '[\\s\\-]+', '_') AS warning_type,
    priority,
    maintenance_type_key,
    technician_key,
    created_by_key,
    cancelled_by_key,
    opening_dt,
    due_date,
    concluded_at,
    cancelled_at,
    regexp_replace(translate(upper(maintenance_status), 'ÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ', 'AAAAAEEEEIIIIOOOOOUUUUC'), '[\\s\\-]+', '_') AS maintenance_status,
    regexp_replace(translate(upper(calculated_status), 'ÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ', 'AAAAAEEEEIIIIOOOOOUUUUC'), '[\\s\\-]+', '_') AS calculated_status,
    estimated_cost,
    labor_cost,
    parts_cost,
    total_cost,
    resolution_days,
    unit_avg_resolution_days,
    unit_resolution_rank,
    unit_running_maintenance_cost
  FROM sem_nulo_falso
),
dominios AS (
  SELECT
    opening_date_key,
    maintenance_key,
    company_unit_key,
    string_key,
    warning_key,
    warning_type,
    priority,
    maintenance_type_key,
    technician_key,
    created_by_key,
    cancelled_by_key,
    opening_dt,
    due_date,
    concluded_at,
    cancelled_at,
    CASE WHEN maintenance_status IN ('ABERTA', 'EM_ANDAMENTO', 'CONCLUIDA', 'CANCELADA') THEN maintenance_status END AS maintenance_status,
    calculated_status,
    estimated_cost,
    labor_cost,
    parts_cost,
    total_cost,
    resolution_days,
    unit_avg_resolution_days,
    unit_resolution_rank,
    unit_running_maintenance_cost
  FROM categorias
),
sem_negativos AS (
  SELECT
    opening_date_key,
    maintenance_key,
    company_unit_key,
    string_key,
    warning_key,
    warning_type,
    priority,
    maintenance_type_key,
    technician_key,
    created_by_key,
    cancelled_by_key,
    opening_dt,
    due_date,
    concluded_at,
    cancelled_at,
    maintenance_status,
    calculated_status,
    CASE WHEN estimated_cost < 0 THEN NULL ELSE estimated_cost END AS estimated_cost,
    CASE WHEN labor_cost < 0 THEN NULL ELSE labor_cost END AS labor_cost,
    CASE WHEN parts_cost < 0 THEN NULL ELSE parts_cost END AS parts_cost,
    CASE WHEN total_cost < 0 THEN NULL ELSE total_cost END AS total_cost,
    CASE WHEN resolution_days < 0 THEN NULL ELSE resolution_days END AS resolution_days,
    CASE WHEN unit_avg_resolution_days < 0 THEN NULL ELSE unit_avg_resolution_days END AS unit_avg_resolution_days,
    unit_resolution_rank,
    CASE WHEN unit_running_maintenance_cost < 0 THEN NULL ELSE unit_running_maintenance_cost END AS unit_running_maintenance_cost
  FROM dominios
),
datas_validas AS (
  SELECT
    opening_date_key,
    maintenance_key,
    company_unit_key,
    string_key,
    warning_key,
    warning_type,
    priority,
    maintenance_type_key,
    technician_key,
    created_by_key,
    cancelled_by_key,
    CASE WHEN opening_dt > current_timestamp() + INTERVAL 1 DAY OR opening_dt < DATE '2000-01-01' THEN NULL ELSE opening_dt END AS opening_dt,
    CASE WHEN due_date < DATE '2000-01-01' THEN NULL ELSE due_date END AS due_date,
    CASE WHEN concluded_at > current_timestamp() + INTERVAL 1 DAY OR concluded_at < DATE '2000-01-01' THEN NULL ELSE concluded_at END AS concluded_at,
    CASE WHEN cancelled_at > current_timestamp() + INTERVAL 1 DAY OR cancelled_at < DATE '2000-01-01' THEN NULL ELSE cancelled_at END AS cancelled_at,
    maintenance_status,
    calculated_status,
    estimated_cost,
    labor_cost,
    parts_cost,
    total_cost,
    resolution_days,
    unit_avg_resolution_days,
    unit_resolution_rank,
    unit_running_maintenance_cost
  FROM sem_negativos
),
sem_duplicadas AS (
  SELECT DISTINCT * FROM datas_validas
),
com_chave AS (
  SELECT * FROM sem_duplicadas
  WHERE maintenance_key IS NOT NULL
),
chave_unica AS (
  SELECT
    opening_date_key,
    maintenance_key,
    company_unit_key,
    string_key,
    warning_key,
    warning_type,
    priority,
    maintenance_type_key,
    technician_key,
    created_by_key,
    cancelled_by_key,
    opening_dt,
    due_date,
    concluded_at,
    cancelled_at,
    maintenance_status,
    calculated_status,
    estimated_cost,
    labor_cost,
    parts_cost,
    total_cost,
    resolution_days,
    unit_avg_resolution_days,
    unit_resolution_rank,
    unit_running_maintenance_cost
  FROM (
    SELECT *, row_number() OVER (PARTITION BY maintenance_key ORDER BY maintenance_key) AS _linha
    FROM com_chave
  )
  WHERE _linha = 1
),
com_pai AS (
  SELECT * FROM chave_unica
  WHERE (company_unit_key IS NULL OR company_unit_key IN (SELECT company_unit_key FROM workspace.cleaned.dim_company_unit))
    AND (maintenance_type_key IS NULL OR maintenance_type_key IN (SELECT maintenance_type_key FROM workspace.cleaned.dim_maintenance_type))
)
SELECT * FROM com_pai;
