-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Limpeza: fact_string_daily_performance
-- MAGIC Refaz `workspace.cleaned.fact_string_daily_performance` a partir de `workspace.raw.fact_string_daily_performance`: padroniza textos, zera valores inválidos e remove linhas duplicadas, sem chave ou sem pai. Fonte da pipeline `Apollo_BI`.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW workspace.cleaned.fact_string_daily_performance
COMMENT 'Desempenho diário das strings tratadas a partir da camada raw'
AS
WITH
lida AS (
  SELECT * FROM workspace.raw.fact_string_daily_performance
),
aparada AS (
  SELECT
    date_key,
    trim(regexp_replace(string_key, '\\s+', ' ')) AS string_key,
    trim(regexp_replace(inverter_key, '\\s+', ' ')) AS inverter_key,
    measurement_date,
    generated_energy_kwh,
    peak_power_w,
    nominal_power_wp,
    peak_to_nominal_ratio,
    fault_count,
    measurement_count,
    running_total_energy_kwh,
    inverter_yield_rank,
    energy_moving_average_7d
  FROM lida
),
sem_nulo_falso AS (
  SELECT
    date_key,
    CASE WHEN lower(string_key) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE string_key END AS string_key,
    CASE WHEN lower(inverter_key) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE inverter_key END AS inverter_key,
    measurement_date,
    generated_energy_kwh,
    peak_power_w,
    nominal_power_wp,
    peak_to_nominal_ratio,
    fault_count,
    measurement_count,
    running_total_energy_kwh,
    inverter_yield_rank,
    energy_moving_average_7d
  FROM aparada
),
sem_negativos AS (
  SELECT
    date_key,
    string_key,
    inverter_key,
    measurement_date,
    CASE WHEN generated_energy_kwh < 0 THEN NULL ELSE generated_energy_kwh END AS generated_energy_kwh,
    CASE WHEN peak_power_w < 0 THEN NULL ELSE peak_power_w END AS peak_power_w,
    CASE WHEN nominal_power_wp < 0 THEN NULL ELSE nominal_power_wp END AS nominal_power_wp,
    peak_to_nominal_ratio,
    CASE WHEN fault_count < 0 THEN NULL ELSE fault_count END AS fault_count,
    CASE WHEN measurement_count < 0 THEN NULL ELSE measurement_count END AS measurement_count,
    CASE WHEN running_total_energy_kwh < 0 THEN NULL ELSE running_total_energy_kwh END AS running_total_energy_kwh,
    inverter_yield_rank,
    CASE WHEN energy_moving_average_7d < 0 THEN NULL ELSE energy_moving_average_7d END AS energy_moving_average_7d
  FROM sem_nulo_falso
),
na_faixa AS (
  SELECT
    date_key,
    string_key,
    inverter_key,
    measurement_date,
    generated_energy_kwh,
    peak_power_w,
    nominal_power_wp,
    CASE WHEN peak_to_nominal_ratio BETWEEN 0 AND 1.5 THEN peak_to_nominal_ratio END AS peak_to_nominal_ratio,
    fault_count,
    measurement_count,
    running_total_energy_kwh,
    inverter_yield_rank,
    energy_moving_average_7d
  FROM sem_negativos
),
limites AS (
  SELECT
    string_key,
    percentile_approx(generated_energy_kwh, 0.25) AS q1,
    percentile_approx(generated_energy_kwh, 0.75) AS q3
  FROM na_faixa
  GROUP BY string_key
),
sem_outlier AS (
  SELECT
    n.date_key,
    n.string_key,
    n.inverter_key,
    n.measurement_date,
    CASE WHEN (l.q3 - l.q1) > 0 AND n.generated_energy_kwh > l.q3 + 3 * (l.q3 - l.q1) THEN NULL ELSE n.generated_energy_kwh END AS generated_energy_kwh,
    n.peak_power_w,
    n.nominal_power_wp,
    n.peak_to_nominal_ratio,
    n.fault_count,
    n.measurement_count,
    n.running_total_energy_kwh,
    n.inverter_yield_rank,
    n.energy_moving_average_7d
  FROM na_faixa n
  LEFT JOIN limites l ON l.string_key = n.string_key
),
datas_validas AS (
  SELECT
    date_key,
    string_key,
    inverter_key,
    CASE WHEN measurement_date > current_timestamp() + INTERVAL 1 DAY OR measurement_date < DATE '2000-01-01' THEN NULL ELSE measurement_date END AS measurement_date,
    generated_energy_kwh,
    peak_power_w,
    nominal_power_wp,
    peak_to_nominal_ratio,
    fault_count,
    measurement_count,
    running_total_energy_kwh,
    inverter_yield_rank,
    energy_moving_average_7d
  FROM sem_outlier
),
sem_duplicadas AS (
  SELECT DISTINCT * FROM datas_validas
),
com_chave AS (
  SELECT * FROM sem_duplicadas
  WHERE string_key IS NOT NULL AND date_key IS NOT NULL
),
chave_unica AS (
  SELECT
    date_key,
    string_key,
    inverter_key,
    measurement_date,
    generated_energy_kwh,
    peak_power_w,
    nominal_power_wp,
    peak_to_nominal_ratio,
    fault_count,
    measurement_count,
    running_total_energy_kwh,
    inverter_yield_rank,
    energy_moving_average_7d
  FROM (
    SELECT *, row_number() OVER (PARTITION BY string_key, date_key ORDER BY string_key, date_key) AS _linha
    FROM com_chave
  )
  WHERE _linha = 1
),
com_pai AS (
  SELECT * FROM chave_unica
  WHERE (string_key IS NULL OR string_key IN (SELECT string_key FROM workspace.cleaned.dim_string))
    AND (date_key IS NULL OR date_key IN (SELECT date_key FROM workspace.cleaned.dim_date))
)
SELECT * FROM com_pai;
