-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## KPI: kpi_is_string
-- MAGIC Calcula `workspace.curated.kpi_is_string` a partir da camada `cleaned`. Fonte da pipeline `Apollo_BI`.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW workspace.curated.kpi_is_string
COMMENT 'IS Energético e IS Ambiental de cada string'
AS
WITH
string_filial AS (
  SELECT s.string_key, s.panel_model_key, i.company_unit_key
  FROM workspace.cleaned.dim_string s
  JOIN workspace.cleaned.dim_inverter i ON i.inverter_key = s.inverter_key
),
no_padrao AS (
  SELECT string_key, avg(CAST(peak_to_nominal_ratio >= 0.80 AS INT)) AS pct_no_padrao
  FROM workspace.cleaned.fact_string_daily_performance
  GROUP BY string_key
),
sem_queda AS (
  SELECT string_key, avg(CAST(health_delta > -10 AS INT)) AS pct_sem_queda
  FROM workspace.cleaned.fact_string_health
  WHERE health_delta IS NOT NULL
  GROUP BY string_key
),
energetico AS (
  SELECT
    string_key,
    round(100 * (coalesce(pct_no_padrao, pct_sem_queda) + coalesce(pct_sem_queda, pct_no_padrao)) / 2, 2) AS is_energetico
  FROM no_padrao
  FULL OUTER JOIN sem_queda USING (string_key)
),
vida_util AS (
  SELECT f.string_key, coalesce(CAST(m.warranty_years AS DOUBLE), 25.0) AS vida_util_anos
  FROM string_filial f
  LEFT JOIN workspace.cleaned.dim_panel_model m ON m.panel_model_key = f.panel_model_key
),
idade AS (
  SELECT string_key, avg(datediff(current_date(), installation_dt) / 365.25) AS idade_media_anos
  FROM workspace.cleaned.dim_panel
  WHERE installation_dt IS NOT NULL AND deactivated_at IS NULL
  GROUP BY string_key
),
ambiental AS (
  SELECT
    i.string_key,
    round(CASE WHEN v.vida_util_anos > 0 THEN greatest(0.0, least(100.0, (1 - i.idade_media_anos / v.vida_util_anos) * 100)) END, 2) AS is_ambiental
  FROM idade i
  JOIN vida_util v ON v.string_key = i.string_key
)
SELECT
  f.string_key,
  f.company_unit_key,
  e.is_energetico,
  a.is_ambiental
FROM string_filial f
LEFT JOIN energetico e ON e.string_key = f.string_key
LEFT JOIN ambiental a ON a.string_key = f.string_key;
