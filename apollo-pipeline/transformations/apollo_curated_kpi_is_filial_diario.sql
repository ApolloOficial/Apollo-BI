-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## KPI: kpi_is_filial_diario
-- MAGIC Calcula `workspace.curated.kpi_is_filial_diario` a partir da camada `cleaned`. Fonte da pipeline `Apollo_BI`.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW workspace.curated.kpi_is_filial_diario
COMMENT 'IS Energético, IS Ambiental, IS Geral e desvio da meta por filial e dia'
AS
WITH
string_filial AS (
  SELECT s.string_key, s.panel_model_key, i.company_unit_key
  FROM workspace.cleaned.dim_string s
  JOIN workspace.cleaned.dim_inverter i ON i.inverter_key = s.inverter_key
),
desempenho AS (
  SELECT
    p.string_key,
    d.full_date,
    avg(CAST(p.peak_to_nominal_ratio >= 0.80 AS INT)) AS pct_no_padrao
  FROM workspace.cleaned.fact_string_daily_performance p
  JOIN workspace.cleaned.dim_date d ON d.date_key = p.date_key
  GROUP BY p.string_key, d.full_date
),
saude AS (
  SELECT
    h.string_key,
    d.full_date,
    avg(CAST(h.health_delta > -10 AS INT)) AS pct_sem_queda
  FROM workspace.cleaned.fact_string_health h
  JOIN workspace.cleaned.dim_date d ON d.date_key = h.date_key
  WHERE h.health_delta IS NOT NULL
  GROUP BY h.string_key, d.full_date
),
energetico AS (
  SELECT
    string_key,
    full_date,
    round(100 * (coalesce(pct_no_padrao, pct_sem_queda) + coalesce(pct_sem_queda, pct_no_padrao)) / 2, 2) AS is_energetico
  FROM desempenho
  FULL OUTER JOIN saude USING (string_key, full_date)
),
vida_util AS (
  SELECT f.string_key, coalesce(CAST(m.warranty_years AS DOUBLE), 25.0) AS vida_util_anos
  FROM string_filial f
  LEFT JOIN workspace.cleaned.dim_panel_model m ON m.panel_model_key = f.panel_model_key
),
idade AS (
  SELECT
    e.string_key,
    e.full_date,
    avg(datediff(e.full_date, p.installation_dt) / 365.25) AS idade_media_anos
  FROM energetico e
  JOIN workspace.cleaned.dim_panel p ON p.string_key = e.string_key
  WHERE p.installation_dt IS NOT NULL
    AND p.installation_dt <= e.full_date
    AND (p.deactivated_at IS NULL OR p.deactivated_at > e.full_date)
  GROUP BY e.string_key, e.full_date
),
ambiental AS (
  SELECT
    i.string_key,
    i.full_date,
    round(CASE WHEN v.vida_util_anos > 0 THEN greatest(0.0, least(100.0, (1 - i.idade_media_anos / v.vida_util_anos) * 100)) END, 2) AS is_ambiental
  FROM idade i
  JOIN vida_util v ON v.string_key = i.string_key
),
por_filial AS (
  SELECT
    f.company_unit_key,
    e.full_date,
    round(avg(e.is_energetico), 2) AS is_energetico,
    round(avg(a.is_ambiental), 2) AS is_ambiental
  FROM energetico e
  LEFT JOIN ambiental a ON a.string_key = e.string_key AND a.full_date = e.full_date
  JOIN string_filial f ON f.string_key = e.string_key
  GROUP BY f.company_unit_key, e.full_date
),
com_geral AS (
  SELECT
    *,
    round((coalesce(is_energetico, is_ambiental) + coalesce(is_ambiental, is_energetico)) / 2, 2) AS is_geral
  FROM por_filial
)
SELECT
  g.*,
  round(g.is_geral - 80, 2) AS desvio_meta,
  u.company_key,
  u.unit_name
FROM com_geral g
JOIN workspace.cleaned.dim_company_unit u ON u.company_unit_key = g.company_unit_key;
