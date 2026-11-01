-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## KPI: kpi_producao_filial_mes
-- MAGIC Calcula `workspace.curated.kpi_producao_filial_mes` a partir da camada `cleaned`. Fonte da pipeline `Apollo_BI`.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW workspace.curated.kpi_producao_filial_mes
COMMENT 'Energia gerada, kWh por kWp e leituras com falha por filial e mês'
AS
WITH
string_filial AS (
  SELECT s.string_key, s.panel_model_key, i.company_unit_key
  FROM workspace.cleaned.dim_string s
  JOIN workspace.cleaned.dim_inverter i ON i.inverter_key = s.inverter_key
),
mensal AS (
  SELECT
    f.company_unit_key,
    date_format(d.full_date, 'yyyy-MM') AS ano_mes,
    round(sum(p.generated_energy_kwh), 2) AS energia_kwh,
    sum(p.fault_count) AS leituras_com_falha
  FROM workspace.cleaned.fact_string_daily_performance p
  JOIN string_filial f ON f.string_key = p.string_key
  JOIN workspace.cleaned.dim_date d ON d.date_key = p.date_key
  GROUP BY f.company_unit_key, date_format(d.full_date, 'yyyy-MM')
)
SELECT
  m.*,
  u.kwp_total,
  CASE WHEN u.kwp_total > 0 THEN round(m.energia_kwh / u.kwp_total, 2) END AS kwh_por_kwp
FROM mensal m
LEFT JOIN workspace.cleaned.dim_company_unit u ON u.company_unit_key = m.company_unit_key;
