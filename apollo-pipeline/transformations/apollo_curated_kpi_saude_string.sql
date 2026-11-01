-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## KPI: kpi_saude_string
-- MAGIC Calcula `workspace.curated.kpi_saude_string` a partir da camada `cleaned`. Fonte da pipeline `Apollo_BI`.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW workspace.curated.kpi_saude_string
COMMENT 'Saúde atual e faixa de cada string'
AS
WITH
ultima AS (
  SELECT
    string_key,
    calculated_at,
    health_score,
    row_number() OVER (PARTITION BY string_key ORDER BY calculated_at DESC) AS linha
  FROM workspace.cleaned.fact_string_health
),
string_filial AS (
  SELECT s.string_key, s.panel_model_key, i.company_unit_key
  FROM workspace.cleaned.dim_string s
  JOIN workspace.cleaned.dim_inverter i ON i.inverter_key = s.inverter_key
)
SELECT
  u.string_key,
  u.calculated_at,
  u.health_score,
  CASE
    WHEN u.health_score < 50 THEN 'CRITICA'
    WHEN u.health_score < 60 THEN 'ALTA'
    WHEN u.health_score < 70 THEN 'MEDIA'
    ELSE 'OK'
  END AS faixa,
  f.company_unit_key
FROM ultima u
JOIN string_filial f ON f.string_key = u.string_key
WHERE u.linha = 1;
