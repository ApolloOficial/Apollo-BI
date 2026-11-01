-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## KPI: kpi_saude_diaria_filial
-- MAGIC Calcula `workspace.curated.kpi_saude_diaria_filial` a partir da camada `cleaned`. Fonte da pipeline `Apollo_BI`.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW workspace.curated.kpi_saude_diaria_filial
COMMENT 'Evolução diária da saúde média por filial'
AS
WITH
string_filial AS (
  SELECT s.string_key, s.panel_model_key, i.company_unit_key
  FROM workspace.cleaned.dim_string s
  JOIN workspace.cleaned.dim_inverter i ON i.inverter_key = s.inverter_key
)
SELECT
  f.company_unit_key,
  d.full_date,
  round(avg(h.health_score), 2) AS saude_media
FROM workspace.cleaned.fact_string_health h
JOIN string_filial f ON f.string_key = h.string_key
JOIN workspace.cleaned.dim_date d ON d.date_key = h.date_key
GROUP BY f.company_unit_key, d.full_date;
