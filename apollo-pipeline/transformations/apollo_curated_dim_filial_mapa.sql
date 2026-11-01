-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## KPI: dim_filial_mapa
-- MAGIC Calcula `workspace.curated.dim_filial_mapa` a partir da camada `cleaned`. Fonte da pipeline `Apollo_BI`.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW workspace.curated.dim_filial_mapa
COMMENT 'Filial com cidade, estado e indicadores de risco para o mapa do dashboard'
AS
SELECT
  u.company_unit_key,
  u.company_key,
  u.unit_name,
  u.city,
  u.state,
  u.is_active,
  u.kwp_total,
  s.saude_media,
  coalesce(s.strings_abaixo_limiar, 0) AS strings_abaixo_limiar,
  coalesce(a.alertas_ativos, 0) AS alertas_ativos
FROM workspace.cleaned.dim_company_unit u
LEFT JOIN workspace.curated.kpi_saude_filial s ON s.company_unit_key = u.company_unit_key
LEFT JOIN workspace.curated.kpi_alertas_filial a ON a.company_unit_key = u.company_unit_key;
