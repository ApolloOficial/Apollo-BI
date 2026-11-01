-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## KPI: kpi_is_filial
-- MAGIC Calcula `workspace.curated.kpi_is_filial` a partir da camada `cleaned`. Fonte da pipeline `Apollo_BI`.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW workspace.curated.kpi_is_filial
COMMENT 'IS Energético, IS Ambiental, IS Geral e desvio da meta por filial'
AS
WITH
por_filial AS (
  SELECT
    company_unit_key,
    round(avg(is_energetico), 2) AS is_energetico,
    round(avg(is_ambiental), 2) AS is_ambiental
  FROM workspace.curated.kpi_is_string
  GROUP BY company_unit_key
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
