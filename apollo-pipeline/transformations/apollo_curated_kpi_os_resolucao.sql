-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## KPI: kpi_os_resolucao
-- MAGIC Calcula `workspace.curated.kpi_os_resolucao` a partir da camada `cleaned`. Fonte da pipeline `Apollo_BI`.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW workspace.curated.kpi_os_resolucao
COMMENT 'Uma linha por OS concluída, com dias de resolução, custo e se ficou dentro da meta'
AS
SELECT
  m.maintenance_key,
  m.company_unit_key,
  date_format(d.full_date, 'yyyy-MM') AS ano_mes,
  t.type_name AS tipo_manutencao,
  m.warning_type,
  m.resolution_days,
  m.total_cost,
  m.resolution_days <= 7 AS dentro_da_meta
FROM workspace.cleaned.fact_maintenance m
JOIN workspace.cleaned.dim_maintenance_type t ON t.maintenance_type_key = m.maintenance_type_key
JOIN workspace.cleaned.dim_date d ON d.date_key = m.opening_date_key
WHERE m.maintenance_status = 'CONCLUIDA'
  AND m.resolution_days IS NOT NULL;
