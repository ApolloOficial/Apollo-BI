-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## KPI: kpi_os_por_tipo
-- MAGIC Calcula `workspace.curated.kpi_os_por_tipo` a partir da camada `cleaned`. Fonte da pipeline `Apollo_BI`.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW workspace.curated.kpi_os_por_tipo
COMMENT 'Quantidade de OS por filial, mês, tipo de manutenção, tipo de alerta e status'
AS
SELECT
  m.company_unit_key,
  date_format(d.full_date, 'yyyy-MM') AS ano_mes,
  t.type_name AS tipo_manutencao,
  m.warning_type,
  m.maintenance_status,
  count(*) AS qtd_os
FROM workspace.cleaned.fact_maintenance m
JOIN workspace.cleaned.dim_maintenance_type t ON t.maintenance_type_key = m.maintenance_type_key
JOIN workspace.cleaned.dim_date d ON d.date_key = m.opening_date_key
GROUP BY m.company_unit_key, date_format(d.full_date, 'yyyy-MM'), t.type_name, m.warning_type, m.maintenance_status;
