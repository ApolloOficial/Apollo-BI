-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## KPI do tracking: kpi_tracking_diario_empresa
-- MAGIC Calcula `workspace.curated.kpi_tracking_diario_empresa` a partir de `workspace.cleaned.tracking_events`. Fonte da pipeline `Apollo_BI`.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW workspace.curated.kpi_tracking_diario_empresa
COMMENT 'Eventos, sessões, usuários ativos e aberturas do app por empresa e dia'
AS
WITH
eventos AS (
  SELECT
    *,
    to_date(from_utc_timestamp(timestamp, 'America/Sao_Paulo')) AS event_date
  FROM workspace.cleaned.tracking_events
  WHERE company_id IS NOT NULL
)
SELECT
  company_id,
  event_date,
  count(*) AS eventos,
  count(DISTINCT session_id) AS sessoes,
  count(DISTINCT user_id) AS usuarios_ativos,
  count_if(event_name = 'app_opened') AS aberturas_app
FROM eventos
GROUP BY company_id, event_date;
