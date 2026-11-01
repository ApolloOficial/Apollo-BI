-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## KPI do tracking: kpi_tracking_acoes
-- MAGIC Calcula `workspace.curated.kpi_tracking_acoes` a partir de `workspace.cleaned.tracking_events`. Fonte da pipeline `Apollo_BI`.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW workspace.curated.kpi_tracking_acoes
COMMENT 'Toques por tela, alvo e forma de entrada'
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
  screen,
  properties_target AS alvo,
  properties_input_method AS forma_entrada,
  count(*) AS acoes
FROM eventos
WHERE event_name = 'ui_action'
GROUP BY company_id, screen, properties_target, properties_input_method;
