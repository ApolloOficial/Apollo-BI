-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## KPI do tracking: kpi_tracking_api_diaria
-- MAGIC Calcula `workspace.curated.kpi_tracking_api_diaria` a partir de `workspace.cleaned.tracking_events`. Fonte da pipeline `Apollo_BI`.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW workspace.curated.kpi_tracking_api_diaria
COMMENT 'Requisições, erros, taxa de erro e latência por endpoint e dia'
AS
WITH
eventos AS (
  SELECT
    *,
    to_date(from_utc_timestamp(timestamp, 'America/Sao_Paulo')) AS event_date
  FROM workspace.cleaned.tracking_events
  WHERE company_id IS NOT NULL
),
por_endpoint AS (
  SELECT
    company_id,
    event_date,
    properties_endpoint AS endpoint,
    count(*) AS requisicoes,
    count_if(properties_http_status >= 400) AS erros,
    round(avg(properties_duration_ms), 0) AS latencia_media_ms,
    percentile_approx(properties_duration_ms, 0.95) AS latencia_p95_ms
  FROM eventos
  WHERE event_name = 'api_request_completed'
  GROUP BY company_id, event_date, properties_endpoint
)
SELECT
  *,
  round(100 * erros / requisicoes, 2) AS taxa_erro_pct
FROM por_endpoint;
