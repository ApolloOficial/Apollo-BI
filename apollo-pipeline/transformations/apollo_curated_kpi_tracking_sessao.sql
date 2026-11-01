-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## KPI do tracking: kpi_tracking_sessao
-- MAGIC Calcula `workspace.curated.kpi_tracking_sessao` a partir de `workspace.cleaned.tracking_events`. Fonte da pipeline `Apollo_BI`.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW workspace.curated.kpi_tracking_sessao
COMMENT 'Uma linha por sessão: duração, eventos e telas distintas'
AS
WITH
eventos AS (
  SELECT
    *,
    to_date(from_utc_timestamp(timestamp, 'America/Sao_Paulo')) AS event_date
  FROM workspace.cleaned.tracking_events
  WHERE company_id IS NOT NULL
),
por_sessao AS (
  SELECT
    session_id,
    company_id,
    user_id,
    min(timestamp) AS inicio,
    max(timestamp) AS fim,
    count(*) AS eventos,
    count(DISTINCT screen) AS telas_distintas
  FROM eventos
  GROUP BY session_id, company_id, user_id
)
SELECT
  *,
  round(CAST(fim AS DOUBLE) - CAST(inicio AS DOUBLE), 1) AS duracao_s
FROM por_sessao;
