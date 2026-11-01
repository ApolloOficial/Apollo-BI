-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## KPI do tracking: kpi_tracking_tela
-- MAGIC Calcula `workspace.curated.kpi_tracking_tela` a partir de `workspace.cleaned.tracking_events`. Fonte da pipeline `Apollo_BI`.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW workspace.curated.kpi_tracking_tela
COMMENT 'Visualizações e tempo médio e mediano por tela'
AS
WITH
eventos AS (
  SELECT
    *,
    to_date(from_utc_timestamp(timestamp, 'America/Sao_Paulo')) AS event_date
  FROM workspace.cleaned.tracking_events
  WHERE company_id IS NOT NULL
),
visualizacoes AS (
  SELECT company_id, screen, count(*) AS visualizacoes
  FROM eventos
  WHERE event_name = 'screen_viewed'
  GROUP BY company_id, screen
),
tempo AS (
  SELECT
    company_id,
    screen,
    round(avg(properties_duration_ms), 0) AS tempo_medio_ms,
    percentile_approx(properties_duration_ms, 0.5) AS tempo_mediano_ms
  FROM eventos
  WHERE event_name = 'screen_exited' AND properties_duration_ms IS NOT NULL
  GROUP BY company_id, screen
)
SELECT *
FROM visualizacoes
FULL OUTER JOIN tempo USING (company_id, screen);
