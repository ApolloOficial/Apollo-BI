-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Resumo da limpeza: tracking
-- MAGIC Compara linhas e nulos do tracking entre `raw` e `cleaned`. Fonte da pipeline `Apollo_BI`.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW workspace.cleaned.cleaning_log_mongo
COMMENT 'Resumo da limpeza do tracking: linhas e nulos das colunas tratadas antes e depois'
AS
SELECT
  'tracking_events' AS tabela,
  (SELECT count(*) FROM workspace.raw.tracking_events) AS linhas_raw,
  (SELECT count(*) FROM workspace.cleaned.tracking_events) AS linhas_cleaned,
  (SELECT count(*) FROM workspace.raw.tracking_events) - (SELECT count(*) FROM workspace.cleaned.tracking_events) AS linhas_removidas,
  (SELECT coalesce(sum(CAST(event_id IS NULL AS INT) + CAST(event_name IS NULL AS INT) + CAST(session_id IS NULL AS INT) + CAST(user_id IS NULL AS INT) + CAST(platform IS NULL AS INT) + CAST(source IS NULL AS INT) + CAST(properties_exit_reason IS NULL AS INT) + CAST(properties_input_method IS NULL AS INT) + CAST(properties_method IS NULL AS INT) + CAST(properties_endpoint IS NULL AS INT) + CAST(properties_target IS NULL AS INT) + CAST(company_id IS NULL AS INT) + CAST(schema_version IS NULL AS INT) + CAST(timestamp IS NULL AS INT) + CAST(received_at IS NULL AS INT) + CAST(properties_duration_ms IS NULL AS INT) + CAST(properties_http_status IS NULL AS INT) + CAST(mongo_id IS NULL AS INT)), 0) FROM workspace.raw.tracking_events) AS nulos_raw,
  (SELECT coalesce(sum(CAST(event_id IS NULL AS INT) + CAST(event_name IS NULL AS INT) + CAST(session_id IS NULL AS INT) + CAST(user_id IS NULL AS INT) + CAST(platform IS NULL AS INT) + CAST(source IS NULL AS INT) + CAST(properties_exit_reason IS NULL AS INT) + CAST(properties_input_method IS NULL AS INT) + CAST(properties_method IS NULL AS INT) + CAST(properties_endpoint IS NULL AS INT) + CAST(properties_target IS NULL AS INT) + CAST(company_id IS NULL AS INT) + CAST(schema_version IS NULL AS INT) + CAST(timestamp IS NULL AS INT) + CAST(received_at IS NULL AS INT) + CAST(properties_duration_ms IS NULL AS INT) + CAST(properties_http_status IS NULL AS INT) + CAST(mongo_id IS NULL AS INT)), 0) FROM workspace.cleaned.tracking_events) AS nulos_cleaned,
  current_timestamp() AS executado_em;
