-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Limpeza: tracking_events
-- MAGIC Refaz `workspace.cleaned.tracking_events` a partir de `workspace.raw.tracking_events`: padroniza textos, converte tipos, zera valores inválidos e remove eventos sem campo obrigatório ou duplicados. Fonte da pipeline `Apollo_BI`.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW workspace.cleaned.tracking_events
COMMENT 'Eventos de tracking do app mobile com tipos convertidos e tratamentos aplicados'
AS
WITH
lida AS (
  SELECT * FROM workspace.raw.tracking_events
),
aparada AS (
  SELECT
    * EXCEPT (event_id, event_name, session_id, user_id, platform, source, properties_exit_reason, properties_input_method, properties_method, properties_endpoint, properties_target, company_id, schema_version, timestamp, received_at, properties_duration_ms, properties_http_status, mongo_id),
    trim(regexp_replace(event_id, '\\s+', ' ')) AS event_id,
    trim(regexp_replace(event_name, '\\s+', ' ')) AS event_name,
    trim(regexp_replace(session_id, '\\s+', ' ')) AS session_id,
    trim(regexp_replace(user_id, '\\s+', ' ')) AS user_id,
    trim(regexp_replace(platform, '\\s+', ' ')) AS platform,
    trim(regexp_replace(source, '\\s+', ' ')) AS source,
    trim(regexp_replace(properties_exit_reason, '\\s+', ' ')) AS properties_exit_reason,
    trim(regexp_replace(properties_input_method, '\\s+', ' ')) AS properties_input_method,
    trim(regexp_replace(properties_method, '\\s+', ' ')) AS properties_method,
    trim(regexp_replace(properties_endpoint, '\\s+', ' ')) AS properties_endpoint,
    trim(regexp_replace(properties_target, '\\s+', ' ')) AS properties_target,
    trim(regexp_replace(company_id, '\\s+', ' ')) AS company_id,
    trim(regexp_replace(schema_version, '\\s+', ' ')) AS schema_version,
    trim(regexp_replace(timestamp, '\\s+', ' ')) AS timestamp,
    trim(regexp_replace(received_at, '\\s+', ' ')) AS received_at,
    trim(regexp_replace(properties_duration_ms, '\\s+', ' ')) AS properties_duration_ms,
    trim(regexp_replace(properties_http_status, '\\s+', ' ')) AS properties_http_status,
    trim(regexp_replace(mongo_id, '\\s+', ' ')) AS mongo_id
  FROM lida
),
padronizada AS (
  SELECT
    * EXCEPT (event_id, event_name, session_id, user_id, platform, source, properties_exit_reason, properties_input_method, properties_method, properties_endpoint, properties_target, company_id, schema_version, timestamp, received_at, properties_duration_ms, properties_http_status, mongo_id),
    CASE WHEN lower(event_id) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|undefined|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE event_id END AS event_id,
    lower(CASE WHEN lower(event_name) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|undefined|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE event_name END) AS event_name,
    CASE WHEN lower(session_id) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|undefined|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE session_id END AS session_id,
    CASE WHEN lower(user_id) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|undefined|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE user_id END AS user_id,
    lower(CASE WHEN lower(platform) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|undefined|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE platform END) AS platform,
    lower(CASE WHEN lower(source) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|undefined|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE source END) AS source,
    lower(CASE WHEN lower(properties_exit_reason) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|undefined|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE properties_exit_reason END) AS properties_exit_reason,
    lower(CASE WHEN lower(properties_input_method) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|undefined|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE properties_input_method END) AS properties_input_method,
    upper(CASE WHEN lower(properties_method) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|undefined|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE properties_method END) AS properties_method,
    CASE WHEN lower(properties_endpoint) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|undefined|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE properties_endpoint END AS properties_endpoint,
    CASE WHEN lower(properties_target) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|undefined|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE properties_target END AS properties_target,
    CASE WHEN lower(company_id) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|undefined|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE company_id END AS company_id,
    CASE WHEN lower(schema_version) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|undefined|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE schema_version END AS schema_version,
    CASE WHEN lower(timestamp) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|undefined|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE timestamp END AS timestamp,
    CASE WHEN lower(received_at) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|undefined|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE received_at END AS received_at,
    CASE WHEN lower(properties_duration_ms) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|undefined|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE properties_duration_ms END AS properties_duration_ms,
    CASE WHEN lower(properties_http_status) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|undefined|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE properties_http_status END AS properties_http_status,
    CASE WHEN lower(mongo_id) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|undefined|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE mongo_id END AS mongo_id
  FROM aparada
),
identificadores AS (
  SELECT
    * EXCEPT (event_id, session_id, user_id),
    CASE WHEN event_id RLIKE '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$' THEN event_id END AS event_id,
    CASE WHEN session_id RLIKE '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$' THEN session_id END AS session_id,
    CASE WHEN user_id RLIKE '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$' THEN user_id END AS user_id
  FROM padronizada
),
tipada AS (
  SELECT
    * EXCEPT (company_id, schema_version, timestamp, received_at, properties_duration_ms, properties_http_status),
    try_cast(try_cast(company_id AS DOUBLE) AS BIGINT) AS company_id,
    try_cast(try_cast(schema_version AS DOUBLE) AS INT) AS schema_version,
    try_cast(timestamp AS TIMESTAMP) AS timestamp,
    try_cast(received_at AS TIMESTAMP) AS received_at,
    try_cast(properties_duration_ms AS DOUBLE) AS properties_duration_ms,
    try_cast(try_cast(properties_http_status AS DOUBLE) AS INT) AS properties_http_status
  FROM identificadores
),
valores_validos AS (
  SELECT
    * EXCEPT (properties_duration_ms, properties_http_status, schema_version),
    CASE WHEN isnan(properties_duration_ms) OR properties_duration_ms IN (double('Infinity'), double('-Infinity')) OR properties_duration_ms NOT BETWEEN 0 AND 86400000 THEN NULL ELSE properties_duration_ms END AS properties_duration_ms,
    CASE WHEN properties_http_status BETWEEN 100 AND 599 THEN properties_http_status END AS properties_http_status,
    CASE WHEN schema_version BETWEEN 1 AND 1000 THEN schema_version END AS schema_version
  FROM tipada
),
datas_validas AS (
  SELECT
    * EXCEPT (timestamp, received_at),
    CASE WHEN timestamp > current_timestamp() + INTERVAL 1 DAY OR timestamp < TIMESTAMP '2020-01-01' THEN NULL ELSE timestamp END AS timestamp,
    CASE WHEN received_at > current_timestamp() + INTERVAL 1 DAY OR received_at < TIMESTAMP '2020-01-01' THEN NULL ELSE received_at END AS received_at
  FROM valores_validos
),
com_campos_obrigatorios AS (
  SELECT * FROM datas_validas
  WHERE event_id IS NOT NULL AND event_name IS NOT NULL AND timestamp IS NOT NULL AND session_id IS NOT NULL
),
evento_unico AS (
  SELECT * EXCEPT (_linha)
  FROM (
    SELECT *, row_number() OVER (PARTITION BY event_id ORDER BY received_at DESC NULLS LAST, ingested_at DESC NULLS LAST) AS _linha
    FROM com_campos_obrigatorios
  )
  WHERE _linha = 1
)
SELECT * FROM evento_unico;
