-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Limpeza: fact_relocation
-- MAGIC Refaz `workspace.cleaned.fact_relocation` a partir de `workspace.raw.fact_relocation`: padroniza textos, zera valores inválidos e remove linhas duplicadas, sem chave ou sem pai. Fonte da pipeline `Apollo_BI`.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW workspace.cleaned.fact_relocation
COMMENT 'Realocações tratadas a partir da camada raw'
AS
WITH
lida AS (
  SELECT * FROM workspace.raw.fact_relocation
),
aparada AS (
  SELECT
    trim(regexp_replace(relocation_scope, '\\s+', ' ')) AS relocation_scope,
    relocation_key,
    trim(regexp_replace(origin_unit_key, '\\s+', ' ')) AS origin_unit_key,
    trim(regexp_replace(destination_unit_key, '\\s+', ' ')) AS destination_unit_key,
    destination_company_key,
    segment_key,
    trim(regexp_replace(string_key, '\\s+', ' ')) AS string_key,
    panel_key,
    panel_quantity,
    trim(regexp_replace(status, '\\s+', ' ')) AS status,
    suggested_at,
    reviewed_at,
    review_days
  FROM lida
),
sem_nulo_falso AS (
  SELECT
    CASE WHEN lower(relocation_scope) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE relocation_scope END AS relocation_scope,
    relocation_key,
    CASE WHEN lower(origin_unit_key) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE origin_unit_key END AS origin_unit_key,
    CASE WHEN lower(destination_unit_key) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE destination_unit_key END AS destination_unit_key,
    destination_company_key,
    segment_key,
    CASE WHEN lower(string_key) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE string_key END AS string_key,
    panel_key,
    panel_quantity,
    CASE WHEN lower(status) RLIKE '^(|s/n|n/a|na|n\\.a\\.|null|none|nan|nil|-+|\\?+|nulo|sem informacao|sem informação|nao informado|não informado|#n/a)$' THEN NULL ELSE status END AS status,
    suggested_at,
    reviewed_at,
    review_days
  FROM aparada
),
categorias AS (
  SELECT
    regexp_replace(translate(upper(relocation_scope), 'ÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ', 'AAAAAEEEEIIIIOOOOOUUUUC'), '[\\s\\-]+', '_') AS relocation_scope,
    relocation_key,
    origin_unit_key,
    destination_unit_key,
    destination_company_key,
    segment_key,
    string_key,
    panel_key,
    panel_quantity,
    regexp_replace(translate(upper(status), 'ÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ', 'AAAAAEEEEIIIIOOOOOUUUUC'), '[\\s\\-]+', '_') AS status,
    suggested_at,
    reviewed_at,
    review_days
  FROM sem_nulo_falso
),
dominios AS (
  SELECT
    relocation_scope,
    relocation_key,
    origin_unit_key,
    destination_unit_key,
    destination_company_key,
    segment_key,
    string_key,
    panel_key,
    panel_quantity,
    CASE WHEN status IN ('PENDENTE', 'APROVADA', 'REJEITADA', 'CONCLUIDA') THEN status END AS status,
    suggested_at,
    reviewed_at,
    review_days
  FROM categorias
),
sem_negativos AS (
  SELECT
    relocation_scope,
    relocation_key,
    origin_unit_key,
    destination_unit_key,
    destination_company_key,
    segment_key,
    string_key,
    panel_key,
    CASE WHEN panel_quantity < 0 THEN NULL ELSE panel_quantity END AS panel_quantity,
    status,
    suggested_at,
    reviewed_at,
    CASE WHEN review_days < 0 THEN NULL ELSE review_days END AS review_days
  FROM dominios
),
datas_validas AS (
  SELECT
    relocation_scope,
    relocation_key,
    origin_unit_key,
    destination_unit_key,
    destination_company_key,
    segment_key,
    string_key,
    panel_key,
    panel_quantity,
    status,
    CASE WHEN suggested_at > current_timestamp() + INTERVAL 1 DAY OR suggested_at < DATE '2000-01-01' THEN NULL ELSE suggested_at END AS suggested_at,
    CASE WHEN reviewed_at > current_timestamp() + INTERVAL 1 DAY OR reviewed_at < DATE '2000-01-01' THEN NULL ELSE reviewed_at END AS reviewed_at,
    review_days
  FROM sem_negativos
),
sem_duplicadas AS (
  SELECT DISTINCT * FROM datas_validas
),
com_chave AS (
  SELECT * FROM sem_duplicadas
  WHERE relocation_scope IS NOT NULL AND relocation_key IS NOT NULL
),
chave_unica AS (
  SELECT
    relocation_scope,
    relocation_key,
    origin_unit_key,
    destination_unit_key,
    destination_company_key,
    segment_key,
    string_key,
    panel_key,
    panel_quantity,
    status,
    suggested_at,
    reviewed_at,
    review_days
  FROM (
    SELECT *, row_number() OVER (PARTITION BY relocation_scope, relocation_key ORDER BY relocation_scope, relocation_key) AS _linha
    FROM com_chave
  )
  WHERE _linha = 1
)
SELECT * FROM chave_unica;
