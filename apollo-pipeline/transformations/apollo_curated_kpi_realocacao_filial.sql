-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## KPI: kpi_realocacao_filial
-- MAGIC Calcula `workspace.curated.kpi_realocacao_filial` a partir da camada `cleaned`. Fonte da pipeline `Apollo_BI`.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW workspace.curated.kpi_realocacao_filial
COMMENT 'Solicitações de realocação por filial de origem e escopo'
AS
SELECT
  origin_unit_key AS company_unit_key,
  relocation_scope,
  count(*) AS total_solicitacoes,
  count_if(status = 'PENDENTE') AS pendentes,
  count_if(status = 'APROVADA') AS aprovadas,
  count_if(status = 'REJEITADA') AS rejeitadas,
  count_if(status = 'CONCLUIDA') AS concluidas,
  sum(panel_quantity) AS placas_envolvidas,
  round(avg(review_days), 2) AS dias_medios_revisao
FROM workspace.cleaned.fact_relocation
GROUP BY origin_unit_key, relocation_scope;
