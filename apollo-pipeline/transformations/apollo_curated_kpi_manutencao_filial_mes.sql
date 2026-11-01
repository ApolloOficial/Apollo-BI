-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## KPI: kpi_manutencao_filial_mes
-- MAGIC Calcula `workspace.curated.kpi_manutencao_filial_mes` a partir da camada `cleaned`. Fonte da pipeline `Apollo_BI`.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW workspace.curated.kpi_manutencao_filial_mes
COMMENT 'Status das OS, atrasos, tempo de resolução, custo, desvio de orçamento, cancelamento e retrabalho por filial e mês'
AS
WITH
cadeia AS (
  SELECT id AS maintenance_key, chain_level > 1 AS retrabalho
  FROM workspace.cleaned.vw_maintenance_chain
),
os AS (
  SELECT
    m.company_unit_key,
    date_format(d.full_date, 'yyyy-MM') AS ano_mes,
    m.maintenance_status,
    m.calculated_status,
    m.resolution_days,
    m.estimated_cost,
    m.total_cost,
    coalesce(c.retrabalho, false) AS retrabalho
  FROM workspace.cleaned.fact_maintenance m
  JOIN workspace.cleaned.dim_date d ON d.date_key = m.opening_date_key
  LEFT JOIN cadeia c ON c.maintenance_key = m.maintenance_key
),
por_mes AS (
  SELECT
    company_unit_key,
    ano_mes,
    count(*) AS total_os,
    count_if(maintenance_status = 'ABERTA') AS os_abertas,
    count_if(maintenance_status = 'EM_ANDAMENTO') AS os_em_andamento,
    count_if(maintenance_status = 'CONCLUIDA') AS os_concluidas,
    count_if(maintenance_status = 'CANCELADA') AS os_canceladas,
    count_if(calculated_status = 'ATRASADA') AS os_atrasadas,
    count_if(retrabalho) AS os_retrabalho,
    round(avg(resolution_days), 2) AS tempo_medio_resolucao_dias,
    round(sum(total_cost), 2) AS custo_total,
    sum(CASE WHEN maintenance_status = 'CONCLUIDA' AND estimated_cost > 0 THEN estimated_cost END) AS orcado_concluidas,
    sum(CASE WHEN maintenance_status = 'CONCLUIDA' AND estimated_cost > 0 THEN total_cost END) AS realizado_concluidas
  FROM os
  GROUP BY company_unit_key, ano_mes
)
SELECT
  *,
  round(100 * os_canceladas / total_os, 2) AS taxa_cancelamento,
  round(100 * os_retrabalho / total_os, 2) AS taxa_retrabalho,
  CASE WHEN orcado_concluidas > 0 THEN round(100 * (realizado_concluidas - orcado_concluidas) / orcado_concluidas, 2) END AS desvio_orcamento_pct,
  round(tempo_medio_resolucao_dias - 7, 2) AS desvio_meta_resolucao
FROM por_mes;
