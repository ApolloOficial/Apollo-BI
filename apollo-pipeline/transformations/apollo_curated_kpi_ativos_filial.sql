-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## KPI: kpi_ativos_filial
-- MAGIC Calcula `workspace.curated.kpi_ativos_filial` a partir da camada `cleaned`. Fonte da pipeline `Apollo_BI`.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW workspace.curated.kpi_ativos_filial
COMMENT 'Placas por status, taxa de ativação e percentual em falha ou manutenção por filial'
AS
WITH
string_filial AS (
  SELECT s.string_key, s.panel_model_key, i.company_unit_key
  FROM workspace.cleaned.dim_string s
  JOIN workspace.cleaned.dim_inverter i ON i.inverter_key = s.inverter_key
),
por_filial AS (
  SELECT
    f.company_unit_key,
    count(*) AS total_placas,
    count_if(p.operating_stats = 'EM_ESTOQUE') AS placas_em_estoque,
    count_if(p.operating_stats = 'OPERACIONAL') AS placas_operacionais,
    count_if(p.operating_stats = 'MANUTENCAO') AS placas_em_manutencao,
    count_if(p.operating_stats = 'INATIVO') AS placas_inativas,
    count_if(p.operating_stats = 'FALHA') AS placas_em_falha
  FROM workspace.cleaned.dim_panel p
  JOIN string_filial f ON f.string_key = p.string_key
  GROUP BY f.company_unit_key
)
SELECT
  a.*,
  u.kwp_total,
  round(100 * (a.placas_em_falha + a.placas_em_manutencao) / a.total_placas, 2) AS pct_falha_ou_manutencao,
  round(100 * (a.total_placas - a.placas_em_estoque) / a.total_placas, 2) AS taxa_ativacao
FROM por_filial a
LEFT JOIN workspace.cleaned.dim_company_unit u ON u.company_unit_key = a.company_unit_key;
