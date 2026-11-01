-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## KPI: kpi_estoque_filial
-- MAGIC Calcula `workspace.curated.kpi_estoque_filial` a partir da camada `cleaned`. Fonte da pipeline `Apollo_BI`.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW workspace.curated.kpi_estoque_filial
COMMENT 'Peças cadastradas, peças abaixo do mínimo e valor do estoque por filial'
AS
SELECT
  company_unit_key,
  count(*) AS pecas_cadastradas,
  count_if(below_minimum) AS pecas_abaixo_minimo,
  round(sum(stock_value), 2) AS valor_estoque
FROM workspace.curated.kpi_estoque_peca
GROUP BY company_unit_key;
