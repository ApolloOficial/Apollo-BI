-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## KPI: kpi_estoque_peca
-- MAGIC Calcula `workspace.curated.kpi_estoque_peca` a partir da camada `cleaned`. Fonte da pipeline `Apollo_BI`.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW workspace.curated.kpi_estoque_peca
COMMENT 'Estoque de cada peça por filial'
AS
SELECT
  s.*,
  p.sku,
  p.part_name
FROM workspace.cleaned.fact_stock s
JOIN workspace.cleaned.dim_part p ON p.part_key = s.part_key;
