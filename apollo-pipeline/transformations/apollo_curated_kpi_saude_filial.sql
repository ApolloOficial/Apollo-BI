-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## KPI: kpi_saude_filial
-- MAGIC Calcula `workspace.curated.kpi_saude_filial` a partir da camada `cleaned`. Fonte da pipeline `Apollo_BI`.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW workspace.curated.kpi_saude_filial
COMMENT 'Saúde média, strings abaixo do limiar e desvio por filial'
AS
WITH
por_filial AS (
  SELECT
    company_unit_key,
    count(*) AS strings_com_nota,
    round(avg(health_score), 2) AS saude_media,
    count_if(health_score < 70) AS strings_abaixo_limiar,
    count_if(faixa = 'CRITICA') AS strings_criticas,
    count_if(faixa = 'ALTA') AS strings_altas,
    count_if(faixa = 'MEDIA') AS strings_medias
  FROM workspace.curated.kpi_saude_string
  GROUP BY company_unit_key
)
SELECT
  *,
  round(100 * strings_abaixo_limiar / strings_com_nota, 2) AS pct_abaixo_limiar,
  round(saude_media - 70, 2) AS desvio_limiar
FROM por_filial;
