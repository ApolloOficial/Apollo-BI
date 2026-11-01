-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## KPI: kpi_alertas_filial
-- MAGIC Calcula `workspace.curated.kpi_alertas_filial` a partir da camada `cleaned`. Fonte da pipeline `Apollo_BI`.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW workspace.curated.kpi_alertas_filial
COMMENT 'Alertas por status e severidade, alertas sem OS e horas até a OS por filial'
AS
SELECT
  company_unit_key,
  count(*) AS total_alertas,
  count_if(status = 'ATIVO') AS alertas_ativos,
  count_if(status = 'RECONHECIDO') AS alertas_reconhecidos,
  count_if(status = 'RESOLVIDO') AS alertas_resolvidos,
  count_if(severity = 'CRITICA') AS alertas_criticos,
  count_if((maintenance_key IS NULL OR maintenance_status = 'CANCELADA') AND status <> 'RESOLVIDO') AS alertas_sem_os,
  round(avg(hours_to_maintenance), 2) AS horas_medias_ate_os,
  round(avg(resolution_days), 2) AS dias_medios_resolucao_alerta
FROM workspace.cleaned.fact_warning
GROUP BY company_unit_key;
