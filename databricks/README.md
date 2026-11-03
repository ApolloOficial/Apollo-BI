# Configuração exportada do Databricks

Arquivos exportados do workspace do Apollo em 02/11/2026, para versionar a configuração que roda no Databricks.

| Arquivo | O que é | Como foi exportado |
|---|---|---|
| [apollo_pipeline.json](apollo_pipeline.json) | Pipeline `Apollo_Pipeline`: serverless, modo Triggered, catálogo `workspace` e fontes em `apollo-pipeline/transformations` | Pipeline > Settings > JSON |
| [apollo_job.yml](apollo_job.yml) | Job `Apollo_Job`: `raw_operacional` e `raw_tracking` em paralelo, depois a pipeline; agendado todo dia às 07:00 (America/Sao_Paulo) | Job > ⋮ > View as code > YAML |
| [metricas_solares.lvdash.json](metricas_solares.lvdash.json) | Dashboard Métricas Solares (AI/BI): 2 páginas, 36 datasets em `workspace.curated`, cards, barras, linhas, histogramas, 3 boxplots, mapa e 9 filtros | Dashboard > Edit draft > ⋮ > Export |

Os caminhos apontam para a Git folder `/Workspace/Shared/Apollo-BI`. Nenhum dos arquivos tem credenciais: o acesso ao Neon e ao MongoDB vem do `.env` da Git folder, que não é versionado.

Para recriar em outro workspace, importe o dashboard em **Dashboards > Create > Import**, troque os caminhos `/Workspace/Shared/Apollo-BI` pelo da sua Git folder e o `pipeline_id` do Job pelo id da pipeline criada.
