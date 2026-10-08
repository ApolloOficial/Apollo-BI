# Apollo · BI (Databricks) — 2º ano

Pipeline em arquitetura medalhão (Bronze → Silver → Gold) que lê o banco do Apollo no **Neon**, calcula os KPIs/IS e gera a base final do dashboard.

```
Neon (Postgres) ──► Bronze (bruto) ──► Silver (limpo) ──► Gold (KPIs) ──► Dashboard
```

## Estrutura
| Pasta / arquivo | O que é |
|---|---|
| `apollo_core/apollo_connection_neon_core` | Conexão com o Neon + funções reutilizáveis (`read_neon`, `list_neon_tables`, `ensure_schemas`) |
| `apollo_pipeline/01_bronze_ingest` | Copia as tabelas do Neon sem transformar |
| `apollo_pipeline/02_silver_transform` | Padroniza nomes, limpa strings, remove duplicatas |
| `apollo_pipeline/03_gold_kpis` | Calcula os KPIs/IS e salva as tabelas do dashboard |
| *(depois)* `apollo_manager/` | Dashboard principal |

Tabelas ficam em `workspace.bronze`, `workspace.silver` e `workspace.gold` (catálogo ajustável em `CATALOG` no core).

## Regras de colaboração (leia antes de mexer)
1. **Nunca edite na `main`.** Cada pessoa trabalha na própria branch: `feat/<seu-nome>-<assunto>` (ex.: `feat/yasmin-bronze`).
2. Antes de começar: **Pull** da `main` → crie/troque para sua branch.
3. Terminou um pedaço: **Commit + Push** da sua branch → abra **Pull Request** → outra pessoa revisa → merge na `main`.
4. **Um notebook, um dono por vez.** Avisem no grupo quem está mexendo em qual notebook (evita conflito de merge em notebook).
5. **Nenhuma senha no código.** Credenciais só em Databricks Secrets (passo 4 abaixo).
6. Notebooks são salvos como `.py` (formato fonte do Databricks) para o diff do Git ficar legível.

## Passo a passo — configuração (cada pessoa, uma vez)
1. **Acesso ao workspace:** peça acesso ao workspace do Databricks do Apollo (convite do dono do workspace).
2. **Conta no GitHub** com permissão de escrita no repositório do grupo.
3. **Conectar o Git:** Databricks → *Settings → Linked accounts → Git integration* → GitHub + Personal Access Token (escopo `repo`).
4. **Criar a pasta Git:** *Workspace → Create → Git folder* → cole a URL do repositório → *Create*. Troque para a sua branch (menu da branch no topo).
5. **Secrets (só o dono do workspace faz, uma vez):** criar o escopo `apollo` com as chaves `neon_host`, `neon_db`, `neon_user`, `neon_password` (Databricks CLI: `databricks secrets create-scope apollo` e `databricks secrets put-secret apollo neon_host`, etc.). Valores vêm da connection string do Neon.
6. **Testar:** abra `apollo_core/apollo_connection_neon_core` → *Run all*. Deve imprimir a lista de tabelas do Neon.

## Passo a passo — fluxo de trabalho diário
1. Abra sua Git folder → **Pull** → confirme que está na sua branch.
2. Edite só o notebook combinado no grupo.
3. Rode para testar (ordem: core → 01 → 02 → 03).
4. **Commit & Push** com mensagem clara (`bronze: ingere tabelas de painel`).
5. Abra o PR no GitHub e avise o grupo.

## Ordem de execução do pipeline
`01_bronze_ingest` → `02_silver_transform` → `03_gold_kpis`. Depois será automatizado em um **Job** (task 1 → 2 → 3), exigido na parte "Extra" da disciplina.

## Checklist da disciplina (mínimo 7,0 + extra)
- [ ] Dashboard conectado à fonte (base gold): barras/linhas, cards de KPI, histograma e boxplot, mapa (se houver geografia), filtros (período, filial)
- [ ] Relatório gerencial: apresentação da base, problema analisado, tratamento dos dados, KPIs
- [ ] **Extra:** pipeline no Databricks (leitura, tratamento, transformação, KPIs, base final) + Job (manual ou agendado)
- [ ] Evidências do Job: configuração, histórico de execução, status e resultado
- [ ] Relatório explica fluxo do pipeline, etapas automatizadas e relação com KPIs/dashboard
