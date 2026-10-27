# Dashboard do Apollo BI

Desenho do dashboard AI/BI do Databricks. Toda página lê o schema `workspace.curated`, gerado pelo pipeline. Nenhum gráfico lê a fonte direta.

## Filtros

| Filtro | Como funciona | Tabelas que respondem |
|---|---|---|
| Período | Campo `ano_mes` (texto `yyyy-MM`) ou `full_date` / `event_date` | `kpi_producao_filial_mes`, `kpi_manutencao_filial_mes`, `kpi_os_por_tipo`, `kpi_os_resolucao`, `kpi_saude_diaria_filial`, `kpi_tracking_diario_empresa`, `kpi_tracking_api_diaria` |
| Filial | Campo `unit_name`, obtido pelo join com `dim_filial_mapa` pelo `company_unit_key` | Todas as tabelas operacionais |
| Empresa | Campo `company_id` | Todas as tabelas de tracking, que não têm filial |

As tabelas de retrato atual (`kpi_saude_string`, `kpi_saude_filial`, `kpi_ativos_filial`, `kpi_alertas_filial`, `kpi_realocacao_filial`, `kpi_estoque_*`, `kpi_is_*`) não têm data. O filtro de período não as afeta.

## Páginas

### 1. Visão geral

| Visual | Tabela | Campos | Meta ou limiar |
|---|---|---|---|
| Card: saúde média e desvio | `kpi_saude_filial` | `saude_media`, `desvio_limiar` | `LIMIAR_SAUDE` = 70 |
| Card: strings abaixo do limiar | `kpi_saude_filial` | `pct_abaixo_limiar` | 70 |
| Card: IS Geral e desvio | `kpi_is_filial` | `is_geral`, `desvio_meta` | `META_IS` = 80 |
| Card: tempo médio de resolução e desvio | `kpi_manutencao_filial_mes` | `tempo_medio_resolucao_dias`, `desvio_meta_resolucao` | `META_RESOLUCAO_DIAS` = 7 |
| Card: alertas ativos | `kpi_alertas_filial` | `alertas_ativos` | |
| Linha: evolução diária da saúde por filial | `kpi_saude_diaria_filial` | `full_date`, `saude_media` | |
| Barras: energia gerada por filial e mês | `kpi_producao_filial_mes` | `ano_mes`, `energia_kwh`, `kwh_por_kwp` | |

### 2. Manutenção

| Visual | Tabela | Campos |
|---|---|---|
| Barras empilhadas: OS por status e mês | `kpi_manutencao_filial_mes` | `ano_mes`, `os_abertas`, `os_em_andamento`, `os_concluidas`, `os_canceladas`, `os_atrasadas` |
| Linha: desvio do orçamento por mês | `kpi_manutencao_filial_mes` | `ano_mes`, `desvio_orcamento_pct` |
| Histograma: dias de resolução | `kpi_os_resolucao` | `resolution_days` |
| Boxplot: dias de resolução por tipo de manutenção | `kpi_os_resolucao` | `tipo_manutencao`, `resolution_days` |
| Barras: OS por tipo | `kpi_os_por_tipo` | `tipo_manutencao`, `qtd_os` |
| Card: OS dentro da meta | `kpi_os_resolucao` | `dentro_da_meta` |

### 3. Ativos e saúde

| Visual | Tabela | Campos |
|---|---|---|
| Histograma: saúde das strings | `kpi_saude_string` | `health_score` |
| Barras: strings por faixa | `kpi_saude_string` | `faixa` (CRITICA, ALTA, MEDIA, OK) |
| Mapa por estado ou cidade | `dim_filial_mapa` | `state`, `city`, `saude_media`, `alertas_ativos` |
| Barras: placas por status e filial | `kpi_ativos_filial` | `placas_operacionais`, `placas_em_manutencao`, `placas_em_falha`, `placas_inativas`, `placas_em_estoque` |
| Barras: peças abaixo do mínimo | `kpi_estoque_filial` | `pecas_abaixo_minimo`, `pecas_cadastradas` |

O mapa usa cidade e estado porque não há latitude e longitude na base.

### 4. Uso do aplicativo (tracking)

| Visual | Tabela | Campos |
|---|---|---|
| Linha: eventos, sessões e usuários ativos por dia | `kpi_tracking_diario_empresa` | `event_date`, `eventos`, `sessoes`, `usuarios_ativos` |
| Barras: visualizações por tela | `kpi_tracking_tela` | `screen`, `visualizacoes` |
| Barras: tempo médio por tela | `kpi_tracking_tela` | `screen`, `tempo_medio_ms`, `tempo_mediano_ms` |
| Histograma: duração das sessões | `kpi_tracking_sessao` | `duracao_s` |
| Boxplot: eventos por sessão | `kpi_tracking_sessao` | `eventos` |
| Linha: taxa de erro da API | `kpi_tracking_api_diaria` | `event_date`, `endpoint`, `taxa_erro_pct`, `latencia_p95_ms` |
| Barras: ações por alvo | `kpi_tracking_acoes` | `alvo`, `acoes` |

## Datasets com nome da filial

Os campos de filial vêm de `dim_filial_mapa`. Cada dataset abaixo junta a tabela de KPI com essa dimensão.

```sql
SELECT f.unit_name, f.city, f.state, k.*
FROM workspace.curated.kpi_saude_filial k
JOIN workspace.curated.dim_filial_mapa f USING (company_unit_key)
```

```sql
SELECT f.unit_name, k.*
FROM workspace.curated.kpi_saude_diaria_filial k
JOIN workspace.curated.dim_filial_mapa f USING (company_unit_key)
```

```sql
SELECT f.unit_name, k.*
FROM workspace.curated.kpi_producao_filial_mes k
JOIN workspace.curated.dim_filial_mapa f USING (company_unit_key)
```

```sql
SELECT f.unit_name, k.*
FROM workspace.curated.kpi_manutencao_filial_mes k
JOIN workspace.curated.dim_filial_mapa f USING (company_unit_key)
```

```sql
SELECT f.unit_name, k.*
FROM workspace.curated.kpi_os_resolucao k
JOIN workspace.curated.dim_filial_mapa f USING (company_unit_key)
```

O `kpi_is_filial` já traz `unit_name`. As tabelas de tracking não precisam de join.

## Regras

- Cada card de desvio mostra o valor, a meta e a diferença, usando `desvio_limiar`, `desvio_meta`, `desvio_meta_resolucao` e `desvio_orcamento_pct`.
- Os parâmetros de meta e limiar ficam na célula de parâmetros do notebook `apollo_curated_operacional`. Mudar a meta exige rodar a `curated` de novo.
- O dashboard fica publicado no workspace do pipeline, para ler as tabelas geradas pelo Job.
