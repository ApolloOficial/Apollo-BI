# Dashboard do Apollo BI

Desenho do dashboard AI/BI do Databricks, uma página única chamada **Métricas Solares**. Toda consulta lê o schema `workspace.curated`, gerado pelo pipeline. Nenhum gráfico lê a fonte direta. As consultas de cada dataset estão em [DASHBOARD_DATASETS.md](DASHBOARD_DATASETS.md).

## Cabeçalho

- Título "Métricas Solares" e uma linha explicando que o dashboard mostra a saúde e a sustentabilidade dos ativos solares.
- Dois carimbos: **Horário de Emissão** (hora atual de São Paulo) e **Data de Atualização** (última data com dados de saúde). Dataset `ds_carimbos`.

## Filtros

Ficam em uma barra fixa no topo, valem para a página inteira e têm botão para voltar ao padrão.

| Filtro | Como funciona | Datasets que respondem |
|---|---|---|
| Tipo de Data | Parâmetro `tipo_data` com as opções Dia, Semana e Mês. Muda o agrupamento do eixo de tempo nos gráficos diários | `ds_saude_diaria`, `ds_is_diario`, `ds_usuarios_diario` |
| Data Início e Data Fim | Intervalo de datas ligado aos campos `full_date`, `event_date`, `dia` e `mes` | Todos os datasets que têm data |
| Filial | Campo `unit_name`, obtido pelo join com `dim_filial_mapa` pelo `company_unit_key` | Todos os datasets operacionais |
| Empresa | Campo `company_id` | Todos os datasets de tracking, que não têm filial |

Os datasets de retrato atual não têm data, então o filtro de período não os afeta: `kpi_saude_string`, `kpi_saude_filial`, `kpi_ativos_filial`, `kpi_alertas_filial`, `kpi_realocacao_filial`, `kpi_estoque_peca`, `kpi_estoque_filial`, `kpi_is_string`, `kpi_is_filial` e `dim_filial_mapa`.

## Cards de comparação

Os blocos de métrica diária têm 4 cards: **Semana Atual**, **S-1**, **Mês Atual** e **M-1**.

- A referência é a data mais recente da base, e não a data de hoje.
- A semana vai de segunda a domingo. S-1 é a semana anterior e M-1 é o mês anterior.
- O valor é a média diária do período. Em usuários ativos, é a média diária de usuários ativos, e não o total de usuários únicos.
- Produção e manutenção só existem por mês, então os blocos delas têm só Mês Atual e M-1.

## Página: Métricas Solares

A página é lida de cima para baixo, em faixas. Cada bloco de métrica tem o título, os 4 cards de comparação e um gráfico combinado: barras do período e uma linha de média móvel de 30 dias.

### Faixa 1: Índices de sustentabilidade

| Visual | Dataset | Campos | Meta |
|---|---|---|---|
| Cards: IS Geral (Semana Atual, S-1, Mês Atual, M-1) e desvio | `ds_is_cards` | `geral_*`, `desvio_meta_*` | `META_IS` = 80 |
| Cards: IS Energético e IS Ambiental (Semana Atual, S-1, Mês Atual, M-1) | `ds_is_cards` | `energetico_*`, `ambiental_*` | |
| Combinado: IS Geral por período e média móvel de 30 dias | `ds_is_diario` | `periodo`, `is_geral`, `media_movel_30` | |
| Barras: IS Energético e IS Ambiental por filial | `ds_is_diario` | `unit_name`, `is_energetico`, `is_ambiental` | |

### Faixa 2: Saúde das strings

| Visual | Dataset | Campos | Meta ou limiar |
|---|---|---|---|
| Cards: saúde média (Semana Atual, S-1, Mês Atual, M-1) | `ds_saude_cards` | `semana_atual`, `semana_anterior`, `mes_atual`, `mes_anterior` | |
| Combinado: saúde por período e média móvel de 30 dias | `ds_saude_diaria` | `periodo`, `saude_media`, `media_movel_30` | |
| Card: saúde média e desvio | `ds_saude_filial` | `saude_media`, `desvio_limiar` | `LIMIAR_SAUDE` = 70 |
| Card: strings abaixo do limiar | `ds_saude_filial` | `pct_abaixo_limiar` | 70 |

### Faixa 3: Produção e manutenção

| Visual | Dataset | Campos | Meta |
|---|---|---|---|
| Cards: energia gerada (Mês Atual e M-1) | `ds_producao_cards` | `mes_atual_kwh`, `mes_anterior_kwh` | |
| Barras: energia gerada por filial e mês | `ds_producao_mensal` | `mes`, `energia_kwh`, `kwh_por_kwp` | |
| Cards: tempo médio de resolução (Mês Atual e M-1) e desvio | `ds_manutencao_cards` | `resolucao_mes_atual`, `resolucao_mes_anterior`, `desvio_meta_mes_atual` | `META_RESOLUCAO_DIAS` = 7 |
| Card: OS atrasadas no mês atual | `ds_manutencao_cards` | `os_atrasadas_mes_atual` | |
| Barras empilhadas: OS por status e mês | `ds_manutencao_mensal` | `mes`, `os_abertas`, `os_em_andamento`, `os_concluidas`, `os_canceladas`, `os_atrasadas` | |
| Linha: desvio do orçamento por mês | `ds_manutencao_mensal` | `mes`, `desvio_orcamento_pct` | |
| Barras: OS por tipo | `ds_os_por_tipo` | `tipo_manutencao`, `qtd_os` | |
| Card: alertas ativos | `ds_alertas_filial` | `alertas_ativos` | |

### Faixa 4: Distribuições, ativos e mapa

| Visual | Dataset | Campos |
|---|---|---|
| Histograma: saúde das strings | `ds_saude_string` | `health_score` |
| Barras: strings por faixa (CRITICA, ALTA, MEDIA, OK) | `ds_saude_string` | `faixa` |
| Histograma: dias de resolução | `ds_os_resolucao` | `resolution_days` |
| Boxplot: dias de resolução por tipo de manutenção | `ds_os_resolucao` | `tipo_manutencao`, `resolution_days` |
| Card: OS dentro da meta | `ds_os_resolucao` | `dentro_da_meta` |
| Mapa por estado ou cidade | `ds_mapa` | `state`, `city`, `saude_media`, `alertas_ativos` |
| Barras: placas por status e filial | `ds_ativos_filial` | `placas_operacionais`, `placas_em_manutencao`, `placas_em_falha`, `placas_inativas`, `placas_em_estoque` |
| Barras: peças abaixo do mínimo | `ds_estoque_filial` | `pecas_abaixo_minimo`, `pecas_cadastradas` |

O mapa usa cidade e estado porque não há latitude e longitude na base.

### Faixa 5: Uso do aplicativo (tracking)

| Visual | Dataset | Campos |
|---|---|---|
| Cards: usuários ativos (Semana Atual, S-1, Mês Atual, M-1) | `ds_usuarios_cards` | `semana_atual`, `semana_anterior`, `mes_atual`, `mes_anterior` |
| Combinado: usuários ativos por período e média móvel de 30 dias | `ds_usuarios_diario` | `periodo`, `usuarios_ativos`, `media_movel_30` |
| Linha: eventos e sessões por período | `ds_usuarios_diario` | `periodo`, `eventos`, `sessoes` |
| Histograma: duração das sessões | `ds_tracking_sessao` | `duracao_s` |
| Boxplot: eventos por sessão | `ds_tracking_sessao` | `eventos` |
| Barras: visualizações por tela | `ds_tracking_tela` | `screen`, `visualizacoes` |
| Barras: tempo médio e mediano por tela | `ds_tracking_tela` | `screen`, `tempo_medio_ms`, `tempo_mediano_ms` |
| Linha: taxa de erro da API | `ds_tracking_api` | `event_date`, `endpoint`, `taxa_erro_pct`, `latencia_p95_ms` |
| Barras: ações por alvo | `ds_tracking_acoes` | `alvo`, `acoes` |

## Rubrica da disciplina

| Requisito | Onde está no dashboard |
|---|---|
| Conectado à fonte de dados da aplicação | Todos os datasets leem `workspace.curated` |
| Gráficos de barra e linha | Faixas 1, 2, 3 e 5 |
| Cards de KPIs e desvios | Faixas 1, 2, 3 e 5 |
| Histogramas e boxplots | Faixas 4 e 5 |
| Mapa | Faixa 4 |
| Filtros interativos | Barra de filtros no topo |

## Regras

- Cada card de desvio mostra o valor, a meta e a diferença, usando `desvio_limiar`, `desvio_meta`, `desvio_meta_resolucao` e `desvio_orcamento_pct`.
- Os parâmetros de meta e limiar ficam na célula de parâmetros de `apollo-pipeline/transformations/apollo_curated_operacional.py`. Mudar a meta exige rodar a pipeline `Apollo_BI` de novo.
- O IS semanal e o mensal são a média dos IS diários do período. O IS acumulado de `kpi_is_filial` não muda com o filtro de período e não entra nesta página.
- O dashboard fica publicado no workspace do pipeline, para ler as tabelas geradas pelo Job.
