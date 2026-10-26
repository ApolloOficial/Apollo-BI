# Guia do pipeline Apollo BI

## Sobre o projeto

O **Apollo BI** é a camada de análise da plataforma SaaS multi-tenant Apollo. Ele lê o data mart do Apollo
no Neon e o tracking do app mobile no MongoDB, trata os dados em três camadas e calcula os KPIs e os índices que aparecem
na aba **Métricas** da web. O **Gerente** acessa o dashboard e vê
apenas a própria filial.

> A unidade monitorada no Apollo é a **string**. A hierarquia dos ativos é:
> empresa, filial, inversor, string e placa.

## Dados de origem

O pipeline tem duas fontes, cada uma com a sua trilha de notebooks:

| Fonte | Trilha | O que traz |
|---|---|---|
| Neon (PostgreSQL), schema `dm` | `operacional` | Dados operacionais do Apollo: ativos, saúde, manutenção, alertas, realocação e estoque |
| MongoDB, coleção `tracking_events` | `tracking` | Eventos de uso do app mobile: telas, ações de toque, chamadas de API e sessões |

### Neon: dados operacionais

A fonte é o banco do Apollo no Neon (PostgreSQL, 32 tabelas). O BI não lê as tabelas
operacionais direto: ele lê o schema **`dm`** (data mart), que expõe 20 views.

| Tipo | Views do `dm` |
|---|---|
| Dimensões | `dim_company`, `dim_segment`, `dim_company_unit`, `dim_panel_model`, `dim_inverter`, `dim_string`, `dim_panel`, `dim_employee`, `dim_date`, `dim_maintenance_type`, `dim_part` |
| Fatos | `fact_string_daily_performance`, `fact_string_health`, `fact_maintenance`, `fact_part_consumption`, `fact_daily_active_user`, `fact_warning`, `fact_relocation`, `fact_stock` |
| Apoio | `vw_maintenance_chain` (cadeia de OS filhas, usada para medir retrabalho) |

As views `dim_maintenance_type`, `dim_part`, `fact_warning`, `fact_relocation` e
`fact_stock` foram criadas para o BI.

As medições por string (`power_w`, `voltage_v`, `current_a`, `generated_energy_kwh`,
`fault_code`) vêm do inversor. Na demonstração, elas são geradas por um simulador, e
a coluna `data_source` registra a origem de cada leitura.

### MongoDB: tracking do mobile

A coleção `tracking_events` guarda um documento por evento do app. Os campos aninhados
em `properties` são achatados para colunas (`properties_duration_ms`,
`properties_http_status` e outras).

| Evento (`event_name`) | O que registra |
|---|---|
| `app_opened`, `app_foregrounded` | Abertura e retorno do app |
| `screen_viewed`, `screen_exited` | Tela exibida e tempo de permanência |
| `ui_action` | Toque do usuário (alvo e forma de entrada) |
| `api_request_completed` | Chamada de API (endpoint, status HTTP e latência) |

O tracking traz `company_id`, mas não a filial. Por isso os KPIs dele são por empresa.

## Índices de sustentabilidade (IS)

| Índice | O que mede | Situação |
|---|---|---|
| **IS Energético** | Queda brusca na captação e desempenho abaixo do padrão da etiqueta do equipamento | Calculado |
| **IS Ambiental** | Vida útil do painel, usando a garantia do modelo de placa como vida útil | Calculado (precisa de `warranty_years` preenchido no `panel_model`) |
| **IS Financeiro** | Custo do painel comparado ao que seria pago de energia sem o sistema | Fora do cálculo: não existem consumo e tarifa no banco |
| **IS Geral** | Média dos IS calculados (Energético e Ambiental), por filial | Calculado |

Regras de cálculo definidas pelo grupo:

- Tudo o que não existir no banco atual fica de fora do cálculo. O schema não será alterado para isso;
- cada IS é calculado por string e depois agregado por filial;
- não existe tabela por painel na camada final.

Como cada IS é calculado:

- **IS Energético:** média de dois percentuais por string. O primeiro é a parcela dos
  dias com pico de potência dentro do padrão da etiqueta (`peak_to_nominal_ratio`).
  O segundo é a parcela das medições de saúde sem queda brusca (`health_delta`);
- **IS Ambiental:** parcela da garantia que ainda resta, a partir da idade média das
  placas instaladas e ativas da string.

## KPIs calculados

Além dos IS, a camada `curated` entrega KPIs para o dashboard.

### Trilha operacional

| Tabela da `curated` | Conteúdo |
|---|---|
| `kpi_saude_string`, `kpi_saude_filial`, `kpi_saude_diaria_filial` | Saúde atual, % de strings abaixo de 70 e evolução diária |
| `kpi_producao_filial_mes` | Energia gerada, kWh por kWp e leituras com falha |
| `kpi_manutencao_filial_mes`, `kpi_os_por_tipo` | Status das OS, atrasadas, tempo de resolução, custo, desvio de orçamento, cancelamento e retrabalho |
| `kpi_ativos_filial` | Placas por status e taxa de ativação |
| `kpi_alertas_filial` | Alertas por status, alertas sem OS e horas até a OS |
| `kpi_realocacao_filial` | Solicitações de realocação, internas e externas |
| `kpi_estoque_peca`, `kpi_estoque_filial` | Estoque de peças e itens abaixo do mínimo |
| `kpi_is_string`, `kpi_is_filial` | IS Energético, IS Ambiental e IS Geral |
| `dim_filial_mapa` | Cidade, estado e indicadores de risco de cada filial |

### Trilha tracking

| Tabela da `curated` | Conteúdo |
|---|---|
| `kpi_tracking_diario_empresa` | Eventos, sessões, usuários ativos e aberturas do app por dia |
| `kpi_tracking_sessao` | Uma linha por sessão: duração, eventos e telas distintas |
| `kpi_tracking_tela` | Visualizações e tempo médio e mediano por tela |
| `kpi_tracking_acoes` | Toques por tela, alvo e forma de entrada |
| `kpi_tracking_api_diaria` | Requisições, erros, taxa de erro e latência (média e p95) por endpoint |

O dia do evento usa o horário de São Paulo. Status HTTP a partir de 400 conta como erro.

### Parâmetros

Parâmetros do cálculo, na célula de parâmetros do notebook `apollo_curated_operacional`:

| Parâmetro | Valor | Situação |
|---|---|---|
| `LIMIAR_SAUDE` | 70 | Definido pelo grupo para o BI: igual ao alerta do sistema, strings com saúde abaixo de 70 contam como abaixo do limiar |
| `PADRAO_MIN` | 0,80 | Definido pelo grupo: razão pico/nominal mínima para estar dentro do padrão da etiqueta |
| `QUEDA_BRUSCA_PTS` | 10 | Definido pelo grupo: queda de saúde entre duas medições que conta como queda brusca |
| `META_IS` | 80 | Definido pelo grupo: meta do IS Geral |
| `META_RESOLUCAO_DIAS` | 7 | Definido pelo grupo: meta do tempo médio de resolução da OS |

## Funcionalidades previstas

- Conexão com o Neon por JDBC e com o MongoDB por `pymongo`, com credenciais em um arquivo `.env` lido com `python-dotenv`;
- ingestão das 20 views do `dm` e da coleção `tracking_events` sem alteração de valores na camada `raw`;
- limpeza na camada `cleaned`: 16 tratamentos na trilha operacional e 10 na trilha tracking, com o efeito de cada um registrado em `cleaning_log` e `cleaning_log_mongo`;
- cálculo dos KPIs e dos IS na camada `curated`;
- dashboard (Lakeview) embutido na aba Métricas da web;
- acesso restrito por filial (Row Filter) para Gerente e Analista;
- Job no Databricks para executar o pipeline sem rodar célula por célula;
- relatório gerencial com o fluxo do pipeline e a relação com os KPIs.

## Arquitetura

O fluxo segue a arquitetura medalhão, com nomes descritivos para cada camada:

```mermaid
flowchart LR
    I[Inversor ou simulador] --> N[(Neon PostgreSQL)]
    N --> DM[Views do schema dm]
    DM --> C[Notebook core]
    M[App mobile] --> MG[(MongoDB tracking_events)]
    C --> R[raw]
    MG --> R
    R --> CL[cleaned]
    CL --> CU[curated]
    CU --> D[Dashboard Lakeview]
    D --> W[Web Apollo: aba Métricas]
    J[Job do Databricks] -. orquestra .-> R
    J -. orquestra .-> CL
    J -. orquestra .-> CU
```

| Camada | Equivalente medalhão | O que contém |
|---|---|---|
| `raw` | Bronze | Cópia fiel das 20 views do `dm` e da coleção `tracking_events` (tudo como texto), sem alterar os dados |
| `cleaned` | Silver | As mesmas tabelas, com tipos convertidos e os tratamentos de qualidade aplicados |
| `curated` | Gold | KPIs e IS prontos para o dashboard |

Tratamentos da `cleaned`:

| Trilha | Tratamentos |
|---|---|
| `operacional` (16) | Espaços extras, nulo escrito como texto, caixa e grafia das categorias, categoria fora do domínio, NaN e infinito, negativo impossível, valor fora da faixa, outlier extremo de geração, data no futuro, data de preenchimento padrão, ordem cronológica, linha duplicada, chave nula, chave duplicada e chave estrangeira órfã |
| `tracking` (10) | Espaços extras, nulo escrito como texto, caixa das categorias, UUID inválido, conversão de tipos, NaN e infinito, valor fora da faixa, data impossível, evento sem campo obrigatório e evento duplicado |

Cada notebook da `cleaned` grava uma tabela de log (`cleaning_log` e `cleaning_log_mongo`)
com as linhas removidas e os nulos criados por tratamento. Elas são a evidência da etapa
de tratamento para o relatório gerencial.

```text
apollo-bi/
├── apollo-core/
│   ├── apollo_connection_neon_core    # Conexão com o Neon, lista de views e criação dos schemas
│   ├── apollo_connection_test         # Teste da conexão com o Neon (conta as views e mostra os dados)
│   └── apollo_connection_mongo_test   # Teste da conexão com o MongoDB (ping, coleções e conversão)
├── apollo-pipeline/
│   ├── apollo_raw_operacional         # Neon (dm) para a camada raw
│   ├── apollo_cleaned_operacional     # raw para a camada cleaned
│   ├── apollo_curated_operacional     # cleaned para a camada curated (KPIs e IS)
│   ├── apollo_raw_tracking            # MongoDB (tracking_events) para a camada raw
│   ├── apollo_cleaned_tracking        # raw para a camada cleaned
│   └── apollo_curated_tracking        # cleaned para a camada curated (KPIs de uso do app)
├── apollo_manager/                    # Dashboard principal (a criar)
└── README.md
```

As tabelas são salvas como `workspace.raw`, `workspace.cleaned` e
`workspace.curated`. O catálogo, o schema de origem e a lista de views ficam nas
variáveis do notebook core. Os notebooks da trilha `tracking` não usam o core e
podem rodar em paralelo com os da trilha `operacional`.

## Tecnologias

| Tecnologia | Uso no projeto |
|---|---|
| Databricks | Execução dos notebooks, Jobs e dashboard Lakeview |
| PySpark | Leitura, tratamento e transformação dos dados |
| Delta Lake | Armazenamento das tabelas de cada camada |
| Neon (PostgreSQL) | Banco operacional do Apollo, fonte do BI |
| MongoDB Atlas | Tracking de uso do app mobile, fonte do BI |
| JDBC | Conexão entre Databricks e Neon |
| pymongo | Conexão entre Databricks e MongoDB |
| python-dotenv | Leitura das credenciais do Neon a partir do arquivo `.env` |
| React | Web do Apollo, onde o dashboard é embutido |
| GitHub | Versionamento e revisão do código |

## Como executar

### Pré-requisitos

- Conta própria no Databricks Free Edition (cada integrante usa a sua);
- Conta no GitHub com permissão de escrita neste repositório;
- Personal Access Token do GitHub com escopo `repo`;
- as views do `dm` criadas no Neon, inclusive as 5 novas;
- acesso ao MongoDB: no Atlas, em **Network Access**, o IP do Databricks precisa estar liberado (o Free Edition não tem IP fixo, então a liberação é `0.0.0.0/0`).

> Não é preciso instalar nada na máquina. Todo o processamento roda no Databricks.

### 1. Conecte o GitHub ao Databricks

No Databricks, acesse **Settings > Linked accounts > Git integration**, escolha
GitHub e informe o seu Personal Access Token.

### 2. Crie a Git folder

Em **Workspace > Create > Git folder**, cole a URL do repositório e confirme:

```text
https://github.com/<ORGANIZACAO>/<REPOSITORIO>.git
```

Depois, troque para a sua branch no menu de branches, no topo da pasta.

### 3. Configure as credenciais do Neon e do MongoDB

Cada integrante cria o seu arquivo `.env` na raiz da Git folder:

| Variável | Fonte | Lida por |
|---|---|---|
| `NEON_HOST`, `NEON_DB`, `NEON_USER`, `NEON_PASSWORD` | Connection string do projeto no painel do Neon | `apollo_connection_neon_core` |
| `MONGO_URI` | Connection string do cluster no painel do Atlas | notebooks da trilha `tracking` |
| `MONGO_DB` | Nome do banco, só se não estiver na `MONGO_URI` | notebooks da trilha `tracking` |

> O `.env` fica fora do repositório. Nunca escreva credenciais em notebooks, commits
> ou mensagens do grupo.

### 4. Teste as conexões

- `apollo-core/apollo_connection_test`: use **Run all**. O resultado esperado é a contagem das 20 views do `dm` e a exibição dos dados;
- `apollo-core/apollo_connection_mongo_test`: troque `NOME_DA_COLECAO` por `tracking_events` e use **Run all**. O resultado esperado é o `ping`, a lista de coleções com as contagens e uma amostra dos eventos.

### 5. Execute o pipeline

Rode os notebooks de `apollo-pipeline` nesta ordem. As duas trilhas são independentes
e podem rodar em paralelo, mas dentro de cada trilha a ordem é fixa:

| Etapa | Trilha operacional | Trilha tracking |
|---|---|---|
| 1 | `apollo_raw_operacional` | `apollo_raw_tracking` |
| 2 | `apollo_cleaned_operacional` | `apollo_cleaned_tracking` |
| 3 | `apollo_curated_operacional` | `apollo_curated_tracking` |

### 6. Job

O Job executa as seis tarefas na ordem acima, com as etapas da mesma trilha encadeadas.
O Job precisa usar a pasta onde está o `.env`, porque o arquivo não vai para o Git.

## Configuração do notebook core

| Variável | Padrão | Descrição |
|---|---|---|
| `CATALOG` | `workspace` | Catálogo onde as camadas são criadas |
| `NEON_SCHEMA` | `dm` | Schema de origem no Neon, onde ficam as views do data mart |
| `VIEWS` | 20 views do `dm` | Lista das views que a `raw` copia do Neon |

## Requisitos da disciplina: Business Intelligence

Esta seção relaciona a implementação aos critérios definidos para a disciplina em
maio de 2026.

| Requisito | Situação | Evidência no projeto |
|---|:---:|---|
| Dashboard conectado à fonte de dados da aplicação | ⏳ | Será construído sobre a camada `curated` |
| Gráficos de barra e linha | ⏳ | Evolução da saúde, produção e manutenção por filial e mês; uso do app por dia |
| Cards de KPIs e desvios | ⏳ | Saúde contra o limiar, IS Geral contra a meta, tempo de resolução contra a meta |
| Histogramas e boxplots | ⏳ | Distribuição da saúde das strings, da duração das sessões e do tempo de resolução das OS |
| Mapa, se houver dados geográficos | ⏳ | `dim_filial_mapa` traz cidade e estado; não há latitude e longitude, então o mapa é por cidade ou estado |
| Filtros interativos (período, filial) | ⏳ | Planejado no dashboard; o tracking filtra por empresa, não por filial |
| Relatório gerencial (base, problema, tratamento, KPIs) | ⏳ | O `cleaning_log` e o `cleaning_log_mongo` documentam o tratamento |
| Pipeline no Databricks (leitura, tratamento, transformação, KPIs, base final) | 🚧 | Seis notebooks escritos (duas trilhas); falta validar a execução completa |
| Pipeline usando a fonte gerada pela aplicação | ✅ | `apollo_raw_operacional` lê o `dm` do Neon e `apollo_raw_tracking` lê o `tracking_events` do MongoDB |
| Job para executar o pipeline sem rodar célula por célula (manual ou agendado) | ⏳ | Planejado |
| Dashboard construído a partir da base final do pipeline | ⏳ | Será lido do schema `workspace.curated` |
| Evidências do Job (configuração, histórico, status, resultado) | ⏳ | Planejado |
| Relatório explica o fluxo, as etapas automatizadas e a relação com KPIs e dashboard | ⏳ | Planejado |

**Legenda:** ✅ concluído · 🚧 em desenvolvimento · ⏳ planejado

## Próximos passos

- Validar a execução completa dos seis notebooks no Databricks;
- criar a tabela `kpi_os_resolucao` (uma linha por OS) para o boxplot do tempo de resolução;
- criar o Job com as seis tarefas e guardar a evidência (configuração, histórico, status e resultado);
- construir o dashboard a partir da base `curated`;
- escrever o relatório gerencial com a base, o problema, o tratamento, os KPIs, o fluxo do pipeline e a relação com o dashboard;
- adicionar o Row Filter por filial quando os dashboards estiverem finalizados;
- deixar o React pronto para receber o embed e ativar o trial de 14 dias do Databricks de 4 a 5 dias antes da apresentação.

## Contribuição

Regras para todo o grupo:

1. **Nunca edite na `main`.** Cada pessoa trabalha em uma branch própria;
2. antes de começar, faça **Pull** da `main` e crie a sua branch: `feat/<seu-nome>-<assunto>`;
3. avise o grupo qual notebook você vai editar, para evitar conflito de merge;
4. faça commits curtos e claros, seguindo o padrão
   [Conventional Commits](https://www.conventionalcommits.org/pt-br/v1.0.0/);
5. envie a branch (**Push**) e abra um **Pull Request** descrevendo a alteração;
6. outra pessoa do grupo revisa e faz o merge na `main`.

Fluxo do dia a dia:

1. Abra a sua Git folder e faça **Pull**;
2. confirme que está na sua branch;
3. edite apenas o notebook combinado e rode para testar;
4. faça **Commit & Push** e abra o Pull Request.

O notebook `apollo_connection_neon_core` é versionado em `.py` e os demais em `.ipynb`.
Limpe as saídas das células antes de fazer o commit.

---

<div align="center">

Desenvolvido pela equipe **Apollo** para o Projeto Interdisciplinar 2026. 🚀

</div>

