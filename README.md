# ☀️ Apollo BI

<p align="center">
  <strong>Indicadores de saúde, manutenção e sustentabilidade dos ativos solares</strong>
</p>

<p align="center">
  <img alt="Databricks" src="https://img.shields.io/badge/Databricks-FF3621">
  <img alt="PySpark" src="https://img.shields.io/badge/PySpark-E25A1C">
  <img alt="Delta Lake" src="https://img.shields.io/badge/Delta%20Lake-00ADD4">
  <img alt="PostgreSQL" src="https://img.shields.io/badge/PostgreSQL-4169E1">
  <img alt="MongoDB" src="https://img.shields.io/badge/MongoDB-47A248">
</p>

O Apollo BI trata dados operacionais e eventos de uso da plataforma Apollo para calcular indicadores de ativos fotovoltaicos. O pipeline utiliza Databricks, PySpark e Delta Lake, com fontes no Neon (PostgreSQL) e MongoDB.

## Como funciona

- **Origem operacional:** views do schema `dm` no Neon.
- **Origem de uso:** coleção `tracking_events` no MongoDB.
- **Tratamento:** duas trilhas independentes passam pelas camadas `raw`, `cleaned` e `curated`.
- **Consumo:** a camada final prepara indicadores para dashboards e análise gerencial.

```text
Neon / MongoDB → raw → cleaned → curated → indicadores e dashboards
```

## Início rápido

Requisitos: workspace Databricks e acesso autorizado às duas fontes.

1. Importe o repositório como Git folder no Databricks.
2. Configure as conexões seguindo o [guia do pipeline](docs/PIPELINE.md).
3. Valide os notebooks de conexão antes de executar a carga.
4. Execute cada trilha na ordem `raw → cleaned → curated`.

Os nomes dos notebooks e a configuração completa estão no guia. Credenciais devem permanecer fora do Git e das saídas das células.

## Pipeline e indicadores

As trilhas operacional e de tracking possuem três notebooks cada. Os indicadores abrangem saúde dos ativos, manutenção, sustentabilidade e uso do aplicativo. A situação dos dashboards, Job e evidências deve ser consultada na [tabela de requisitos](docs/PIPELINE.md#requisitos-da-disciplina-business-intelligence); itens planejados não representam entregas validadas.

## Testes

Execute os notebooks com **Run all**, confira contagens e tratamentos entre as camadas e registre as evidências do resultado. Quando houver alteração no Job, valide sua execução completa. Limpe saídas com dados ou credenciais antes do commit.

## Documentação

- [Fontes, configuração, notebooks, requisitos e próximos passos](docs/PIPELINE.md)
- [API operacional](https://github.com/ApolloOficial/Apollo-API-Spring)
- [Interface web](https://github.com/ApolloOficial/Apollo-Web)

