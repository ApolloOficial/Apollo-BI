# Datasets do dashboard Métricas Solares

Cada seção é um dataset do dashboard AI/BI do Databricks, e o SQL abaixo dela é a consulta para colar no editor. Todos leem `workspace.curated`. O desenho do dashboard está em [DASHBOARD.md](DASHBOARD.md).

## Antes de começar

- Os datasets `ds_saude_diaria`, `ds_is_diario` e `ds_usuarios_diario` usam o parâmetro `tipo_data`. Crie o parâmetro no dataset com valor padrão `Dia` e as opções `Dia`, `Semana` e `Mês`.
- A média móvel de 30 dias usa a janela `RANGE BETWEEN INTERVAL 29 DAYS PRECEDING AND CURRENT ROW`. Se o Databricks recusar essa sintaxe, troque por `ROWS BETWEEN 29 PRECEDING AND CURRENT ROW`, que só vale se a série não tiver dias faltando.
- As consultas de tracking supõem que `event_date` é do tipo data. Se for texto, troque `event_date` por `to_date(event_date)`.
- Nas consultas de cards, a "referência" é a data mais recente da base (`MAX` da data), e não a data de hoje.

## 1. ds_carimbos

Hora de emissão em São Paulo e última data com dados de saúde, para os carimbos do cabeçalho.

```sql
SELECT
  from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo') AS horario_emissao,
  (SELECT MAX(full_date) FROM workspace.curated.kpi_saude_diaria_filial) AS data_atualizacao
```

## 2. ds_is_diario

IS por filial e dia, com o período escolhido em `tipo_data` e a média móvel de 30 dias do IS Geral.

```sql
SELECT
  k.unit_name,
  k.full_date,
  CASE :tipo_data
    WHEN 'Semana' THEN trunc(k.full_date, 'WEEK')
    WHEN 'Mês' THEN trunc(k.full_date, 'MONTH')
    ELSE k.full_date
  END AS periodo,
  k.is_energetico,
  k.is_ambiental,
  k.is_geral,
  k.desvio_meta,
  ROUND(AVG(k.is_geral) OVER (
    PARTITION BY k.company_unit_key
    ORDER BY k.full_date
    RANGE BETWEEN INTERVAL 29 DAYS PRECEDING AND CURRENT ROW
  ), 2) AS media_movel_30
FROM workspace.curated.kpi_is_filial_diario k
```

## 3. ds_is_cards

Média dos IS na semana e no mês da data de referência e nos períodos anteriores, mais o desvio contra a meta do IS Geral.

```sql
WITH base AS (
  SELECT
    unit_name,
    full_date,
    is_geral,
    is_energetico,
    is_ambiental,
    desvio_meta,
    MAX(full_date) OVER () AS ref
  FROM workspace.curated.kpi_is_filial_diario
)
SELECT
  unit_name,
  ROUND(AVG(CASE WHEN trunc(full_date, 'WEEK') = trunc(ref, 'WEEK') THEN is_geral END), 2) AS geral_semana_atual,
  ROUND(AVG(CASE WHEN trunc(full_date, 'WEEK') = date_sub(trunc(ref, 'WEEK'), 7) THEN is_geral END), 2) AS geral_semana_anterior,
  ROUND(AVG(CASE WHEN trunc(full_date, 'MONTH') = trunc(ref, 'MONTH') THEN is_geral END), 2) AS geral_mes_atual,
  ROUND(AVG(CASE WHEN trunc(full_date, 'MONTH') = add_months(trunc(ref, 'MONTH'), -1) THEN is_geral END), 2) AS geral_mes_anterior,
  ROUND(AVG(CASE WHEN trunc(full_date, 'WEEK') = trunc(ref, 'WEEK') THEN desvio_meta END), 2) AS desvio_meta_semana_atual,
  ROUND(AVG(CASE WHEN trunc(full_date, 'MONTH') = trunc(ref, 'MONTH') THEN desvio_meta END), 2) AS desvio_meta_mes_atual,
  ROUND(AVG(CASE WHEN trunc(full_date, 'WEEK') = trunc(ref, 'WEEK') THEN is_energetico END), 2) AS energetico_semana_atual,
  ROUND(AVG(CASE WHEN trunc(full_date, 'WEEK') = date_sub(trunc(ref, 'WEEK'), 7) THEN is_energetico END), 2) AS energetico_semana_anterior,
  ROUND(AVG(CASE WHEN trunc(full_date, 'MONTH') = trunc(ref, 'MONTH') THEN is_energetico END), 2) AS energetico_mes_atual,
  ROUND(AVG(CASE WHEN trunc(full_date, 'MONTH') = add_months(trunc(ref, 'MONTH'), -1) THEN is_energetico END), 2) AS energetico_mes_anterior,
  ROUND(AVG(CASE WHEN trunc(full_date, 'WEEK') = trunc(ref, 'WEEK') THEN is_ambiental END), 2) AS ambiental_semana_atual,
  ROUND(AVG(CASE WHEN trunc(full_date, 'WEEK') = date_sub(trunc(ref, 'WEEK'), 7) THEN is_ambiental END), 2) AS ambiental_semana_anterior,
  ROUND(AVG(CASE WHEN trunc(full_date, 'MONTH') = trunc(ref, 'MONTH') THEN is_ambiental END), 2) AS ambiental_mes_atual,
  ROUND(AVG(CASE WHEN trunc(full_date, 'MONTH') = add_months(trunc(ref, 'MONTH'), -1) THEN is_ambiental END), 2) AS ambiental_mes_anterior
FROM base
GROUP BY unit_name
```

## 4. ds_saude_diaria

Saúde média por filial e dia, com o período escolhido em `tipo_data` e a média móvel de 30 dias.

```sql
SELECT
  f.unit_name,
  k.full_date,
  CASE :tipo_data
    WHEN 'Semana' THEN trunc(k.full_date, 'WEEK')
    WHEN 'Mês' THEN trunc(k.full_date, 'MONTH')
    ELSE k.full_date
  END AS periodo,
  k.saude_media,
  ROUND(AVG(k.saude_media) OVER (
    PARTITION BY k.company_unit_key
    ORDER BY k.full_date
    RANGE BETWEEN INTERVAL 29 DAYS PRECEDING AND CURRENT ROW
  ), 2) AS media_movel_30
FROM workspace.curated.kpi_saude_diaria_filial k
JOIN workspace.curated.dim_filial_mapa f USING (company_unit_key)
```

## 5. ds_saude_cards

Saúde média na semana e no mês da data de referência e nos períodos anteriores.

```sql
WITH base AS (
  SELECT
    f.unit_name,
    k.full_date,
    k.saude_media,
    MAX(k.full_date) OVER () AS ref
  FROM workspace.curated.kpi_saude_diaria_filial k
  JOIN workspace.curated.dim_filial_mapa f USING (company_unit_key)
)
SELECT
  unit_name,
  ROUND(AVG(CASE WHEN trunc(full_date, 'WEEK') = trunc(ref, 'WEEK') THEN saude_media END), 2) AS semana_atual,
  ROUND(AVG(CASE WHEN trunc(full_date, 'WEEK') = date_sub(trunc(ref, 'WEEK'), 7) THEN saude_media END), 2) AS semana_anterior,
  ROUND(AVG(CASE WHEN trunc(full_date, 'MONTH') = trunc(ref, 'MONTH') THEN saude_media END), 2) AS mes_atual,
  ROUND(AVG(CASE WHEN trunc(full_date, 'MONTH') = add_months(trunc(ref, 'MONTH'), -1) THEN saude_media END), 2) AS mes_anterior
FROM base
GROUP BY unit_name
```

## 6. ds_saude_filial

Saúde atual por filial, com o desvio contra o limiar. Não tem data.

```sql
SELECT f.unit_name, f.city, f.state, k.*
FROM workspace.curated.kpi_saude_filial k
JOIN workspace.curated.dim_filial_mapa f USING (company_unit_key)
```

## 7. ds_saude_string

Saúde atual de cada string, com a faixa. Alimenta o histograma e as barras por faixa. Não tem data.

```sql
SELECT f.unit_name, k.*
FROM workspace.curated.kpi_saude_string k
JOIN workspace.curated.dim_filial_mapa f USING (company_unit_key)
```

## 8. ds_producao_mensal

Energia gerada por filial e mês. O campo `mes` é uma data, para o filtro de período valer.

```sql
SELECT
  f.unit_name,
  to_date(concat(k.ano_mes, '-01')) AS mes,
  k.energia_kwh,
  k.kwh_por_kwp,
  k.leituras_com_falha
FROM workspace.curated.kpi_producao_filial_mes k
JOIN workspace.curated.dim_filial_mapa f USING (company_unit_key)
```

## 9. ds_producao_cards

Energia gerada no mês da data de referência e no mês anterior.

```sql
WITH base AS (
  SELECT
    f.unit_name,
    k.ano_mes,
    k.energia_kwh,
    MAX(k.ano_mes) OVER () AS ref
  FROM workspace.curated.kpi_producao_filial_mes k
  JOIN workspace.curated.dim_filial_mapa f USING (company_unit_key)
)
SELECT
  unit_name,
  SUM(CASE WHEN ano_mes = ref THEN energia_kwh END) AS mes_atual_kwh,
  SUM(CASE WHEN ano_mes = date_format(add_months(to_date(concat(ref, '-01')), -1), 'yyyy-MM') THEN energia_kwh END) AS mes_anterior_kwh
FROM base
GROUP BY unit_name
```

## 10. ds_manutencao_mensal

Ordens de serviço, tempo de resolução e desvios por filial e mês.

```sql
SELECT
  f.unit_name,
  to_date(concat(k.ano_mes, '-01')) AS mes,
  k.os_abertas,
  k.os_em_andamento,
  k.os_concluidas,
  k.os_canceladas,
  k.os_atrasadas,
  k.tempo_medio_resolucao_dias,
  k.desvio_meta_resolucao,
  k.desvio_orcamento_pct
FROM workspace.curated.kpi_manutencao_filial_mes k
JOIN workspace.curated.dim_filial_mapa f USING (company_unit_key)
```

## 11. ds_manutencao_cards

Tempo médio de resolução no mês da data de referência e no mês anterior, o desvio contra a meta e as OS atrasadas.

```sql
WITH base AS (
  SELECT
    f.unit_name,
    k.ano_mes,
    k.tempo_medio_resolucao_dias,
    k.desvio_meta_resolucao,
    k.os_atrasadas,
    MAX(k.ano_mes) OVER () AS ref
  FROM workspace.curated.kpi_manutencao_filial_mes k
  JOIN workspace.curated.dim_filial_mapa f USING (company_unit_key)
)
SELECT
  unit_name,
  MAX(CASE WHEN ano_mes = ref THEN tempo_medio_resolucao_dias END) AS resolucao_mes_atual,
  MAX(CASE WHEN ano_mes = ref THEN desvio_meta_resolucao END) AS desvio_meta_mes_atual,
  MAX(CASE WHEN ano_mes = date_format(add_months(to_date(concat(ref, '-01')), -1), 'yyyy-MM') THEN tempo_medio_resolucao_dias END) AS resolucao_mes_anterior,
  MAX(CASE WHEN ano_mes = ref THEN os_atrasadas END) AS os_atrasadas_mes_atual
FROM base
GROUP BY unit_name
```

## 12. ds_os_resolucao

Uma linha por OS concluída. Alimenta o histograma, o boxplot e o card de OS dentro da meta.

```sql
SELECT
  f.unit_name,
  to_date(concat(k.ano_mes, '-01')) AS mes,
  k.tipo_manutencao,
  k.resolution_days,
  k.dentro_da_meta
FROM workspace.curated.kpi_os_resolucao k
JOIN workspace.curated.dim_filial_mapa f USING (company_unit_key)
```

## 13. ds_os_por_tipo

Quantidade de OS por filial, mês, tipo de manutenção e status.

```sql
SELECT
  f.unit_name,
  to_date(concat(k.ano_mes, '-01')) AS mes,
  k.tipo_manutencao,
  k.maintenance_status,
  k.qtd_os
FROM workspace.curated.kpi_os_por_tipo k
JOIN workspace.curated.dim_filial_mapa f USING (company_unit_key)
```

## 14. ds_alertas_filial

Alertas por filial. Não tem data.

```sql
SELECT f.unit_name, k.*
FROM workspace.curated.kpi_alertas_filial k
JOIN workspace.curated.dim_filial_mapa f USING (company_unit_key)
```

## 15. ds_ativos_filial

Placas por status em cada filial. Não tem data.

```sql
SELECT f.unit_name, k.*
FROM workspace.curated.kpi_ativos_filial k
JOIN workspace.curated.dim_filial_mapa f USING (company_unit_key)
```

## 16. ds_estoque_filial

Peças cadastradas e abaixo do mínimo por filial. Não tem data.

```sql
SELECT f.unit_name, k.*
FROM workspace.curated.kpi_estoque_filial k
JOIN workspace.curated.dim_filial_mapa f USING (company_unit_key)
```

## 17. ds_mapa

Filiais com cidade, estado, saúde média e alertas ativos, para o mapa.

```sql
SELECT *
FROM workspace.curated.dim_filial_mapa
```

## 18. ds_usuarios_diario

Atividade do app por empresa e dia, com o período escolhido em `tipo_data` e a média móvel de 30 dias dos usuários ativos.

```sql
SELECT
  company_id,
  event_date,
  CASE :tipo_data
    WHEN 'Semana' THEN trunc(event_date, 'WEEK')
    WHEN 'Mês' THEN trunc(event_date, 'MONTH')
    ELSE event_date
  END AS periodo,
  eventos,
  sessoes,
  usuarios_ativos,
  aberturas_app,
  ROUND(AVG(usuarios_ativos) OVER (
    PARTITION BY company_id
    ORDER BY event_date
    RANGE BETWEEN INTERVAL 29 DAYS PRECEDING AND CURRENT ROW
  ), 2) AS media_movel_30
FROM workspace.curated.kpi_tracking_diario_empresa
```

## 19. ds_usuarios_cards

Média diária de usuários ativos na semana e no mês da data de referência e nos períodos anteriores.

```sql
WITH base AS (
  SELECT
    company_id,
    event_date,
    usuarios_ativos,
    MAX(event_date) OVER () AS ref
  FROM workspace.curated.kpi_tracking_diario_empresa
)
SELECT
  company_id,
  ROUND(AVG(CASE WHEN trunc(event_date, 'WEEK') = trunc(ref, 'WEEK') THEN usuarios_ativos END), 1) AS semana_atual,
  ROUND(AVG(CASE WHEN trunc(event_date, 'WEEK') = date_sub(trunc(ref, 'WEEK'), 7) THEN usuarios_ativos END), 1) AS semana_anterior,
  ROUND(AVG(CASE WHEN trunc(event_date, 'MONTH') = trunc(ref, 'MONTH') THEN usuarios_ativos END), 1) AS mes_atual,
  ROUND(AVG(CASE WHEN trunc(event_date, 'MONTH') = add_months(trunc(ref, 'MONTH'), -1) THEN usuarios_ativos END), 1) AS mes_anterior
FROM base
GROUP BY company_id
```

## 20. ds_tracking_sessao

Uma linha por sessão do app, com o dia em que ela começou.

```sql
SELECT
  company_id,
  CAST(inicio AS DATE) AS dia,
  duracao_s,
  eventos,
  telas_distintas
FROM workspace.curated.kpi_tracking_sessao
```

## 21. ds_tracking_tela

Visualizações e tempo de permanência por tela. Não tem data.

```sql
SELECT *
FROM workspace.curated.kpi_tracking_tela
```

## 22. ds_tracking_acoes

Toques no app por tela e alvo. Não tem data.

```sql
SELECT *
FROM workspace.curated.kpi_tracking_acoes
```

## 23. ds_tracking_api

Requisições, erros e latência da API por dia e endpoint.

```sql
SELECT *
FROM workspace.curated.kpi_tracking_api_diaria
```
