# Databricks notebook source
# MAGIC %md
# MAGIC ## Cálculo dos KPIs: cleaned para curated
# MAGIC Fonte da pipeline `Apollo_BI` (camada Gold). Lê as 20 tabelas da camada `cleaned`, calcula os KPIs de saúde, produção, manutenção, ativos, alertas, realocação e estoque, calcula os IS Energético e Ambiental por string, por filial e por dia, e define cada tabela da camada `curated` com `@dp.materialized_view()`. A `curated` é a base final do dashboard.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Imports e camadas

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = "workspace"
CLEANED = f"{CATALOG}.cleaned"
CURATED = f"{CATALOG}.curated"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Parâmetros dos KPIs
# MAGIC Mudar um parâmetro exige executar a pipeline de novo.

# COMMAND ----------

LIMIAR_SAUDE = 70
PADRAO_MIN = 0.80
QUEDA_BRUSCA_PTS = 10
META_IS = 80
META_RESOLUCAO_DIAS = 7

# COMMAND ----------

# MAGIC %md
# MAGIC #### Leitura
# MAGIC Cada função de cálculo lê a `cleaned` ou outra tabela da `curated` pelo nome completo, e a pipeline deduz a ordem de execução por essas leituras.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Ler uma tabela da cleaned

# COMMAND ----------

def read_cleaned(table):
    return spark.read.table(f"{CLEANED}.{table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Ler uma tabela da curated

# COMMAND ----------

def read_curated(table):
    return spark.read.table(f"{CURATED}.{table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Contar linhas que atendem a uma condição

# COMMAND ----------

def count_if(condition):
    return F.sum(F.when(condition, 1).otherwise(0))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Tabelas de apoio

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Filial

# COMMAND ----------

def unit_info():
    return read_cleaned("dim_company_unit").select(
        "company_unit_key", "company_key", "unit_name", "city", "state", "is_active", "kwp_total")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### String e filial

# COMMAND ----------

def string_unit():
    strings = read_cleaned("dim_string").select("string_key", "inverter_key", "panel_model_key")
    inverters = read_cleaned("dim_inverter").select("inverter_key", "company_unit_key")
    return strings.join(inverters, "inverter_key").select("string_key", "panel_model_key", "company_unit_key")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Calendário

# COMMAND ----------

def calendario():
    return read_cleaned("dim_date").select(
        "date_key", "full_date", F.date_format("full_date", "yyyy-MM").alias("ano_mes"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Saúde das strings

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Saúde atual de cada string

# COMMAND ----------

def saude_atual_string():
    w = Window.partitionBy("string_key").orderBy(F.col("calculated_at").desc())
    ultima = (read_cleaned("fact_string_health")
              .withColumn("rn", F.row_number().over(w))
              .filter(F.col("rn") == 1)
              .select("string_key", "calculated_at", "health_score"))
    nota = F.col("health_score")
    faixa = (F.when(nota < 50, "CRITICA")
              .when(nota < 60, "ALTA")
              .when(nota < LIMIAR_SAUDE, "MEDIA")
              .otherwise("OK"))
    return (ultima.withColumn("faixa", faixa)
                  .join(string_unit().select("string_key", "company_unit_key"), "string_key"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Saúde por filial

# COMMAND ----------

def saude_por_filial(saude_string):
    df = saude_string.groupBy("company_unit_key").agg(
        F.count("*").alias("strings_com_nota"),
        F.round(F.avg("health_score"), 2).alias("saude_media"),
        count_if(F.col("health_score") < LIMIAR_SAUDE).alias("strings_abaixo_limiar"),
        count_if(F.col("faixa") == "CRITICA").alias("strings_criticas"),
        count_if(F.col("faixa") == "ALTA").alias("strings_altas"),
        count_if(F.col("faixa") == "MEDIA").alias("strings_medias"),
    )
    return (df.withColumn("pct_abaixo_limiar", F.round(100 * F.col("strings_abaixo_limiar") / F.col("strings_com_nota"), 2))
              .withColumn("desvio_limiar", F.round(F.col("saude_media") - LIMIAR_SAUDE, 2)))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Evolução diária da saúde por filial

# COMMAND ----------

def saude_diaria_filial():
    saude = read_cleaned("fact_string_health").join(string_unit().select("string_key", "company_unit_key"), "string_key")
    return (saude.join(calendario().select("date_key", "full_date"), "date_key")
                 .groupBy("company_unit_key", "full_date")
                 .agg(F.round(F.avg("health_score"), 2).alias("saude_media")))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Produção

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Energia gerada por filial e mês

# COMMAND ----------

def producao_filial_mes():
    perf = (read_cleaned("fact_string_daily_performance")
            .join(string_unit().select("string_key", "company_unit_key"), "string_key")
            .join(calendario().select("date_key", "ano_mes"), "date_key"))
    mensal = perf.groupBy("company_unit_key", "ano_mes").agg(
        F.round(F.sum("generated_energy_kwh"), 2).alias("energia_kwh"),
        F.sum("fault_count").alias("leituras_com_falha"),
    )
    kwp = unit_info().select("company_unit_key", "kwp_total")
    return (mensal.join(kwp, "company_unit_key", "left")
                  .withColumn("kwh_por_kwp", F.when(F.col("kwp_total") > 0, F.round(F.col("energia_kwh") / F.col("kwp_total"), 2))))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Manutenção

# COMMAND ----------

# MAGIC %md
# MAGIC ##### OS por filial e mês

# COMMAND ----------

def manutencao_filial_mes():
    cadeia = read_cleaned("vw_maintenance_chain").select(
        F.col("id").alias("maintenance_key"), (F.col("chain_level") > 1).alias("retrabalho"))
    m = (read_cleaned("fact_maintenance")
         .join(calendario().select(F.col("date_key").alias("opening_date_key"), "ano_mes"), "opening_date_key")
         .join(cadeia, "maintenance_key", "left"))

    status = F.col("maintenance_status")
    com_orcamento = (status == "CONCLUIDA") & (F.col("estimated_cost") > 0)

    df = m.groupBy("company_unit_key", "ano_mes").agg(
        F.count("*").alias("total_os"),
        count_if(status == "ABERTA").alias("os_abertas"),
        count_if(status == "EM_ANDAMENTO").alias("os_em_andamento"),
        count_if(status == "CONCLUIDA").alias("os_concluidas"),
        count_if(status == "CANCELADA").alias("os_canceladas"),
        count_if(F.col("calculated_status") == "ATRASADA").alias("os_atrasadas"),
        count_if(F.coalesce(F.col("retrabalho"), F.lit(False))).alias("os_retrabalho"),
        F.round(F.avg("resolution_days"), 2).alias("tempo_medio_resolucao_dias"),
        F.round(F.sum("total_cost"), 2).alias("custo_total"),
        F.sum(F.when(com_orcamento, F.col("estimated_cost"))).alias("orcado_concluidas"),
        F.sum(F.when(com_orcamento, F.col("total_cost"))).alias("realizado_concluidas"),
    )
    return (df.withColumn("taxa_cancelamento", F.round(100 * F.col("os_canceladas") / F.col("total_os"), 2))
              .withColumn("taxa_retrabalho", F.round(100 * F.col("os_retrabalho") / F.col("total_os"), 2))
              .withColumn("desvio_orcamento_pct", F.when(F.col("orcado_concluidas") > 0,
                          F.round(100 * (F.col("realizado_concluidas") - F.col("orcado_concluidas")) / F.col("orcado_concluidas"), 2)))
              .withColumn("desvio_meta_resolucao", F.round(F.col("tempo_medio_resolucao_dias") - META_RESOLUCAO_DIAS, 2)))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### OS por tipo de manutenção e tipo de alerta

# COMMAND ----------

def os_por_tipo():
    tipos = read_cleaned("dim_maintenance_type").select(
        "maintenance_type_key", F.col("type_name").alias("tipo_manutencao"))
    m = (read_cleaned("fact_maintenance")
         .join(tipos, "maintenance_type_key")
         .join(calendario().select(F.col("date_key").alias("opening_date_key"), "ano_mes"), "opening_date_key"))
    return (m.groupBy("company_unit_key", "ano_mes", "tipo_manutencao", "warning_type", "maintenance_status")
             .agg(F.count("*").alias("qtd_os")))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Tempo de resolução por OS
# MAGIC Uma linha por OS concluída, com os dias de resolução. Alimenta o boxplot e o histograma do tempo de resolução.

# COMMAND ----------

def os_resolucao():
    tipos = read_cleaned("dim_maintenance_type").select(
        "maintenance_type_key", F.col("type_name").alias("tipo_manutencao"))
    return (read_cleaned("fact_maintenance")
            .join(tipos, "maintenance_type_key")
            .join(calendario().select(F.col("date_key").alias("opening_date_key"), "ano_mes"), "opening_date_key")
            .filter(F.col("maintenance_status") == "CONCLUIDA")
            .filter(F.col("resolution_days").isNotNull())
            .select("maintenance_key", "company_unit_key", "ano_mes", "tipo_manutencao",
                    "warning_type", "resolution_days", "total_cost")
            .withColumn("dentro_da_meta", F.col("resolution_days") <= META_RESOLUCAO_DIAS))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Ativos

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Placas por status e filial

# COMMAND ----------

def ativos_filial():
    panels = read_cleaned("dim_panel")
    unit_via_string = string_unit().select("string_key", F.col("company_unit_key").alias("unit_via_string"))
    placas = panels.join(unit_via_string, "string_key", "left")
    own_unit = F.col("company_unit_key") if "company_unit_key" in panels.columns else F.lit(None)
    placas = (placas.withColumn("company_unit_key", F.coalesce(own_unit, F.col("unit_via_string")))
                    .filter(F.col("company_unit_key").isNotNull()))
    status = F.col("operating_stats")
    df = placas.groupBy("company_unit_key").agg(
        F.count("*").alias("total_placas"),
        count_if(status == "EM_ESTOQUE").alias("placas_em_estoque"),
        count_if(status == "OPERACIONAL").alias("placas_operacionais"),
        count_if(status == "MANUTENCAO").alias("placas_em_manutencao"),
        count_if(status == "INATIVO").alias("placas_inativas"),
        count_if(status == "FALHA").alias("placas_em_falha"),
    )
    df = df.join(unit_info().select("company_unit_key", "kwp_total"), "company_unit_key", "left")
    return (df.withColumn("pct_falha_ou_manutencao",
                          F.round(100 * (F.col("placas_em_falha") + F.col("placas_em_manutencao")) / F.col("total_placas"), 2))
              .withColumn("taxa_ativacao",
                          F.round(100 * (F.col("total_placas") - F.col("placas_em_estoque")) / F.col("total_placas"), 2)))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Alertas

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Alertas por filial

# COMMAND ----------

def alertas_filial():
    w = read_cleaned("fact_warning")
    sem_os = F.col("maintenance_key").isNull() | (F.col("maintenance_status") == "CANCELADA")
    return w.groupBy("company_unit_key").agg(
        F.count("*").alias("total_alertas"),
        count_if(F.col("status") == "ATIVO").alias("alertas_ativos"),
        count_if(F.col("status") == "RECONHECIDO").alias("alertas_reconhecidos"),
        count_if(F.col("status") == "RESOLVIDO").alias("alertas_resolvidos"),
        count_if(F.col("severity") == "CRITICA").alias("alertas_criticos"),
        count_if(sem_os & (F.col("status") != "RESOLVIDO")).alias("alertas_sem_os"),
        F.round(F.avg("hours_to_maintenance"), 2).alias("horas_medias_ate_os"),
        F.round(F.avg("resolution_days"), 2).alias("dias_medios_resolucao_alerta"),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Realocação e estoque

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Realocações por filial de origem

# COMMAND ----------

def realocacao_filial():
    r = read_cleaned("fact_relocation")
    return r.groupBy("origin_unit_key", "relocation_scope").agg(
        F.count("*").alias("total_solicitacoes"),
        count_if(F.col("status") == "PENDENTE").alias("pendentes"),
        count_if(F.col("status") == "APROVADA").alias("aprovadas"),
        count_if(F.col("status") == "REJEITADA").alias("rejeitadas"),
        count_if(F.col("status") == "CONCLUIDA").alias("concluidas"),
        F.sum("panel_quantity").alias("placas_envolvidas"),
        F.round(F.avg("review_days"), 2).alias("dias_medios_revisao"),
    ).withColumnRenamed("origin_unit_key", "company_unit_key")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Estoque por peça

# COMMAND ----------

def estoque_peca():
    pecas = read_cleaned("dim_part").select("part_key", "sku", "part_name")
    return read_cleaned("fact_stock").join(pecas, "part_key")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Estoque por filial

# COMMAND ----------

def estoque_filial(estoque):
    return estoque.groupBy("company_unit_key").agg(
        F.count("*").alias("pecas_cadastradas"),
        count_if(F.col("below_minimum")).alias("pecas_abaixo_minimo"),
        F.round(F.sum("stock_value"), 2).alias("valor_estoque"),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Índices de sustentabilidade (IS)
# MAGIC O IS Financeiro fica de fora: ele depende de consumo e tarifa do usuário, que não existem no banco. O IS Geral é a média dos IS que existem (Energético e Ambiental).

# COMMAND ----------

# MAGIC %md
# MAGIC ##### IS Energético por string

# COMMAND ----------

def is_energetico_string():
    no_padrao = (read_cleaned("fact_string_daily_performance")
                 .groupBy("string_key")
                 .agg(F.avg((F.col("peak_to_nominal_ratio") >= PADRAO_MIN).cast("int")).alias("pct_no_padrao")))
    sem_queda = (read_cleaned("fact_string_health")
                 .filter(F.col("health_delta").isNotNull())
                 .groupBy("string_key")
                 .agg(F.avg((F.col("health_delta") > -QUEDA_BRUSCA_PTS).cast("int")).alias("pct_sem_queda")))
    df = no_padrao.join(sem_queda, "string_key", "outer")
    media = (F.coalesce(F.col("pct_no_padrao"), F.col("pct_sem_queda")) + F.coalesce(F.col("pct_sem_queda"), F.col("pct_no_padrao"))) / 2
    return df.withColumn("is_energetico", F.round(100 * media, 2))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### IS Ambiental por string

# COMMAND ----------

def is_ambiental_string():
    placas = read_cleaned("dim_panel").filter(F.col("installation_dt").isNotNull() & F.col("deactivated_at").isNull())
    idade = placas.groupBy("string_key").agg(
        F.avg(F.datediff(F.current_date(), F.col("installation_dt")) / 365.25).alias("idade_media_anos"))
    garantia = string_unit().select("string_key", "panel_model_key").join(
        read_cleaned("dim_panel_model").select("panel_model_key", "warranty_years"), "panel_model_key")
    df = idade.join(garantia, "string_key")
    restante = (1 - F.col("idade_media_anos") / F.col("warranty_years")) * 100
    is_amb = F.when(F.col("warranty_years") > 0, F.greatest(F.lit(0.0), F.least(F.lit(100.0), restante)))
    return df.select("string_key", "idade_media_anos", "warranty_years", F.round(is_amb, 2).alias("is_ambiental"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### IS por string

# COMMAND ----------

def is_por_string():
    base = string_unit().select("string_key", "company_unit_key")
    energetico = is_energetico_string().select("string_key", "is_energetico")
    ambiental = is_ambiental_string().select("string_key", "is_ambiental")
    return base.join(energetico, "string_key", "left").join(ambiental, "string_key", "left")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### IS por filial e IS Geral

# COMMAND ----------

def is_por_filial(is_string):
    df = is_string.groupBy("company_unit_key").agg(
        F.round(F.avg("is_energetico"), 2).alias("is_energetico"),
        F.round(F.avg("is_ambiental"), 2).alias("is_ambiental"),
    )
    geral = (F.coalesce(F.col("is_energetico"), F.col("is_ambiental")) + F.coalesce(F.col("is_ambiental"), F.col("is_energetico"))) / 2
    df = (df.withColumn("is_geral", F.round(geral, 2))
            .withColumn("desvio_meta", F.round(F.col("is_geral") - META_IS, 2)))
    return df.join(unit_info().select("company_unit_key", "company_key", "unit_name"), "company_unit_key")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Índices de sustentabilidade por dia
# MAGIC Mesmas regras do IS acumulado, aplicadas a cada dia. O IS semanal e o mensal são a média dos IS diários do período, calculada no dashboard.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### IS Energético por string e dia

# COMMAND ----------

def is_energetico_string_dia():
    dias = calendario().select("date_key", "full_date")

    desempenho = (read_cleaned("fact_string_daily_performance")
                  .join(dias, "date_key")
                  .withColumn("no_padrao", (F.col("peak_to_nominal_ratio") >= PADRAO_MIN).cast("int"))
                  .groupBy("string_key", "full_date")
                  .agg(F.avg("no_padrao").alias("pct_no_padrao")))

    saude = (read_cleaned("fact_string_health")
             .filter(F.col("health_delta").isNotNull())
             .join(dias, "date_key")
             .withColumn("sem_queda", (F.col("health_delta") > -QUEDA_BRUSCA_PTS).cast("int"))
             .groupBy("string_key", "full_date")
             .agg(F.avg("sem_queda").alias("pct_sem_queda")))

    df = desempenho.join(saude, ["string_key", "full_date"], "outer")
    media = (F.coalesce(F.col("pct_no_padrao"), F.col("pct_sem_queda")) + F.coalesce(F.col("pct_sem_queda"), F.col("pct_no_padrao"))) / 2
    return df.withColumn("is_energetico", F.round(100 * media, 2)).select("string_key", "full_date", "is_energetico")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### IS Ambiental por string e dia
# MAGIC Usa as placas instaladas e ainda ativas em cada dia. Calcula só para os dias que têm IS Energético.

# COMMAND ----------

def is_ambiental_string_dia(dias_da_string):
    placas = (read_cleaned("dim_panel")
              .filter(F.col("installation_dt").isNotNull())
              .select("string_key", "installation_dt", "deactivated_at"))

    ja_instalada = F.col("installation_dt") <= F.col("full_date")
    ainda_ativa = F.col("deactivated_at").isNull() | (F.col("deactivated_at") > F.col("full_date"))
    placas_ativas = dias_da_string.join(placas, "string_key").filter(ja_instalada & ainda_ativa)

    idade = placas_ativas.groupBy("string_key", "full_date").agg(
        F.avg(F.datediff(F.col("full_date"), F.col("installation_dt")) / 365.25).alias("idade_media_anos"))

    garantia = string_unit().select("string_key", "panel_model_key").join(
        read_cleaned("dim_panel_model").select("panel_model_key", "warranty_years"), "panel_model_key")

    df = idade.join(garantia, "string_key")
    restante = (1 - F.col("idade_media_anos") / F.col("warranty_years")) * 100
    is_ambiental = F.when(F.col("warranty_years") > 0, F.greatest(F.lit(0.0), F.least(F.lit(100.0), restante)))
    return df.select("string_key", "full_date", F.round(is_ambiental, 2).alias("is_ambiental"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### IS por filial e dia

# COMMAND ----------

def is_por_filial_dia():
    energetico = is_energetico_string_dia()
    ambiental = is_ambiental_string_dia(energetico.select("string_key", "full_date"))

    por_string = energetico.join(ambiental, ["string_key", "full_date"], "left")
    por_string = por_string.join(string_unit().select("string_key", "company_unit_key"), "string_key")

    df = por_string.groupBy("company_unit_key", "full_date").agg(
        F.round(F.avg("is_energetico"), 2).alias("is_energetico"),
        F.round(F.avg("is_ambiental"), 2).alias("is_ambiental"),
    )
    geral = (F.coalesce(F.col("is_energetico"), F.col("is_ambiental")) + F.coalesce(F.col("is_ambiental"), F.col("is_energetico"))) / 2
    df = (df.withColumn("is_geral", F.round(geral, 2))
            .withColumn("desvio_meta", F.round(F.col("is_geral") - META_IS, 2)))
    return df.join(unit_info().select("company_unit_key", "company_key", "unit_name"), "company_unit_key")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Mapa de filiais

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Filial com cidade, estado e indicadores de risco

# COMMAND ----------

def filial_mapa(saude_filial, alertas):
    return (unit_info()
            .join(saude_filial.select("company_unit_key", "saude_media", "strings_abaixo_limiar"), "company_unit_key", "left")
            .join(alertas.select("company_unit_key", "alertas_ativos"), "company_unit_key", "left")
            .fillna(0, subset=["strings_abaixo_limiar", "alertas_ativos"]))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Definir as tabelas da curated
# MAGIC Uma `@dp.materialized_view()` para cada tabela. As que dependem de outra KPI leem a tabela já calculada da `curated`.

# COMMAND ----------

@dp.materialized_view(
    name=f"{CURATED}.kpi_saude_string",
    comment="Saúde atual e faixa de cada string"
)
def kpi_saude_string():
    return saude_atual_string()

# COMMAND ----------

@dp.materialized_view(
    name=f"{CURATED}.kpi_saude_filial",
    comment="Saúde média, strings abaixo do limiar e desvio por filial"
)
def kpi_saude_filial():
    return saude_por_filial(read_curated("kpi_saude_string"))

# COMMAND ----------

@dp.materialized_view(
    name=f"{CURATED}.kpi_saude_diaria_filial",
    comment="Evolução diária da saúde média por filial"
)
def kpi_saude_diaria_filial():
    return saude_diaria_filial()

# COMMAND ----------

@dp.materialized_view(
    name=f"{CURATED}.kpi_producao_filial_mes",
    comment="Energia gerada, kWh por kWp e leituras com falha por filial e mês"
)
def kpi_producao_filial_mes():
    return producao_filial_mes()

# COMMAND ----------

@dp.materialized_view(
    name=f"{CURATED}.kpi_manutencao_filial_mes",
    comment="Status das OS, atrasos, tempo de resolução, custo, desvio de orçamento, cancelamento e retrabalho por filial e mês"
)
def kpi_manutencao_filial_mes():
    return manutencao_filial_mes()

# COMMAND ----------

@dp.materialized_view(
    name=f"{CURATED}.kpi_os_por_tipo",
    comment="Quantidade de OS por filial, mês, tipo de manutenção, tipo de alerta e status"
)
def kpi_os_por_tipo():
    return os_por_tipo()

# COMMAND ----------

@dp.materialized_view(
    name=f"{CURATED}.kpi_os_resolucao",
    comment="Uma linha por OS concluída, com dias de resolução, custo e se ficou dentro da meta"
)
def kpi_os_resolucao():
    return os_resolucao()

# COMMAND ----------

@dp.materialized_view(
    name=f"{CURATED}.kpi_ativos_filial",
    comment="Placas por status, taxa de ativação e percentual em falha ou manutenção por filial"
)
def kpi_ativos_filial():
    return ativos_filial()

# COMMAND ----------

@dp.materialized_view(
    name=f"{CURATED}.kpi_alertas_filial",
    comment="Alertas por status e severidade, alertas sem OS e horas até a OS por filial"
)
def kpi_alertas_filial():
    return alertas_filial()

# COMMAND ----------

@dp.materialized_view(
    name=f"{CURATED}.kpi_realocacao_filial",
    comment="Solicitações de realocação por filial de origem e escopo"
)
def kpi_realocacao_filial():
    return realocacao_filial()

# COMMAND ----------

@dp.materialized_view(
    name=f"{CURATED}.kpi_estoque_peca",
    comment="Estoque de cada peça por filial"
)
def kpi_estoque_peca():
    return estoque_peca()

# COMMAND ----------

@dp.materialized_view(
    name=f"{CURATED}.kpi_estoque_filial",
    comment="Peças cadastradas, peças abaixo do mínimo e valor do estoque por filial"
)
def kpi_estoque_filial():
    return estoque_filial(read_curated("kpi_estoque_peca"))

# COMMAND ----------

@dp.materialized_view(
    name=f"{CURATED}.kpi_is_string",
    comment="IS Energético e IS Ambiental de cada string"
)
def kpi_is_string():
    return is_por_string()

# COMMAND ----------

@dp.materialized_view(
    name=f"{CURATED}.kpi_is_filial",
    comment="IS Energético, IS Ambiental, IS Geral e desvio da meta por filial"
)
def kpi_is_filial():
    return is_por_filial(read_curated("kpi_is_string"))

# COMMAND ----------

@dp.materialized_view(
    name=f"{CURATED}.kpi_is_filial_diario",
    comment="IS Energético, IS Ambiental, IS Geral e desvio da meta por filial e dia"
)
def kpi_is_filial_diario():
    return is_por_filial_dia()

# COMMAND ----------

@dp.materialized_view(
    name=f"{CURATED}.dim_filial_mapa",
    comment="Filial com cidade, estado e indicadores de risco para o mapa do dashboard"
)
def dim_filial_mapa():
    return filial_mapa(read_curated("kpi_saude_filial"), read_curated("kpi_alertas_filial"))
