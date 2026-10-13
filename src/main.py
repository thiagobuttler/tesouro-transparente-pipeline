import os
import sys

# Garante o apontamento exato para a pasta do projeto
current_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in locals() else os.getcwd()
if current_dir not in sys.path:
    sys.path.append(current_dir)

from session.spark_session import SparkSessionManager
from extract.validator import EnvironmentValidator
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType
from pyspark.sql import Window
from pyspark.sql import functions as F

# Captura a sessão spark
spark = SparkSessionManager.get_spark_session()

# Parâmetros estruturados
catalog_name = 'tesouro_transparente_pipeline'
volume_path_bronze = f'/Volumes/{catalog_name}/bronze/raw_data/'
schema_silver = f'`{catalog_name}`.`silver`'
schema_gold = f'`{catalog_name}`.`gold`'


# Valida a existência e configuração do ambiente, tanto a leitura dos arquivos quanto os schemas para gravação
print('Iniciando validação do ambiente...')
EnvironmentValidator.validate_volume_path(volume_path_bronze)

# Definição do schema
schema_tesouro_transparente = StructType([
    StructField('Tipo Titulo', StringType(), True),
    StructField('Data Vencimento', StringType(), True),
    StructField('Data Base', StringType(), True),
    StructField('Taxa Compra Manha', DoubleType(), True),
    StructField('Taxa Venda Manha', DoubleType(), True),
    StructField('PU Compra Manha', DoubleType(), True),
    StructField('PU Venda Manha', DoubleType(), True),
    StructField('PU Base Manha', DoubleType(), True)
])

# Leitura dos arquivos na camada bronze
print(f'Lendo dados Parquet de: {volume_path_bronze}')
tesouro_transparente_bronze = (
    spark.read.format("parquet")
    .schema(schema_tesouro_transparente)
    .load(volume_path_bronze)
)

print(f'Total de registros na camada Bronze: {tesouro_transparente_bronze.count()}')

# Mapeamento limpo
de_para_colunas = {
    'Tipo Titulo': 'tipo_titulo',
    'Data Vencimento': 'data_vencimento',
    'Data Base': 'data_base',
    'Taxa Compra Manha': 'taxa_compra_manha',
    'Taxa Venda Manha': 'taxa_venda_manha',
    'PU Compra Manha': 'pu_compra_manha',
    'PU Venda Manha': 'pu_venda_manha',
    'PU Base Manha': 'pu_base_manha'
}

# Renomeando as colunas
tesouro_transparente_silver = (
    tesouro_transparente_bronze
    .withColumnsRenamed(de_para_colunas)
)

tesouro_transparente_silver = (
    tesouro_transparente_silver
    .withColumn('data_vencimento', F.to_date('data_vencimento', 'dd/MM/yyyy'))
    .withColumn('data_base', F.to_date('data_base', 'dd/MM/yyyy'))
)

# Salvando o dataframe na camada silver, em delta tables
tesouro_transparente_silver.write \
    .format('delta') \
    .mode('overwrite') \
    .saveAsTable(schema_silver + '.`tesouro_transparente_silver`')

# Criando as tabelas analíticas
# Dim Titulo
dim_titulo = (
    tesouro_transparente_silver
    .select('tipo_titulo', 'data_vencimento')
    .distinct()
    .orderBy(['tipo_titulo', 'data_vencimento'], ascending=[True, False])
)

# Enriquecimento analítico (Surrogate, Indexador e Ano)
dim_titulo = (
    dim_titulo

    .withColumn(
        "id_titulo",
        F.md5(F.concat_ws('||', F.col('tipo_titulo'), F.col('data_vencimento')))
    )

    .withColumn('ano_vencimento', F.year('data_vencimento'))

    .withColumn(
        "indexador",
        F.when(F.col("tipo_titulo").contains("Selic"), "Pós-fixado (Selic)")
         .when(F.col("tipo_titulo").contains("IPCA"), "Híbrido (IPCA+)")
         .when(F.col("tipo_titulo").contains("Prefixado"), "Prefixado")
         .when(F.col("tipo_titulo").contains("IGP-M"), "Híbrido (IGP-M)")
         .otherwise("Outro")
    )

    .select('id_titulo', 'tipo_titulo', 'data_vencimento', 'ano_vencimento', 'indexador')
)

dim_titulo.write \
    .format('delta') \
    .mode('overwrite') \
    .saveAsTable(schema_gold + '.`dim_titulo`')

# tabela fato fct_precos_taxas
fct_precos_taxas = (
    tesouro_transparente_silver
    
    .withColumn(
        "id_titulo",
        F.md5(F.concat_ws("||", F.col("tipo_titulo"), F.col("data_vencimento")))
    )

    .withColumn(
        "dias_ate_vencimento",
        F.datediff(F.col("data_vencimento"), F.col("data_base"))
    )

    .withColumn(
        "spread_taxa_manha",
        F.round(F.col("taxa_venda_manha") - F.col("taxa_compra_manha"), 4)
    )
    .select(
        "id_titulo",
        "data_base",
        "taxa_compra_manha",
        "taxa_venda_manha",
        "spread_taxa_manha",
        "pu_compra_manha",
        "pu_venda_manha",
        "pu_base_manha",
        "dias_ate_vencimento"
    )
)

(
    fct_precos_taxas.write
    .format('delta')
    .mode('overwrite')
    .option('overwriteSchema', 'true')
    .saveAsTable(schema_gold + '.`fct_precos_taxas`')
)

# Função para salvar na camada gold

# I/O

def salvar_gold(df, nome):
    (
        df.write
        .format('delta')
        .mode('overwrite')
        .option('overwriteSchema', 'true')
        .saveAsTable(f'{schema_gold}.`{nome}`')
    )
    total = spark.table(f'{schema_gold}.`{nome}`').count()
    print(f'gold.{nome} gravada: {total} linhas')

# Base de trabalho (zero cupom)
fct = spark.table(f'{schema_gold}.`fct_precos_taxas`')
dim_t = spark.table(f'{schema_gold}.`dim_titulo`')

base_zc_df = (
    fct
    .join(dim_t.select('id_titulo', 'tipo_titulo', 'data_vencimento'), 'id_titulo')
    .filter(F.col('tipo_titulo').isin('Tesouro Prefixado', 'Tesouro IPCA+'))
    .filter(F.col('dias_ate_vencimento') > 0)
    .withColumn(
        'familia',
        F.when(F.col('tipo_titulo') == 'Tesouro Prefixado', F.lit('PRE')).otherwise(F.lit('IPCA'))
    )
    .withColumn('taxa_mid', (F.col('taxa_compra_manha') + F.col('taxa_venda_manha')) / 2)
    .filter(F.col('taxa_mid').isNotNull() & F.col('pu_base_manha').isNotNull())
    .dropDuplicates(['id_titulo', 'data_base'])
)

# substitui o cache(): materializa em Delta e lê de volta
(
    base_zc_df.write
    .format('delta')
    .mode('overwrite')
    .option('overwriteSchema', 'true')
    .saveAsTable(f"{schema_silver}.`stg_zero_cupom`")
)

base_zc = spark.table(f"{schema_silver}.`stg_zero_cupom`")
print(f'Base zero cupom: {base_zc.count()} linhas')

# dim_data
lim = fct.agg(F.min('data_base').alias('mn'), F.max('data_base').alias('mx')).first()

dias_com_cotacao = (
    fct.select('data_base').distinct()
    .withColumn('flag_dia_util_mercado', F.lit(True))
)

dim_data = (
    spark.range(1)
    .select(
        F.explode(
            F.sequence(F.lit(lim['mn']), F.lit(lim['mx']), F.expr('interval 1 day'))
        ).alias('data_base')
    )
    .join(dias_com_cotacao, 'data_base', 'left')
    .withColumn('flag_dia_util_mercado', F.coalesce(F.col('flag_dia_util_mercado'), F.lit(False)))
    .withColumn('ano', F.year('data_base'))
    .withColumn('trimestre', F.quarter('data_base'))
    .withColumn('mes', F.month('data_base'))
    .withColumn('ano_mes', F.date_format('data_base', 'yyyy-MM'))
    .withColumn('dia_semana', F.dayofweek('data_base'))
    .withColumn('flag_fim_de_semana', F.col('dia_semana').isin(1, 7)) 
)

salvar_gold(dim_data, 'dim_data')

# fct_curva_juros_vertices
VERTICES_ANOS = [1, 2, 3, 5, 10]
MAX_GAP_DIAS = 1095

vertices = spark.createDataFrame(
    [(a, a * 365) for a in VERTICES_ANOS], ['vertice_anos', 'vertice_dias']
)

pts = (
    base_zc
    .select(
        'data_base', 'familia',
        F.col('dias_ate_vencimento').alias('dias'),
        F.col('taxa_mid').alias('taxa')
    )
    .crossJoin(vertices)
)

chaves = ['data_base', 'familia', 'vertice_anos', 'vertice_dias']

# Titulo imediatamente antes do vértice (dias <= vértice)
w_lo = Window.partitionBy(*chaves).orderBy(F.col('dias').desc())
lo = (
    pts.filter(F.col('dias') <= F.col('vertice_dias'))
    .withColumn('rn', F.row_number().over(w_lo))
    .filter(F.col('rn') == 1)
    .select(*chaves, F.col('dias').alias('dias_lo'), F.col('taxa').alias('taxa_lo'))
)

w_hi = Window.partitionBy(*chaves).orderBy(F.col('dias').asc())
hi = (
    pts.filter(F.col('dias') >= F.col('vertice_dias'))
    .withColumn('rn', F.row_number().over(w_hi))
    .filter(F.col('rn') == 1)
    .select(*chaves, F.col('dias').alias('dias_hi'), F.col('taxa').alias('taxa_hi'))
)

fct_curva_juros_vertices = (
    lo.join(hi, chaves)
    .filter((F.col('dias_hi') - F.col('dias_lo')) <= MAX_GAP_DIAS)
    .withColumn(
        'taxa_vertice',
        F.round(
            F.when(F.col('dias_hi') == F.col('dias_lo'), F.col('taxa_lo'))
            .otherwise(
                F.col('taxa_lo')
                + (F.col('taxa_hi') - F.col('taxa_lo'))
                * (F.col('vertice_dias') - F.col('dias_lo'))
                / (F.col('dias_hi') - F.col('dias_lo'))
            ),
            4
        )
    )
    .select('data_base', 'familia', 'vertice_anos', 'taxa_vertice', 'dias_lo', 'dias_hi')
)

salvar_gold(fct_curva_juros_vertices, 'fct_curva_juros_vertices')
 
curva = spark.table(f"{schema_gold}.`fct_curva_juros_vertices`")

# fct_inflacao_implicita - breakeven
pre = (
    curva.filter(F.col('familia') == 'PRE')
    .select('data_base', 'vertice_anos', F.col('taxa_vertice').alias('taxa_pre'))
)
real = (
    curva.filter(F.col('familia') == 'IPCA')
    .select('data_base', 'vertice_anos', F.col('taxa_vertice').alias('taxa_real_ipca'))
)

fct_inflacao_implicita = (
    pre.join(real, ['data_base', 'vertice_anos'])
    .withColumn(
        'inflacao_implicita',
        F.round(((1 + F.col('taxa_pre') / 100) / (1 + F.col('taxa_real_ipca') / 100) - 1) * 100, 4)
    )
)

salvar_gold(fct_inflacao_implicita, 'fct_inflacao_implicita')

# fct_risco_titulo
w_t = Window.partitionBy('id_titulo').orderBy('data_base')
w_21 = w_t.rowsBetween(-20, 0)
w_63 = w_t.rowsBetween(-62, 0)
w_acum = w_t.rowsBetween(Window.unboundedPreceding, 0)

anos = F.col('dias_ate_vencimento') / 365.0
y = F.col('taxa_mid') / 100.0

fct_risco_titulo = (
    base_zc
    .withColumn('duration_macaulay', F.round(anos, 4))
    .withColumn('duration_modificada', F.round(anos / (1 + y), 4))
    .withColumn('convexidade', F.round(anos * (anos + 1) / F.pow(1 + y, 2), 4))
    .withColumn('dv01', F.round(F.col('pu_base_manha') * F.col('duration_modificada') * 0.0001, 6))
    # aproximação de 2ª ordem: dP/P = -D_mod*dy + 0.5*C*dy^2
    .withColumn(
        'impacto_pu_alta_100bps_pct',
        F.round((-F.col('duration_modificada') * 0.01 + 0.5 * F.col('convexidade') * 0.0001) * 100, 4)
    )
    .withColumn(
        'impacto_pu_queda_100bps_pct',
        F.round((F.col('duration_modificada') * 0.01 + 0.5 * F.col('convexidade') * 0.0001) * 100, 4)
    )
    .withColumn('retorno_pu_diario', F.col('pu_base_manha') / F.lag('pu_base_manha').over(w_t) - 1)
    .withColumn('var_taxa_bps', F.round((F.col('taxa_mid') - F.lag('taxa_mid').over(w_t)) * 100, 2))
    .withColumn(
        'vol_21d_anualizada_pct',
        F.when(
            F.count('retorno_pu_diario').over(w_21) >= 21,
            F.round(F.stddev('retorno_pu_diario').over(w_21) * F.sqrt(F.lit(252.0)) * 100, 4)
        )
    )
    .withColumn(
        'vol_63d_anualizada_pct',
        F.when(
            F.count('retorno_pu_diario').over(w_63) >= 63,
            F.round(F.stddev('retorno_pu_diario').over(w_63) * F.sqrt(F.lit(252.0)) * 100, 4)
        )
    )
    .withColumn('pu_maximo_historico', F.max('pu_base_manha').over(w_acum))
    .withColumn(
        'drawdown_pct',
        F.round((F.col('pu_base_manha') / F.col('pu_maximo_historico') - 1) * 100, 4)
    )
    .withColumn('max_drawdown_pct', F.min('drawdown_pct').over(w_acum))
    .select(
        'id_titulo', 'data_base', 'familia', 'taxa_mid', 'pu_base_manha', 'dias_ate_vencimento',
        'duration_macaulay', 'duration_modificada', 'convexidade', 'dv01',
        'impacto_pu_alta_100bps_pct', 'impacto_pu_queda_100bps_pct',
        'retorno_pu_diario', 'var_taxa_bps',
        'vol_21d_anualizada_pct', 'vol_63d_anualizada_pct',
        'drawdown_pct', 'max_drawdown_pct'
    )
)
 
salvar_gold(fct_risco_titulo, 'fct_risco_titulo')
 
 
# ------------------------------------------------------------------------------
# 5) fct_sinais_mercado - percentil e z-score da taxa de cada vértice vs. histórico
#    Grão: data_base x familia x vertice_anos
#    Só calcula quando a janela inteira está preenchida (252 / 756 / 1260 pregões)
# ------------------------------------------------------------------------------
JANELAS = {'1a': 252, '3a': 756, '5a': 1260}
 
w_serie = Window.partitionBy('familia', 'vertice_anos').orderBy('data_base')
 
sinais = (
    curva
    .select('data_base', 'familia', 'vertice_anos', 'taxa_vertice')
    .withColumn('var_21d_bps', F.round((F.col('taxa_vertice') - F.lag('taxa_vertice', 21).over(w_serie)) * 100, 2))
)
 
for nome_janela, n in JANELAS.items():
    w = w_serie.rowsBetween(-(n - 1), 0)
    sinais = (
        sinais
        .withColumn('_hist', F.collect_list('taxa_vertice').over(w))
        .withColumn(
            f'zscore_{nome_janela}',
            F.when(
                F.size('_hist') >= n,
                F.round(
                    (F.col('taxa_vertice') - F.avg('taxa_vertice').over(w))
                    / F.stddev('taxa_vertice').over(w),
                    4
                )
            )
        )
        .withColumn(
            f'percentil_{nome_janela}',
            F.when(
                F.size('_hist') >= n,
                F.round(
                    F.size(F.filter('_hist', lambda x: x <= F.col('taxa_vertice')))
                    / F.size('_hist') * 100,
                    2
                )
            )
        )
        .drop('_hist')
    )
 
fct_sinais_mercado = sinais.withColumn(
    'leitura_historica_5a',
    F.when(F.col('percentil_5a') >= 90, F.lit('Taxa no topo da distribuição (>= P90)'))
     .when(F.col('percentil_5a') <= 10, F.lit('Taxa no piso da distribuição (<= P10)'))
     .when(F.col('percentil_5a').isNotNull(), F.lit('Zona intermediária'))
)
 
salvar_gold(fct_sinais_mercado, 'fct_sinais_mercado')
 
 
# ------------------------------------------------------------------------------
# 6) fct_inclinacao_curva - formato da curva (2s5s e 2s10s, em bps) e inversão
#    Grão: data_base x familia
# ------------------------------------------------------------------------------
curva_larga = (
    curva
    .groupBy('data_base', 'familia')
    .pivot('vertice_anos', VERTICES_ANOS)
    .agg(F.first('taxa_vertice'))
)
for a in VERTICES_ANOS:
    curva_larga = curva_larga.withColumnRenamed(str(a), f'taxa_{a}a')
 
fct_inclinacao_curva = (
    curva_larga
    .withColumn('inclinacao_2s5s_bps', F.round((F.col('taxa_5a') - F.col('taxa_2a')) * 100, 2))
    .withColumn('inclinacao_2s10s_bps', F.round((F.col('taxa_10a') - F.col('taxa_2a')) * 100, 2))
    .withColumn('flag_curva_invertida_2s5s', F.col('inclinacao_2s5s_bps') < 0)
)
 
salvar_gold(fct_inclinacao_curva, 'fct_inclinacao_curva')
 

