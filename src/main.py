import os
import sys

# Garante o apontamento exato para a pasta do projeto.
current_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in locals() else os.getcwd()
if current_dir not in sys.path:
    sys.path.append(current_dir)

# from session.spark_session      import SparkSessionManager
from extract.validator          import EnvironmentValidator
from config.settings            import carregar_config
from io_utils.data_handler      import DataHandler
from processing.transformations import Transformation

# A sessão spark é injetada automaticamente pelo ambiente Databricks Serverless.
# spark = SparkSessionManager.get_spark_session()

# Carrega configurações externalizadas.
config = carregar_config()

# Cria uma instância da classe DataHandler
dh = DataHandler(spark)

# Cria uma instância da classe Transformation.
transformer = Transformation()

# Obtendo os caminhos.
volume_path_bronze = config['paths']['volume_path_bronze']
schema_silver      = config['paths']['silver']
schema_gold        = config['paths']['gold']

print(f"""
      Obtido os seguintes paths:
      bronze =  {volume_path_bronze}
      silver =  {schema_silver}
      gold   =  {schema_gold}
      """)

# Valida a existência e configuração do ambiente, tanto a leitura dos arquivos quanto os schemas para gravação.
print('Iniciando validação do ambiente...')
EnvironmentValidator.validate_volume_path(volume_path_bronze)

# Leitura dos arquivos na camada bronze.
print(f'Lendo dados Parquet de: {volume_path_bronze}')

# Carregando os dados da camada bronze.
tesouro_transparente_bronze = dh.load_tesouro_transparente(path = volume_path_bronze)

print(f'Total de registros na camada Bronze: {tesouro_transparente_bronze.count()}')

# Transformação e criação do dataframe tesouro_transparente_silver.
tesouro_transparente_silver = transformer.transform_silver(df = tesouro_transparente_bronze)

# Salvando o dataframe na camada silver, em formato delta.
print("Salvando a tabela tesouro_transparente_silver...")
dh.write_delta(df = tesouro_transparente_silver, path = schema_silver, table_name = 'tesouro_transparente_silver')

# Criando as tabelas analíticas

# Dim Titulo
dim_titulo = transformer.create_dim_titulo(tesouro_transparente_silver)

# Salvando o dataframe dim_titulo
print("Salvando a tabela dim_titulo...")
dh.write_delta(df = dim_titulo, path = schema_gold, table_name = 'dim_titulo')

# fct_precos_taxas
fct_precos_taxas = transformer.create_fct_precos_taxas(tesouro_transparente_silver)

# Salvando a tabela fct_precos_taxas
print("Salvando a tabela fct_precos_taxas...")
dh.write_delta(df = fct_precos_taxas, path = schema_gold, table_name = 'fct_precos_taxas')
print(f"fct_precos_taxas: linhas {fct_precos_taxas.count()}")

# base_zc_df
base_zc_df = transformer.create_base_zc_df(df = fct_precos_taxas, dim_titulo = dim_titulo)

# Salvando o dataframe create_base_zc_df
print("Salvando a tabela stg_zero_cupom...")
dh.write_delta(df = base_zc_df, path = schema_silver, table_name = 'stg_zero_cupom')

# Lendo o dataframe stg_zero_cupom
base_zc = dh.load_silver_or_gold(path = schema_silver, table_name = 'stg_zero_cupom')
print(f'Base zero cupom: {base_zc.count()} linhas')

# dim_data
dim_data = transformer.create_dim_data(spark = spark, df = fct_precos_taxas)

# Salvando o dataframe dim_data
print("Salvando a tabela dim_data...")
dh.write_delta(df = dim_data, path = schema_gold, table_name = 'dim_data')

# fct_curva_juros_vertices
fct_curva_juros_vertices = transformer.create_fct_juros_vertices(spark = spark, df = base_zc)

# Salvando o dataframe fct_curva_juros_vertices
print("Salvando a tabela fct_curva_juros_vertices")
dh.write_delta(df = fct_curva_juros_vertices, path = schema_gold, table_name = 'fct_curva_juros_vertices')

# fct_inflacao_implicita
fct_inflacao_implicita = transformer.create_fct_inflacao_implicita(df = fct_curva_juros_vertices)

# Salvando o dataframe fct_inflacao_implicita
print("Salvando tabela fct_inflacao_implicita")
dh.write_delta(df = fct_inflacao_implicita, path = schema_gold, table_name = 'fct_inflacao_implicita')

# fct_risco_titulo
fct_risco_titulo = transformer.create_fct_risco_titulo(df = base_zc)

# Salvando o dataframe fct_risco_titulo
print("Salvando a tabela fct_risco_titulo")
dh.write_delta(df = fct_risco_titulo, path = schema_gold, table_name = 'fct_risco_titulo')
 
# fct_sinais_mercado.
fct_sinais_mercado = transformer.create_fct_sinais_mercado(df = fct_curva_juros_vertices)

# Salvando o dataframe fct_sinais_mercado.
print("Salvando a tabela fct_sinais_mercado...")
dh.write_delta(df = fct_sinais_mercado, path = schema_gold, table_name = 'fct_sinais_mercado')

# fct_inclinacao_curva.
fct_inclinacao_curva = transformer.create_fct_inclinacao_curva(df = fct_curva_juros_vertices)

# Salvando o dataframe fct_inclinacao_curva.
print("Salvando a tabela fct_inclinacao_curva")
dh.write_delta(df = fct_inclinacao_curva, path = schema_gold, table_name = 'fct_inclinacao_curva') 

# spark.stop()  # removido: a sessão é gerenciada pelo runtime do Databricks.