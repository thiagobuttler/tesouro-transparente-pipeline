# src/pipeline/pipeline.py

import logging
from io_utils.data_handler import DataHandler
from processing.transformations import Transformation
from pyspark.sql import SparkSession
from extract.validator import EnvironmentValidator

logger = logging.getLogger(__name__)

class Pipeline:
    """
    Encapsula a lógica de execução do pipeline de dados.
    """
    def __init__(self, spark: SparkSession, data_handler: DataHandler, transformer: Transformation):
        self.spark = spark
        self.data_handler = data_handler
        self.transformer = transformer

    def run(self, config):
        """
        Executa o pipeline completo: carga, transformação, e salvamento.
        """
        logger.info("Pipeline iniciado...")

        # Obtém os caminhos das camadas bronze, silver e gold a partir das configurações carregadas.
        volume_path_bronze = config['paths']['volume_path_bronze']
        schema_silver      = config['paths']['silver']
        schema_gold        = config['paths']['gold']

        logger.info(f"""
            Obtido os seguintes paths:
            bronze =  {volume_path_bronze}
            silver =  {schema_silver}
            gold   =  {schema_gold}
            """)

        # Valida a existência do caminho de leitura (camada bronze) no Volume do Unity Catalog antes de iniciar o pipeline.
        logger.info('Iniciando validação do ambiente...')
        EnvironmentValidator.validate_volume_path(volume_path_bronze)

        # Leitura dos arquivos Parquet na camada bronze (dados brutos ingressados no Volume do Unity Catalog).
        logger.info(f'Lendo dados Parquet de: {volume_path_bronze}')

        # Carrega o DataFrame da camada bronze contendo os dados brutos do Tesouro Transparente.
        tesouro_transparente_bronze = self.data_handler.load_tesouro_transparente(path = volume_path_bronze)

        logger.info(f'Total de registros na camada Bronze: {tesouro_transparente_bronze.count()}')

        # Aplica as transformações de limpeza, padronização e enriquecimento para gerar o DataFrame da camada silver.
        tesouro_transparente_silver = self.transformer.transform_silver(df = tesouro_transparente_bronze)

        # Persiste o DataFrame da camada silver em formato Delta Lake, criando a tabela gerenciada tesouro_transparente_silver.
        logger.info("Salvando a tabela tesouro_transparente_silver...")
        self.data_handler.write_delta(df = tesouro_transparente_silver, path = schema_silver, table_name = 'tesouro_transparente_silver')

        # ---------------------------------------------------------------------------
        # Construção das tabelas analíticas da camada Gold (esquema dimensional)
        # ---------------------------------------------------------------------------

        # Cria a dimensão de títulos (dim_titulo) a partir dos dados tratados da camada silver.
        dim_titulo = self.transformer.create_dim_titulo(tesouro_transparente_silver)

        # Persiste a tabela de dimensão dim_titulo na camada gold em formato Delta Lake.
        logger.info("Salvando a tabela dim_titulo...")
        self.data_handler.write_delta(df = dim_titulo, path = schema_gold, table_name = 'dim_titulo')

        # Cria a tabela de fatos de preços e taxas (fct_precos_taxas) a partir dos dados da camada silver.
        fct_precos_taxas = self.transformer.create_fct_precos_taxas(tesouro_transparente_silver)

        # Persiste a tabela de fatos fct_precos_taxas na camada gold em formato Delta Lake.
        logger.info("Salvando a tabela fct_precos_taxas...")
        self.data_handler.write_delta(df = fct_precos_taxas, path = schema_gold, table_name = 'fct_precos_taxas')
        logger.info(f"fct_precos_taxas: linhas {fct_precos_taxas.count()}")

        # Cria o DataFrame da base de juros zero-cupom (stg_zero_cupom), intermediária entre silver e gold.
        base_zc_df = self.transformer.create_base_zc_df(df = fct_precos_taxas, dim_titulo = dim_titulo)

        # Persiste a tabela intermediária stg_zero_cupom na camada silver em formato Delta Lake.
        logger.info("Salvando a tabela stg_zero_cupom...")
        self.data_handler.write_delta(df = base_zc_df, path = schema_silver, table_name = 'stg_zero_cupom')

        # Lê a tabela stg_zero_cupom previamente persistida para uso nas próximas transformações analíticas.
        base_zc = self.data_handler.load_silver_or_gold(path = schema_silver, table_name = 'stg_zero_cupom')
        logger.info(f'Base zero cupom: {base_zc.count()} linhas')

        # Cria a dimensão de datas (dim_data) utilizando o Spark para geração de calendário a partir das datas da fct_precos_taxas.
        dim_data = self.transformer.create_dim_data(spark = self.spark, df = fct_precos_taxas)

        # Persiste a tabela de dimensão dim_data na camada gold em formato Delta Lake.
        logger.info("Salvando a tabela dim_data...")
        self.data_handler.write_delta(df = dim_data, path = schema_gold, table_name = 'dim_data')

        # Cria a tabela de fatos de curva de juros por vértices (fct_curva_juros_vertices) a partir da base zero-cupom.
        fct_curva_juros_vertices = self.transformer.create_fct_juros_vertices(spark = self.spark, df = base_zc)

        # Persiste a tabela de fatos fct_curva_juros_vertices na camada gold em formato Delta Lake.
        logger.info("Salvando a tabela fct_curva_juros_vertices")
        self.data_handler.write_delta(df = fct_curva_juros_vertices, path = schema_gold, table_name = 'fct_curva_juros_vertices')

        # Cria a tabela de fatos de inflação implícita (fct_inflacao_implicita) derivada dos vértices da curva de juros.
        fct_inflacao_implicita = self.transformer.create_fct_inflacao_implicita(df = fct_curva_juros_vertices)

        # Persiste a tabela de fatos fct_inflacao_implicita na camada gold em formato Delta Lake.
        logger.info("Salvando tabela fct_inflacao_implicita")
        self.data_handler.write_delta(df = fct_inflacao_implicita, path = schema_gold, table_name = 'fct_inflacao_implicita')

        # Cria a tabela de fatos de risco por título (fct_risco_titulo) a partir da base zero-cupom.
        fct_risco_titulo = self.transformer.create_fct_risco_titulo(df = base_zc)

        # Persiste a tabela de fatos fct_risco_titulo na camada gold em formato Delta Lake.
        logger.info("Salvando a tabela fct_risco_titulo")
        self.data_handler.write_delta(df = fct_risco_titulo, path = schema_gold, table_name = 'fct_risco_titulo')
        
        # Cria a tabela de fatos de sinais de mercado (fct_sinais_mercado) derivada da curva de juros por vértices.
        fct_sinais_mercado = self.transformer.create_fct_sinais_mercado(df = fct_curva_juros_vertices)

        # Persiste a tabela de fatos fct_sinais_mercado na camada gold em formato Delta Lake.
        logger.info("Salvando a tabela fct_sinais_mercado...")
        self.data_handler.write_delta(df = fct_sinais_mercado, path = schema_gold, table_name = 'fct_sinais_mercado')

        # Cria a tabela de fatos de inclinação da curva (fct_inclinacao_curva) derivada da curva de juros por vértices.
        fct_inclinacao_curva = self.transformer.create_fct_inclinacao_curva(df = fct_curva_juros_vertices)

        # Persiste a tabela de fatos fct_inclinacao_curva na camada gold em formato Delta Lake.
        logger.info("Salvando a tabela fct_inclinacao_curva")
        self.data_handler.write_delta(df = fct_inclinacao_curva, path = schema_gold, table_name = 'fct_inclinacao_curva')

        logger.info("Pipeline concluído com sucesso!")

