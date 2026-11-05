# src/pipeline/pipeline.py

import logging
import time
from io_utils.data_handler import DataHandler
from processing.transformations import Transformation
from pyspark.sql import SparkSession, DataFrame
from io_utils.validator import EnvironmentValidator
from quality.quality import DataQuality

logger = logging.getLogger(__name__)


class Pipeline:
    """
    Encapsula a lógica de execução do pipeline de dados.
    """

    def __init__(
        self,
        spark: SparkSession,
        data_handler: DataHandler,
        transformer: Transformation,
        quality: DataQuality,
    ):
        self.spark = spark
        self.data_handler = data_handler
        self.transformer = transformer
        self.quality = quality

    def run(self, config):
        """
        Executa o pipeline completo: carga, transformação, e salvamento.
        """
        logger.info("Pipeline iniciado...")
        t0 = time.time()

        # Obtém os caminhos das camadas bronze, silver e gold a partir das configurações carregadas.
        volume_path_bronze = config["paths"]["volume_path_bronze"]
        schema_silver = config["paths"]["silver"]
        schema_gold = config["paths"]["gold"]

        validator = EnvironmentValidator(self.spark)
        validator.validate_schema_exists(schema_silver)
        validator.validate_schema_exists(schema_gold)

        logger.info(
            f"""
            Paths obtidos e schemas validados:
            bronze =  {volume_path_bronze}
            silver =  {schema_silver}
            gold   =  {schema_gold}
            """
        )

        # Pre-flight: valida o caminho do Volume da camada bronze antes de iniciar o pipeline.
        logger.info(f"Iniciando validação de ambiente da camada bronze no caminho {volume_path_bronze}")
        validator.validate_volume_path(volume_path_bronze)
        logger.info("Camada bronze validada com sucesso!")

        # Orquestra o pipeline por camadas: bronze -> silver -> gold.
        bronze = self.transform_bronze(volume_path_bronze=volume_path_bronze)
        silver = self.transform_silver(bronze=bronze, schema_silver=schema_silver)
        self.transform_gold(silver=silver, schema_silver=schema_silver, schema_gold=schema_gold)

        logger.info(f"Pipeline concluído com sucesso! Duração: {round(time.time() - t0, 2)}s.")

    def transform_bronze(self, volume_path_bronze: str) -> DataFrame:
        """Carrega os dados brutos (Parquet) da camada bronze no Volume do Unity Catalog e retorna o DataFrame da camada."""

        # Leitura dos arquivos Parquet na camada bronze (dados brutos ingressados no Volume do Unity Catalog).
        logger.info(f"Lendo dados Parquet de: {volume_path_bronze}")
        bronze = self.data_handler.load_tesouro_transparente(
            path=volume_path_bronze
        )
        logger.info(f"Dados da camada bronze lidos com sucesso do caminho {volume_path_bronze}")

        return bronze

    def transform_silver(self, bronze: DataFrame, schema_silver: str) -> DataFrame:
        """Padroniza o schema dos dados da bronze, executa o check de Data Quality (apenas informativo) e persiste a tabela da camada silver."""
        silver = self.transformer.transform_silver(df=bronze)

        # Executa validações de qualidade de dados (Data Quality) sobre a camada silver — apenas informativo, sem interromper o fluxo.
        dq = self.quality.validate_data_quality(silver)
        logger.info(f"Data Quality: {dq}")

        # Persiste o DataFrame da camada silver em formato Delta Lake, criando a tabela gerenciada tesouro_transparente_silver.
        logger.info("Salvando a tabela tesouro_transparente_silver...")
        self.data_handler.write_delta(
            df=silver,
            path=schema_silver,
            table_name="tesouro_transparente_silver",
        )
        logger.info(f"Tabela tesouro_transparente_silver salva com sucesso!")
        return silver

    def transform_gold(self, silver: DataFrame, schema_silver: str, schema_gold: str) -> None:
        """Constrói e persiste as tabelas dimensionais e de fatos da camada gold a partir da camada silver."""

        # ---------------------------------------------------------------------------
        # Construção das tabelas analíticas da camada Gold (esquema dimensional)
        # ---------------------------------------------------------------------------

        # Cria a dimensão de títulos (dim_titulo) a partir dos dados tratados da camada silver.
        dim_titulo = self.transformer.create_dim_titulo(silver)

        # Persiste a tabela de dimensão dim_titulo na camada gold em formato Delta Lake.
        logger.info("Salvando a tabela dim_titulo...")
        self.data_handler.write_delta(
            df=dim_titulo, path=schema_gold, table_name="dim_titulo"
        )
        logger.info("Tabela dim_titulo salva com sucesso!")

        # Cria a tabela de fatos de preços e taxas (fct_precos_taxas) a partir dos dados da camada silver.
        fct_precos_taxas = self.transformer.create_fct_precos_taxas(silver)

        # Persiste a tabela de fatos fct_precos_taxas na camada gold em formato Delta Lake.
        logger.info("Salvando a tabela fct_precos_taxas...")
        self.data_handler.write_delta(
            df=fct_precos_taxas, path=schema_gold, table_name="fct_precos_taxas",
        )
        logger.info("Tabela fct_precos_taxas salva com sucesso!")

        # Cria o DataFrame da base de juros zero-cupom (stg_zero_cupom), intermediária entre silver e gold.
        base_zc_df = self.transformer.create_base_zc_df(
            df=fct_precos_taxas, dim_titulo=dim_titulo
        )

        # Persiste a tabela intermediária stg_zero_cupom na camada silver em formato Delta Lake.
        logger.info("Salvando a tabela stg_zero_cupom...")
        self.data_handler.write_delta(
            df=base_zc_df, path=schema_silver, table_name="stg_zero_cupom",
        )
        logger.info("Tabela stg_zero_cupom salva com sucesso!")

        # Lê a tabela stg_zero_cupom previamente persistida para uso nas próximas transformações analíticas.
        base_zc = self.data_handler.load_silver_or_gold(
            path=schema_silver, table_name="stg_zero_cupom"
        )

        # Cria a dimensão de datas (dim_data) utilizando o Spark para geração de calendário a partir das datas da fct_precos_taxas.
        dim_data = self.transformer.create_dim_data(
            spark=self.spark, df=fct_precos_taxas
        )

        # Persiste a tabela de dimensão dim_data na camada gold em formato Delta Lake.
        logger.info("Salvando a tabela dim_data...")
        self.data_handler.write_delta(
            df=dim_data, path=schema_gold, table_name="dim_data"
        )
        logger.info("Tabela dim_data salva com sucesso!")

        # Cria a tabela de fatos de curva de juros por vértices (fct_curva_juros_vertices) a partir da base zero-cupom.
        fct_curva_juros_vertices = self.transformer.create_fct_juros_vertices(
            spark=self.spark, df=base_zc
        )

        # Persiste a tabela de fatos fct_curva_juros_vertices na camada gold em formato Delta Lake.
        logger.info("Salvando a tabela fct_curva_juros_vertices...")
        self.data_handler.write_delta(
            df=fct_curva_juros_vertices,
            path=schema_gold,
            table_name="fct_curva_juros_vertices",
        )
        logger.info("Tabela fct_curva_juros_vertices salva com sucesso!")

        # Cria a tabela de fatos de inflação implícita (fct_inflacao_implicita) derivada dos vértices da curva de juros.
        fct_inflacao_implicita = self.transformer.create_fct_inflacao_implicita(
            df=fct_curva_juros_vertices
        )

        # Persiste a tabela de fatos fct_inflacao_implicita na camada gold em formato Delta Lake.
        logger.info("Salvando tabela fct_inflacao_implicita...")
        self.data_handler.write_delta(
            df=fct_inflacao_implicita,
            path=schema_gold,
            table_name="fct_inflacao_implicita",
        )
        logger.info("Tabela fct_inflacao_implicita salva com sucesso!")

        # Cria a tabela de fatos de risco por título (fct_risco_titulo) a partir da base zero-cupom.
        fct_risco_titulo = self.transformer.create_fct_risco_titulo(df=base_zc)

        # Persiste a tabela de fatos fct_risco_titulo na camada gold em formato Delta Lake.
        logger.info("Salvando a tabela fct_risco_titulo...")
        self.data_handler.write_delta(
            df=fct_risco_titulo, path=schema_gold, table_name="fct_risco_titulo",
        )
        logger.info("Tabela fct_risco_titulo salva com sucesso!")

        # Cria a tabela de fatos de sinais de mercado (fct_sinais_mercado) derivada da curva de juros por vértices.
        fct_sinais_mercado = self.transformer.create_fct_sinais_mercado(
            df=fct_curva_juros_vertices
        )

        # Persiste a tabela de fatos fct_sinais_mercado na camada gold em formato Delta Lake.
        logger.info("Salvando a tabela fct_sinais_mercado...")
        self.data_handler.write_delta(
            df=fct_sinais_mercado, path=schema_gold, table_name="fct_sinais_mercado",
        )
        logger.info("Tabela fct_sinais_mercado salva com sucesso!")

        # Cria a tabela de fatos de inclinação da curva (fct_inclinacao_curva) derivada da curva de juros por vértices.
        fct_inclinacao_curva = self.transformer.create_fct_inclinacao_curva(
            df=fct_curva_juros_vertices
        )

        # Persiste a tabela de fatos fct_inclinacao_curva na camada gold em formato Delta Lake.
        logger.info("Salvando a tabela fct_inclinacao_curva...")
        self.data_handler.write_delta(
            df=fct_inclinacao_curva, path=schema_gold, table_name="fct_inclinacao_curva",
        )
        logger.info("Tabela fct_inclinacao_curva salva com sucesso!")

        logger.info("Camada gold construída e persistida com sucesso!")
