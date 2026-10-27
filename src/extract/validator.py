# src/extract/validator

import os
from pyspark.sql import SparkSession

class EnvironmentValidator:
    """
    Classe responsável pela validação do ambiente, tanto caminhos de leitura como caminhos de escrita.
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark

    @staticmethod
    def validate_volume_path(volume_path: str) -> None:

        if not os.path.exists(volume_path):
            raise FileNotFoundError(
                f"[Pre-Flight Check] O caminho do Volum '{volume_path}' não foi encontrado."
                "Verifique se o Schema e o Volume foram criados no Unity Catalog."
            )
        
        files = [f for f in os.listdir(volume_path) if not f.startswith('.')]
        if not files:
            raise ValueError(
                f"[Pre-Flight Check] O Volume '{volume_path}' existe, mas está vazio."
                "Certifique-se de que a ingestão da Lambda gravou os dados brutos"    
            )
        
        print(f"[Pre-Flight Check] Volume validado com sucesso. Arquivos detectados: {len(files)}")

    def validate_schema_exists(self, schema_path: str) -> None:
        current_catalog = self.spark.catalog.currentCatalog()

        databases = [
            f"{current_catalog}.{db.name}"
            for db in self.spark.catalog.listDatabases()
        ]

        target_schema = schema_path.replace("`", "")

        schema_exists = any(db == target_schema for db in databases)

        if not schema_exists:
            try:
                self.spark.sql(f"DESCRIBE SCHEMA {schema_path}")
            except Exception as e:
                raise RuntimeError(
                    f"[Pre-Flight Check] Não foi possível acessar "
                    f"o schema '{schema_path}': {e}"
                ) from e
            