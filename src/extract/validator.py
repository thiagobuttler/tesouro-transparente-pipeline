import os
from session.spark_session import SparkSessionManager

spark = SparkSessionManager.get_spark_session()

class EnvironmentValidator:

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

        @staticmethod
        def validade_schema_exists(spark: spark, catalog_name: str, schema_name: str) -> None:

                databases = [db.name for db in spark.catalog.listDatabases()]
                
                target_schema = f"{catalog_name}.{schema_name}"

                schema_exists = any(db == schema_name or db == target_schema for db in databases)
                if not schema_exists:

                    try:
                        spark.sql(f"DESCRIBE SCHEMA `{catalog_name}`.`{schema_name}`")
                    except Exception as e:
                        raise RuntimeError(
                            f"[Pre-Flight Check] O Schema '{catalog_name}.{schema_name}' não existe no Unity Catalog: {e}"
                            )

            