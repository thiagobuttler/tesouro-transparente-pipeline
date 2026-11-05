# src/quality/quality.py

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

class DataQuality:

    def validate_data_quality(self, df: DataFrame) -> dict:
        """Executa validações de qualidade de dados sobre a camada silver e retorna um dicionário com as métricas coletadas: total de registros, taxas fora do range esperado (0%-30%), PUs negativos, datas base futuras, nulos em campos críticos e duplicatas por título e data."""
        metricas = df.agg(
            F.count(F.lit(1)).alias("total_registros"),
            F.sum(
                F.when(
                    (F.col("taxa_compra_manha") < 0)
                    | (F.col("taxa_compra_manha") > 30)
                    | (F.col("taxa_venda_manha") < 0)
                    | (F.col("taxa_venda_manha") > 30),
                    F.lit(1),
                ).otherwise(F.lit(0))
            ).alias("taxas_fora_range"),
            F.sum(
                F.when(
                    (F.col("pu_compra_manha") < 0)
                    | (F.col("pu_venda_manha") < 0)
                    | (F.col("pu_base_manha") < 0),
                    F.lit(1),
                ).otherwise(F.lit(0))
            ).alias("pu_negativo"),
            F.sum(
                F.when(F.col("data_base") > F.current_date(), F.lit(1)).otherwise(
                    F.lit(0)
                )
            ).alias("data_base_futura"),
            F.sum(
                F.when(
                    F.col("tipo_titulo").isNull()
                    | F.col("data_base").isNull()
                    | F.col("data_vencimento").isNull(),
                    F.lit(1),
                ).otherwise(F.lit(0))
            ).alias("nulos_campos_criticos"),
        ).collect()[0]

        duplicatas = (
            df.groupBy("tipo_titulo", "data_vencimento", "data_base")
            .agg(F.count(F.lit(1)).alias("cnt"))
            .filter(F.col("cnt") > 1)
            .count()
        )

        return {
            "total_registros": metricas["total_registros"],
            "taxas_fora_range": metricas["taxas_fora_range"],
            "pu_negativo": metricas["pu_negativo"],
            "data_base_futura": metricas["data_base_futura"],
            "nulos_campos_criticos": metricas["nulos_campos_criticos"],
            "duplicatas": duplicatas,
        }