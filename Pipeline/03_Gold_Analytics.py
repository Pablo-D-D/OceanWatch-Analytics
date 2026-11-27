
# ============================================================
# OCEANWATCH ANALYTICS
# CAPA GOLD - ANALYTICS
# ============================================================
#
# Fuente:
# workspace.oceanwatch.ais_silver
#
# Vistas materializadas:
# 1. ais_gold_indicadores
# 2. ais_gold_tipos_embarcacion
# 3. ais_gold_categorias
# 4. ais_gold_velocidades
#
# Pipeline:
# OceanWatch_ETL_Pipeline
#
# ============================================================

from pyspark import pipelines as dp
from pyspark.sql import functions as F


# ============================================================
# 1. CONFIGURACIÓN
# ============================================================

TABLA_SILVER = "workspace.oceanwatch.ais_silver"


# ============================================================
# 2. FUNCIONES AUXILIARES
# ============================================================

def leer_silver():
    """
    Lee los registros validados y normalizados
    de la capa Silver.

    No ejecuta acciones Spark.
    """
    return spark.read.table(TABLA_SILVER)


# ============================================================
# 3. GOLD 01 - INDICADORES GENERALES
# ============================================================

@dp.materialized_view(
    name="ais_gold_indicadores",
    comment=(
        "Indicadores generales acumulados "
        "de actividad maritima AIS"
    )
)
def ais_gold_indicadores():

    df = leer_silver()

    return df.agg(

        F.count("*").alias(
            "total_posiciones"
        ),

        F.countDistinct("MMSI").alias(
            "buques_unicos"
        ),

        F.round(
            F.avg("SOG"), 2
        ).alias(
            "velocidad_promedio"
        ),

        F.max("SOG").alias(
            "velocidad_maxima"
        ),

        F.min("SOG").alias(
            "velocidad_minima"
        ),

        F.countDistinct(
            "VesselType"
        ).alias(
            "total_tipos"
        ),

        F.countDistinct(
            "VesselType_Categoria"
        ).alias(
            "total_categorias"
        ),

        F.sum(
            F.when(
                F.col("VesselType") == 0,
                F.lit(1)
            ).otherwise(F.lit(0))
        ).alias(
            "posiciones_indefinidas"
        ),

        F.min("BaseDateTime").alias(
            "fecha_inicial"
        ),

        F.max("BaseDateTime").alias(
            "fecha_final"
        )
    )


# ============================================================
# 4. GOLD 02 - TIPOS DE EMBARCACIÓN
# ============================================================

@dp.materialized_view(
    name="ais_gold_tipos_embarcacion",
    comment=(
        "Distribucion e indicadores "
        "por tipo de embarcacion AIS"
    )
)
def ais_gold_tipos_embarcacion():

    df = leer_silver()

    return (
        df.groupBy(
            "VesselType",
            "VesselType_Descripcion",
            "VesselType_Categoria"
        )
        .agg(

            F.count("*").alias(
                "total_posiciones"
            ),

            F.countDistinct("MMSI").alias(
                "buques_unicos"
            ),

            F.round(
                F.avg("SOG"), 2
            ).alias(
                "velocidad_promedio"
            ),

            F.max("SOG").alias(
                "velocidad_maxima"
            )
        )
    )


# ============================================================
# 5. GOLD 03 - CATEGORÍAS DE EMBARCACIÓN
# ============================================================

@dp.materialized_view(
    name="ais_gold_categorias",
    comment=(
        "Distribucion e indicadores "
        "por categoria de embarcacion AIS"
    )
)
def ais_gold_categorias():

    df = leer_silver()

    return (
        df.groupBy(
            "VesselType_Categoria"
        )
        .agg(

            F.count("*").alias(
                "total_posiciones"
            ),

            F.countDistinct("MMSI").alias(
                "buques_unicos"
            ),

            F.countDistinct("VesselType").alias(
                "tipos_embarcacion"
            ),

            F.round(
                F.avg("SOG"), 2
            ).alias(
                "velocidad_promedio"
            )
        )
    )


# ============================================================
# 6. GOLD 04 - DISTRIBUCIÓN DE VELOCIDADES
# ============================================================
'''
@dp.materialized_view(
    name="ais_gold_velocidades",
    comment=(
        "Distribucion de posiciones AIS "
        "por intervalos de velocidad SOG"
    )
)
def ais_gold_velocidades():

    df = leer_silver()

    # --------------------------------------------------------
    # Clasificación de velocidades en nudos
    # --------------------------------------------------------

    df = df.withColumn(
        "intervalo_velocidad",

        F.when(
            F.col("SOG") == 0,
            "Sin velocidad registrada"
        )

        .when(
            (F.col("SOG") > 0) &
            (F.col("SOG") <= 5),
            "Baja"
        )

        .when(
            (F.col("SOG") > 5) &
            (F.col("SOG") <= 10),
            "Moderada"
        )

        .when(
            (F.col("SOG") > 10) &
            (F.col("SOG") <= 20),
            "Alta"
        )

        .when(
            (F.col("SOG") > 20) &
            (F.col("SOG") <= 40),
            "Muy alta"
        )

        .otherwise("Sin clasificar")
    )

    # --------------------------------------------------------
    # Agregaciones por intervalo
    # --------------------------------------------------------

    return (
        df.groupBy(
            "intervalo_velocidad"
        )
        .agg(

            F.count("*").alias(
                "total_posiciones"
            ),

            F.countDistinct("MMSI").alias(
                "buques_unicos"
            ),

            F.round(
                F.avg("SOG"), 2
            ).alias(
                "velocidad_promedio"
            )
        )
    )
'''
