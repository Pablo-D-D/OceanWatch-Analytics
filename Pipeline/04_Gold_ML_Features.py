from pyspark import pipelines as dp
from pyspark.sql import functions as F


# ============================================================
# 1. CONFIGURACIÓN
# ============================================================

TABLA_SILVER = "workspace.oceanwatch.ais_silver"

VARIABLES_MODELO = [
    "SOG",
    "COG",
    "Heading",
    "VesselType",
    "Length",
    "Width",
    "LAT",
    "LON"
]

COLUMNAS_IDENTIFICACION = [
    "MMSI",
    "BaseDateTime"
]

COLUMNAS_TRAZABILIDAD = [
    "fecha",
    "fecha_ingesta",
    "archivo_origen"
]


# ============================================================
# 2. VALIDACIÓN DE VARIABLES DEL MODELO
# ============================================================

def filtrar_registros_ml(df):

    # Validación de completitud
    df = df.dropna(subset=VARIABLES_MODELO)

    # Condiciones de calidad del modelo
    df = df.filter(
        F.col("SOG").between(0, 40)
        & (F.col("COG") >= 0)
        & (F.col("COG") < 360)
        & F.col("Heading").between(0, 359)
        & F.col("VesselType").between(1, 99)
        & (F.col("Length") > 0)
        & (F.col("Length") < float("inf"))
        & (F.col("Width") > 0)
        & (F.col("Width") < float("inf"))
        & F.col("LAT").between(-90, 90)
        & F.col("LON").between(-180, 180)
    )

    return df


# ============================================================
# 3. TABLA GOLD ML FEATURES
# ============================================================

@dp.table(
    name="ais_gold_ml_features",
    comment=(
        "Observaciones AIS preparadas para la inferencia "
        "del modelo Isolation Forest de OceanWatch Analytics."
    ),
    partition_cols=["fecha"]
)
def ais_gold_ml_features():

    # Lectura incremental desde Silver
    df_silver = spark.readStream.table(
        TABLA_SILVER
    )

    # Validación de registros para Machine Learning
    df_ml = filtrar_registros_ml(df_silver)

    # Selección de variables y campos de trazabilidad
    return df_ml.select(
        *COLUMNAS_IDENTIFICACION,
        *VARIABLES_MODELO,
        *COLUMNAS_TRAZABILIDAD
    )