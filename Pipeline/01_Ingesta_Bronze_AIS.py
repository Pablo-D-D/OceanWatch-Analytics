
# ============================================================
# OCEANWATCH ANALYTICS
# 01_INGESTA_BRONZE_AIS.py
# ============================================================

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)


# 1. CONFIGURACIÓN DE LANDING

RUTA_LANDING = (
    "/Volumes/workspace/oceanwatch/"
    "ais_landing/incoming/"
)


# 2. ESQUEMA ORIGINAL AIS

columnas_ais = [
    "MMSI",
    "BaseDateTime",
    "LAT",
    "LON",
    "SOG",
    "COG",
    "Heading",
    "VesselName",
    "IMO",
    "CallSign",
    "VesselType",
    "Status",
    "Length",
    "Width",
    "Draft",
    "Cargo",
    "TransceiverClass"
]

# Conservar los valores originales como texto en Bronze
schema_ais = StructType([
    StructField(columna, StringType(), True)
    for columna in columnas_ais
])


# 3. TABLA BRONZE DE STREAMING

@dp.table(
    name="ais_bronze",
    comment=(
        "Datos AIS originales de NOAA ingeridos "
        "mediante Auto Loader"
    ),
    partition_cols=["fecha"]
)
def ais_bronze():

    # Lectura incremental desde Landing
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("delimiter", ",")
        .option("mode", "PERMISSIVE")
        .schema(schema_ais)
        .load(RUTA_LANDING)
    )

    # Incorporación de metadatos de trazabilidad
    return (
        df
        .withColumn(
            "fecha",
            F.to_date(
                F.substring(F.col("BaseDateTime"), 1, 10)
            )
        )
        .withColumn(
            "origen_datos",
            F.lit("NOAA_AIS")
        )
        .withColumn(
            "fecha_ingesta",
            F.current_timestamp()
        )
        .withColumn(
            "archivo_origen",
            F.col("_metadata.file_path")
        )
    )
