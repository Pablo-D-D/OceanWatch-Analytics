
from pyspark import pipelines as dp
from pyspark.sql import functions as F


# ============================================================
# 1. CONFIGURACIÓN
# ============================================================

TABLA_BRONZE = "workspace.oceanwatch.ais_bronze"

UMBRAL_SOG = 40.0
VESSELTYPE_INDEFINIDO = 0

COLUMNAS_BRONZE = [
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
    "TransceiverClass",
    "fecha",
    "origen_datos",
    "fecha_ingesta",
    "archivo_origen"
]


# ============================================================
# 2. FUNCIONES AUXILIARES
# ============================================================

def convertir_double(campo):
    return F.expr(f"try_cast(`{campo}` AS DOUBLE)")


def convertir_integer(campo):

    valor = F.expr(
        f"try_cast(`{campo}` AS DOUBLE)"
    )

    return F.when(
        valor.isNotNull()
        & (~F.isnan(valor))
        & valor.between(-2147483648, 2147483647)
        & (valor == F.floor(valor)),
        valor.cast("int")
    ).otherwise(F.lit(None).cast("int"))


def convertir_timestamp(campo):
    return F.expr(f"try_cast(`{campo}` AS TIMESTAMP)")


def numero_valido(columna, minimo, maximo):
    return (
        columna.isNotNull()
        & (~F.isnan(columna))
        & columna.between(minimo, maximo)
    )


def limpiar_texto(campo):
    valor = F.trim(F.col(campo))

    return F.when(
        valor.isNull() | (valor == ""),
        F.lit(None).cast("string")
    ).otherwise(valor)


def normalizar_numero(campo, minimo, maximo):
    valor = convertir_double(campo)

    return F.when(
        numero_valido(valor, minimo, maximo),
        valor
    ).otherwise(F.lit(None).cast("double"))


def normalizar_entero(campo, minimo, maximo):
    valor = convertir_integer(campo)

    return F.when(
        valor.between(minimo, maximo),
        valor
    ).otherwise(F.lit(None).cast("int"))


def normalizar_dimension(campo):
    valor = convertir_double(campo)

    return F.when(
        valor.isNotNull()
        & (~F.isnan(valor))
        & (valor > 0),
        valor
    ).otherwise(F.lit(None).cast("double"))


# ============================================================
# 3. REGLAS CRÍTICAS DE CALIDAD
# RGN_S02 - RGN_S06
# ============================================================

def evaluar_reglas_criticas(df):

    mmsi = F.trim(F.col("MMSI"))

    fecha_ais = convertir_timestamp("BaseDateTime")

    lat = convertir_double("LAT")
    lon = convertir_double("LON")
    sog = convertir_double("SOG")

    # Cada regla conserva su código técnico y descripción.
    # Las condiciones de validación originales no se modifican.

    reglas = [
        (
            "RGN_S02",
            "MMSI inválido: debe contener exactamente 9 dígitos numéricos",
            mmsi.rlike(r"^[0-9]{9}$")
        ),
        (
            "RGN_S03",
            "Fecha AIS inválida o ausente",
            fecha_ais.isNotNull()
        ),
        (
            "RGN_S04",
            "Latitud inválida: valor ausente, no numérico o fuera del rango [-90, 90]",
            numero_valido(lat, -90.0, 90.0)
        ),
        (
            "RGN_S05",
            "Longitud inválida: valor ausente, no numérico o fuera del rango [-180, 180]",
            numero_valido(lon, -180.0, 180.0)
        ),
        (
            "RGN_S06",
            "Velocidad SOG inválida: valor ausente, no numérico o fuera del rango [0, 40] nudos",
            numero_valido(sog, 0.0, UMBRAL_SOG)
        )
    ]

    # Identificación de las reglas incumplidas

    codigos = F.array(*[
        F.when(
            ~F.coalesce(condicion, F.lit(False)),
            F.lit(codigo)
        )
        for codigo, descripcion, condicion in reglas
    ])

    # Descripción de los motivos de rechazo

    descripciones = F.array(*[
        F.when(
            ~F.coalesce(condicion, F.lit(False)),
            F.lit(descripcion)
        )
        for codigo, descripcion, condicion in reglas
    ])

    return (
        df
        .withColumn(
            "reglas_incumplidas",
            F.filter(
                codigos,
                lambda x: x.isNotNull()
            )
        )
        .withColumn(
            "motivos_rechazo",
            F.filter(
                descripciones,
                lambda x: x.isNotNull()
            )
        )
        .withColumn(
            "cantidad_errores",
            F.size(F.col("reglas_incumplidas"))
        )
        .withColumn(
            "fecha_validacion",
            F.current_timestamp()
        )
    )


# ============================================================
# 4. DICCIONARIO DE VESSELTYPE
# RGN_S07 Y RGN_S08
# Fuente: MarineActivity AIS Ship Type Codes
# ============================================================

DESCRIPCIONES_ESPECIALES = {
    30: "Fishing",
    31: "Towing",
    32: "Towing, length of the tow exceeds 200 m or breadth exceeds 25 m",
    33: "Dredging or underwater operations",
    34: "Diving operations",
    35: "Military operations",
    36: "Sailing",
    37: "Pleasure craft",
    38: "Reserved for future use",
    39: "Reserved for future use",
    50: "Pilot vessel",
    51: "Search and rescue vessel",
    52: "Tug",
    53: "Port tender",
    54: "Anti-pollution equipment",
    55: "Law enforcement",
    56: "Spare — local vessel",
    57: "Spare — local vessel",
    58: "Medical transport",
    59: "Noncombatant ship (RR Resolution No. 18)"
}

CATEGORIAS_ESPECIALES = {
    30: "Fishing",
    31: "Tug & tow",
    32: "Tug & tow",
    33: "Service",
    34: "Service",
    35: "Military",
    36: "Sailing",
    37: "Pleasure craft",
    38: "Unknown",
    39: "Unknown",
    50: "Service",
    51: "Service",
    52: "Tug & tow",
    53: "Service",
    54: "Service",
    55: "Military",
    56: "Service",
    57: "Service",
    58: "Service",
    59: "Military"
}

FAMILIAS_AIS = {
    20: ("Wing-in-ground (WIG) craft", "High-speed craft"),
    40: ("High-speed craft", "High-speed craft"),
    60: ("Passenger ship", "Passenger"),
    70: ("Cargo ship", "Cargo"),
    80: ("Tanker", "Tanker"),
    90: ("Other type", "Other")
}

DICCIONARIO_VESSELTYPE = {
    0: ("Indefinido", "Unknown")
}

# Códigos reservados 1-19

for codigo in range(1, 20):
    DICCIONARIO_VESSELTYPE[codigo] = (
        "Reserved for future use",
        "Unknown"
    )

# Familias con subcategorías

for inicio, (base, categoria) in FAMILIAS_AIS.items():

    for codigo in range(inicio, inicio + 10):

        posicion = codigo - inicio

        if posicion == 0:
            descripcion = base

        elif 1 <= posicion <= 4:
            letra = "ABCD"[posicion - 1]
            descripcion = (
                f"{base}, hazardous category {letra}"
            )

        elif 5 <= posicion <= 8:
            descripcion = f"{base}, reserved"

        else:
            descripcion = (
                f"{base}, no additional information"
            )

        DICCIONARIO_VESSELTYPE[codigo] = (
            descripcion,
            categoria
        )

# Códigos particulares 30-39 y 50-59

for codigo, descripcion in DESCRIPCIONES_ESPECIALES.items():

    DICCIONARIO_VESSELTYPE[codigo] = (
        descripcion,
        CATEGORIAS_ESPECIALES[codigo]
    )

# Validación de completitud

assert set(DICCIONARIO_VESSELTYPE.keys()) == set(range(100)), (
    "Error: el diccionario debe contener los códigos 0-99"
)


# ============================================================
# 5. EXPRESIONES NATIVAS DE SPARK
# ============================================================

MAPA_DESCRIPCIONES = F.create_map(*[
    elemento
    for codigo, (descripcion, categoria)
    in sorted(DICCIONARIO_VESSELTYPE.items())
    for elemento in (
        F.lit(codigo),
        F.lit(descripcion)
    )
])

MAPA_CATEGORIAS = F.create_map(*[
    elemento
    for codigo, (descripcion, categoria)
    in sorted(DICCIONARIO_VESSELTYPE.items())
    for elemento in (
        F.lit(codigo),
        F.lit(categoria)
    )
])


def enriquecer_vesseltype(df):

    return (
        df
        .withColumn(
            "VesselType_Descripcion",
            F.coalesce(
                F.element_at(
                    MAPA_DESCRIPCIONES,
                    F.col("VesselType")
                ),
                F.lit("Indefinido")
            )
        )
        .withColumn(
            "VesselType_Categoria",
            F.coalesce(
                F.element_at(
                    MAPA_CATEGORIAS,
                    F.col("VesselType")
                ),
                F.lit("Unknown")
            )
        )
    )


# ============================================================
# 6. TRANSFORMACIÓN DE SILVER
# ============================================================

def transformar_ais_silver(df):

    vessel_type = convertir_integer("VesselType")

    vessel_type_normalizado = (
        F.when(
            vessel_type.between(1, 99),
            vessel_type
        )
        .otherwise(
            F.lit(VESSELTYPE_INDEFINIDO)
        )
        .cast("int")
    )

    df_silver = df.select(

        # Identificación
        F.trim(F.col("MMSI")).alias("MMSI"),

        # Tiempo
        convertir_timestamp(
            "BaseDateTime"
        ).alias("BaseDateTime"),

        # Posición
        convertir_double("LAT").alias("LAT"),
        convertir_double("LON").alias("LON"),

        # Velocidad y rumbo
        convertir_double("SOG").alias("SOG"),

        normalizar_numero(
            "COG", 0.0, 359.999999
        ).alias("COG"),

        normalizar_entero(
            "Heading", 0, 359
        ).alias("Heading"),

        # Identificación complementaria
        limpiar_texto(
            "VesselName"
        ).alias("VesselName"),

        limpiar_texto(
            "IMO"
        ).alias("IMO"),

        limpiar_texto(
            "CallSign"
        ).alias("CallSign"),

        # Tipo de embarcación
        vessel_type_normalizado.alias("VesselType"),

        # Estado y características
        normalizar_entero(
            "Status", 0, 15
        ).alias("Status"),

        normalizar_dimension(
            "Length"
        ).alias("Length"),

        normalizar_dimension(
            "Width"
        ).alias("Width"),

        normalizar_dimension(
            "Draft"
        ).alias("Draft"),

        normalizar_entero(
            "Cargo", 1, 99
        ).alias("Cargo"),

        limpiar_texto(
            "TransceiverClass"
        ).alias("TransceiverClass"),

        # Trazabilidad
        F.col("fecha").cast("date").alias("fecha"),

        F.col("origen_datos").alias("origen_datos"),

        F.col("fecha_ingesta").cast(
            "timestamp"
        ).alias("fecha_ingesta"),

        F.col("archivo_origen").alias("archivo_origen")
    )

    # RGN_S08: enriquecimiento descriptivo

    df_silver = enriquecer_vesseltype(df_silver)

    return df_silver


# ============================================================
# 7. STREAMING TABLE SILVER
# ============================================================

@dp.table(
    name="ais_silver",
    comment=(
        "Capa Silver AIS: registros validados, "
        "normalizados y enriquecidos con "
        "descripción y categoría de VesselType."
    )
)
def ais_silver():

    df_bronze = spark.readStream.table(
        TABLA_BRONZE
    )

    df_validado = evaluar_reglas_criticas(
        df_bronze
    )

    df_aceptado = df_validado.filter(
        F.col("cantidad_errores") == 0
    )

    return transformar_ais_silver(
        df_aceptado
    )


# ============================================================
# 8. STREAMING TABLE CUARENTENA
# ============================================================

@dp.table(
    name="ais_silver_cuarentena",
    comment=(
        "Registros AIS rechazados por reglas "
        "críticas de calidad, con códigos técnicos, "
        "descripciones de errores y trazabilidad."
    )
)
def ais_silver_cuarentena():

    df_bronze = spark.readStream.table(
        TABLA_BRONZE
    )

    df_validado = evaluar_reglas_criticas(
        df_bronze
    )

    return (
        df_validado
        .filter(
            F.col("cantidad_errores") > 0
        )
        .select(
            *COLUMNAS_BRONZE,
            "reglas_incumplidas",
            "motivos_rechazo",
            "cantidad_errores",
            "fecha_validacion"
        )
    )
