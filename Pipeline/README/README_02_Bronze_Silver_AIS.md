# 02 — Validación y transformación Silver

**OceanWatch Analytics** · `02_Bronze_Silver_AIS`

## Descripción y objetivo

Separa registros aceptados y rechazados, normaliza tipos y enriquece VesselType.

## Entradas

`workspace.oceanwatch.ais_bronze`.

## Tecnologías

PySpark pipelines, funciones Spark SQL nativas.

## Flujo de procesamiento

1. Convertir valores mediante try_cast; enteros aceptan representaciones decimales integrales.
2. Evaluar RGN_S02 MMSI de nueve dígitos, S03 fecha válida, S04 LAT [-90,90], S05 LON [-180,180], S06 SOG [0,40].
3. Separar por cantidad_errores.
4. Normalizar COG, Heading, Status, dimensiones, Cargo y texto; VesselType inválido se convierte en 0.
5. Enriquecer descripción y categoría con diccionario 0–99 (RGN_S07/S08).

## Salidas

`ais_silver` y `ais_silver_cuarentena` en workspace.oceanwatch. Cuarentena conserva columnas Bronze, reglas_incumplidas, motivos_rechazo, cantidad_errores y fecha_validacion.

## Validaciones y resultados

Assert de cobertura completa del diccionario; reglas críticas con tratamiento de nulos. Prueba final reportada: 60.444.266 aceptados acumulados y 259.293 rechazados.

## Dependencias y consideraciones

No elimina duplicados ni aplica filtro geográfico US. Campos no críticos inválidos se normalizan, no necesariamente causan cuarentena. Alimenta scripts 03 y 04.
