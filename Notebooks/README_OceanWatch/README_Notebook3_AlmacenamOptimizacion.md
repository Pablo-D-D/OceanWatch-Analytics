# Notebook 3 — Almacenamiento y optimización

**OceanWatch Analytics** · `Notebook3_AlmacenamOptimizacion`

## Descripción y objetivo

Compara formatos de almacenamiento y selecciona Delta Lake particionado por fecha para el histórico.

## Entradas

CSV en `/Volumes/workspace/oceanwatch/ais_data/csv/`.

## Tecnologías

PySpark, Delta Lake, Parquet, dbutils, time.

## Flujo de procesamiento

1. Leer CSV con esquema explícito y establecer consulta de referencia temporal y espacial.
2. Escribir Parquet y Delta sin particionar cuando no existan las rutas.
3. Inspeccionar tamaño, archivos, planes y tiempos de consulta.
4. Crear Delta con partición diaria `fecha`.
5. Comparar resultados y verificar partition pruning.

## Salidas

Bajo `/Volumes/workspace/oceanwatch/ais_data/almacenamiento/`: `parquet`, `delta` y `delta_particionado_fecha`.

## Validaciones y resultados

Se verifica equivalencia de conteos de la consulta y el plan con PartitionFilters; el notebook reporta siete archivos de datos en la versión particionada frente a 52 en Delta sin particionar.

## Dependencias y consideraciones

Depende del Notebook 1. Los tiempos dependen del entorno y caché. Este histórico es independiente de las tablas declarativas del pipeline.
