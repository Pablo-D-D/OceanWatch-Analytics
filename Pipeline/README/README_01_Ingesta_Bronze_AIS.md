# 01 — Ingesta Bronze AIS

**OceanWatch Analytics** · `01_Ingesta_Bronze_AIS`

## Descripción y objetivo

Ingiere CSV de Landing conservando los valores originales.

## Entradas

`/Volumes/workspace/oceanwatch/ais_landing/incoming/`, CSV con 17 columnas AIS.

## Tecnologías

PySpark pipelines, Auto Loader cloudFiles, Structured Streaming.

## Flujo de procesamiento

1. Definir esquema explícito de 17 campos StringType.
2. Leer CSV incremental con encabezado, coma y modo PERMISSIVE.
3. Agregar fecha derivada de BaseDateTime, origen_datos, fecha_ingesta y archivo_origen.

## Salidas

`workspace.oceanwatch.ais_bronze`, streaming table particionada por fecha.

## Validaciones y resultados

Esquema explícito y metadatos de trazabilidad; no aplica reglas críticas de Silver.

## Dependencias y consideraciones

Se ejecuta en pipeline declarativo. origen_datos se fija a NOAA_AIS incluso para sintéticos; distinguirlos mediante archivo_origen. Alimenta script 02.
