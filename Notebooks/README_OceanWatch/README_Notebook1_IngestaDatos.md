# Notebook 1 — Ingesta de datos

**OceanWatch Analytics** · `Notebook1_IngestaDatos`

## Descripción y objetivo

Prepara el conjunto histórico AIS de NOAA para la exploración inicial.

## Entradas

Archivos `AIS_2023_06_01.zip` a `AIS_2023_06_07.zip` de `https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2023`.

## Tecnologías

Python, requests, zipfile, Spark SQL, dbutils y Unity Catalog.

## Flujo de procesamiento

1. Crear esquema `workspace.oceanwatch` y volumen `ais_data`.
2. Crear carpetas zip y csv.
3. Descargar por bloques, omitir archivos existentes y reintentar descargas fallidas.
4. Verificar integridad CRC y descomprimir ZIP válidos.
5. Inventariar archivos resultantes.

## Salidas

`/Volumes/workspace/oceanwatch/ais_data/zip/` y `/Volumes/workspace/oceanwatch/ais_data/csv/`.

## Validaciones y resultados

El notebook reporta siete archivos descargados y validados; la interrupción del 3 de junio se recuperó mediante reintento.

## Dependencias y consideraciones

Alimenta los notebooks 2 y 3. No publica directamente en Landing; requiere permisos de creación y escritura.
