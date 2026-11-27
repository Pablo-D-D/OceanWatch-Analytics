# 04 — Gold ML Features

**OceanWatch Analytics** · `04_Gold_ML_Features`

## Descripción y objetivo

Prepara observaciones elegibles para Isolation Forest.

## Entradas

`workspace.oceanwatch.ais_silver`.

## Tecnologías

PySpark pipelines y Structured Streaming.

## Flujo de procesamiento

1. Leer Silver incrementalmente.
2. Eliminar nulos en SOG, COG, Heading, VesselType, Length, Width, LAT y LON.
3. Validar SOG 0–40, COG [0,360), Heading 0–359, VesselType 1–99, dimensiones positivas finitas y rangos globales de coordenadas.
4. Seleccionar ocho variables, MMSI, BaseDateTime y tres campos de trazabilidad.

## Salidas

`workspace.oceanwatch.ais_gold_ml_features`, streaming table de 13 campos particionada por fecha.

## Validaciones y resultados

Filtros de completitud y rango. Los registros excluidos de ML pueden seguir siendo válidos en Silver.

## Dependencias y consideraciones

Alimenta entrenamiento externo a los archivos entregados e inferencia de notebooks 13/14. No calcula scores ni entrena el modelo.
