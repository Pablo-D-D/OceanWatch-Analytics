# 03 — Gold Analytics

**OceanWatch Analytics** · `03_Gold_Analytics`

## Descripción y objetivo

Genera agregaciones de negocio sobre Silver para consumo analítico.

## Entradas

`workspace.oceanwatch.ais_silver`.

## Tecnologías

PySpark pipelines, vistas materializadas y agregaciones Spark.

## Flujo de procesamiento

1. Leer Silver como tabla.
2. Agregar indicadores generales: posiciones, MMSI distintos, velocidades, tipos, categorías, indefinidos y fechas extremas.
3. Agrupar por VesselType, descripción y categoría para indicadores por tipo.
4. Agrupar por categoría para posiciones, buques, tipos y velocidad promedio.

## Salidas

`ais_gold_indicadores`, `ais_gold_tipos_embarcacion`, `ais_gold_categorias` en workspace.oceanwatch.

## Validaciones y resultados

Conteos y estadísticas agregadas; el dashboard publicado refleja las actualizaciones del pipeline según pruebas reportadas.

## Dependencias y consideraciones

El bloque ais_gold_velocidades está comentado mediante cadena multilínea y no crea una vista activa en este código. Los conteos de MMSI por grupo no son aditivos entre grupos.
