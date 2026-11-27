# Notebook 4 — Límites marítimos y terrestres de Estados Unidos

**OceanWatch Analytics** · `Notebook4_Capa_limitesMarítimos_US`

## Descripción y objetivo

Integra geometrías terrestres y marítimas para delimitar el área de selección de puertos.

## Entradas

En `/Volumes/workspace/oceanwatch/geo_data/Raw/`: `LimitesTerrestres_US_WGS84.zip` y `Limites_terrestres_Maritimos_US_Antimeridiano.gpkg` (capa `Limites_terrestres_Maritimos_US_P4_Backup`).

## Tecnologías

GeoPandas, Pyogrio, pandas, Folium, zipfile, QGIS como preparación previa.

## Flujo de procesamiento

1. Cargar capas y trabajar en EPSG:4326.
2. Visualizar y excluir el polígono marítimo con id 11.
3. Concatenar geometrías con atributo de origen.
4. Disolver para obtener cobertura integrada.
5. Exportar Shapefile comprimido y releerlo.

## Salidas

`/Volumes/workspace/oceanwatch/geo_data/Processed/Limites_Terrestres_Maritimos_US_WGS84.zip`.

## Validaciones y resultados

Inspección de CRS, tipos, validez y equivalencia geométrica tras exportación.

## Dependencias y consideraciones

Alimenta Notebook 5. La digitalización marítima se realizó previamente en QGIS; el filtro id 11 es específico de esta fuente. No es una regla espacial activa del ETL AIS.
