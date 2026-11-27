# Notebook 5 — Selección de puertos de Estados Unidos

**OceanWatch Analytics** · `Notebook5_OrganizacionPuntos_puertos_US`

## Descripción y objetivo

Consolida puertos de interés mediante selección por atributos y cobertura espacial.

## Entradas

`/Volumes/workspace/oceanwatch/geo_data/Raw/WPI_Shapefile.zip` y capa integrada del Notebook 4.

## Tecnologías

GeoPandas, Pyogrio, pandas, Folium y zipfile.

## Flujo de procesamiento

1. Explorar World Port Index y sus atributos.
2. Seleccionar COUNTRY = US.
3. Homogeneizar CRS y seleccionar puntos que intersectan la cobertura integrada.
4. Unir índices de ambas selecciones sin repetir registros de origen.
5. Exportar Shapefile ZIP y verificar geometrías.

## Salidas

`/Volumes/workspace/oceanwatch/geo_data/Processed/Puertos_US_Seleccion_Final.zip`.

## Validaciones y resultados

3.630 puntos originales; 666 por atributos, 641 por intersección, 612 coincidentes y 695 finales. Se comprueban cantidad y equivalencia geométrica tras exportación.

## Dependencias y consideraciones

Depende del Notebook 4. La unión conserva puntos seleccionados por cualquiera de los criterios; no implica que todos estén dentro del polígono. La versión entregada exporta Shapefile, no GeoParquet.
