# Notebook 2 — Exploración, perfilamiento y preguntas de negocio

**OceanWatch Analytics** · `Notebook2_ExplorPerfil_PregNeg`

## Descripción y objetivo

Caracteriza la telemetría histórica y evalúa su calidad y patrones de tráfico.

## Entradas

CSV del Notebook 1 en `/Volumes/workspace/oceanwatch/ais_data/csv/`.

## Tecnologías

PySpark, funciones SQL, ventanas y visualizaciones Databricks.

## Flujo de procesamiento

1. Verificar encabezados y leer con esquema explícito.
2. Perfilar posiciones diarias, MMSI distintos, tipos y longitud.
3. Evaluar nulos, rangos geográficos, velocidades, identificadores y duplicados.
4. Comparar conteos exactos y aproximados y analizar tráfico por tipo.
5. Estimar distancias mediante Haversine y ventanas por MMSI; filtrar saltos mediante percentil 99 de velocidad implícita.
6. Agrupar posiciones en grilla de 0,1 grados y analizar permanencia y visitantes de un día.

## Salidas

Tablas y visualizaciones exploratorias en el notebook; no constituye una transformación persistente de Silver.

## Validaciones y resultados

Se reportan 60.533.559 filas, 6,04 GB, 1.388 grupos duplicados exactos y 49.897 registros con MMSI de longitud anómala. El conteo aproximado redujo el tiempo de la prueba alrededor de 20,8 %.

## Dependencias y consideraciones

Depende del Notebook 1. Los conteos aproximados no equivalen a exactos; el filtro de velocidad implícita es heurístico. La grilla no tiene área uniforme y no se implementa aquí el cruce final con puertos.
