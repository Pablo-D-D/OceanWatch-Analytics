# Notebook 13 — Desarrollo y validación de inferencia ML

**OceanWatch Analytics** · `Notebook13_ML_Inferencia`

## Descripción y objetivo

Implementa y verifica la inferencia distribuida e incremental del modelo registrado.

## Entradas

`workspace.oceanwatch.ais_gold_ml_features` y modelo `models:/workspace.oceanwatch.ais_isolation_forest/1` registrado en Unity Catalog.

## Tecnologías

MLflow pyfunc, pandas, numpy, cloudpickle, PySpark y Delta Lake.

## Flujo de procesamiento

1. Configurar tablas, versión y checkpoint.
2. Cargar modelo MLflow y consultar firma.
3. Definir esquema de salida: 13 campos de entrada, nueve de predicción y dos de trazabilidad.
4. Serializar modelo con cloudpickle y ejecutar predict por lotes pandas mediante mapInPandas.
5. Leer Delta en streaming y escribir en append, particionado por fecha, con availableNow y awaitTermination.
6. Probar inferencia sobre muestra y revisar completitud, clasificación y comportamiento sin datos nuevos.

## Salidas

`workspace.oceanwatch.ais_gold_ml_predicciones`: score, umbrales, indicadores global/categoría, código y clasificación final, versión y fecha de inferencia, además de campos originales.

## Validaciones y resultados

Prueba histórica reportada: 21.970.432 predicciones para igual cantidad de features; 283.982 anomalías. Se verificó estabilidad de conteos al repetir sin datos nuevos.

## Dependencias y consideraciones

Notebook de desarrollo, no tarea productiva. Checkpoint compartido `/Volumes/workspace/oceanwatch/ais_landing/control/checkpoints/ml_inferencia_v1/`. No ejecutar ambos notebooks concurrentemente ni reiniciar checkpoint sin plan de reconstrucción. El modelo debe existir; su entrenamiento no está incluido en los archivos entregados. Score no equivale a probabilidad o gravedad.
