# Notebook 14 — Inferencia ML en producción

**OceanWatch Analytics** · `Notebook14_ML_Inferencia_Prod`

## Descripción y objetivo

Ejecuta exclusivamente el flujo operativo de inferencia incremental, sin bloques de pruebas del Notebook 13.

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

## Salidas

`workspace.oceanwatch.ais_gold_ml_predicciones`: score, umbrales, indicadores global/categoría, código y clasificación final, versión y fecha de inferencia, además de campos originales.

## Validaciones y resultados

Reporta disponibilidad de tablas, firma, esquema y tiempo de ejecución. La prueba automática final reportada incorporó 3.571 predicciones y 56 anomalías; la validación funcional no mide precisión predictiva.

## Dependencias y consideraciones

Tarea ML_Inferencia del Job_Oceanwatch_AIS, después de ETL y antes del refresco del dashboard. Checkpoint compartido `/Volumes/workspace/oceanwatch/ais_landing/control/checkpoints/ml_inferencia_v1/`. No ejecutar ambos notebooks concurrentemente ni reiniciar checkpoint sin plan de reconstrucción. El modelo debe existir; su entrenamiento no está incluido en los archivos entregados. Score no equivale a probabilidad o gravedad.
