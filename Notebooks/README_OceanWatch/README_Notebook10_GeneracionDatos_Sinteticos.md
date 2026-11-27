# Notebook 10 — Generación de datos sintéticos

**OceanWatch Analytics** · `Notebook10_GeneracionDatos_Sinteticos`

## Descripción y objetivo

Genera lotes de prueba para calidad, ingesta incremental e inferencia.

## Entradas

`workspace.oceanwatch.ais_silver`; parámetros cantidad_registros, porcentaje_invalidos, fecha_inicio, fecha_fin, semilla y guardar_archivo.

## Tecnologías

Python, PySpark, random, datetime, uuid, dbutils.

## Flujo de procesamiento

1. Explorar y muestrear Silver; el notebook contiene etapas de desarrollo y una función final del bloque 8.
2. Seleccionar referencias válidas y copiar registros con fechas aleatorias dentro del intervalo.
3. Introducir errores controlados de MMSI, fecha, LAT, LON y SOG en índices seleccionados.
4. Construir DataFrame de 17 columnas de texto y comprobar distribución de errores.
5. Opcionalmente escribir CSV fuera de incoming y moverlo a Landing con nombre único.

## Salidas

DataFrame y, con guardar_archivo=True, `AIS_SINTETICO_*.csv` en `/Volumes/workspace/oceanwatch/ais_landing/incoming/`.

## Validaciones y resultados

Prueba final reportada en la conversación: 10.000 válidos, 3.571 evaluados por ML, 56 anomalías (1,57 % de evaluados). La función valida cantidades y errores por regla.

## Dependencias y consideraciones

Requiere Silver disponible. No inyecta anomalías ML deliberadas ni garantiza elegibilidad Gold ML. La celda final guarda archivos; usar guardar_archivo=False para pruebas sin publicación. Mantener trigger pausado si no se desea ejecución automática.
