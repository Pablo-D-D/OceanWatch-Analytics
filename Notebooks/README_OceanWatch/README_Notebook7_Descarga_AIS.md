# Notebook 7 — Descarga AIS para Landing

**OceanWatch Analytics** · `Notebook7_Descarga_AIS`

## Descripción y objetivo

Publica archivos NOAA para la ingesta incremental y conserva originales y control de descargas.

## Entradas

URL NOAA de 2023; lista configurada del 1 al 7 de junio de 2023.

## Tecnologías

Python, requests, zipfile, tempfile, shutil, JSON, dbutils.

## Flujo de procesamiento

1. Verificar acceso a Landing y cargar manifiesto.
2. Omitir URLs registradas cuyo ZIP local existe.
3. Descargar ZIP por bloques a temporal y validar CRC.
4. Extraer CSV mediante archivo temporal y renombrar al destino final; conservar ZIP.
5. Registrar descarga completada en manifiesto y comprobar archivos y encabezados.

## Salidas

`/Volumes/workspace/oceanwatch/ais_landing/incoming/`, `originales/` y `control/descargas_ais.json` dentro de ais_landing.

## Validaciones y resultados

Verifica integridad ZIP, presencia de CSV, ausencia de temporales al finalizar y consistencia de encabezados.

## Dependencias y consideraciones

Depende del Notebook 6. Puede activar File Arrival Trigger. El temporal CSV se crea dentro de incoming: no se garantiza que el monitor ignore temporales. Una interrupción entre publicación y registro puede requerir reconciliación manual; no es una transacción global.
