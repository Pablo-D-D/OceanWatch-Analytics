# Notebook 6 — Administración de Unity Catalog

**OceanWatch Analytics** · `Notebook6_Admin_UnityCatalog`

## Descripción y objetivo

Prepara e inventaría los recursos de almacenamiento y acceso del proyecto.

## Entradas

`workspace.oceanwatch`, volumen existente `ais_data` y contexto de usuario Databricks.

## Tecnologías

Spark SQL, Unity Catalog, dbutils.

## Flujo de procesamiento

1. Inventariar catálogo, esquema y recursos.
2. Crear volumen `ais_landing` si no existe.
3. Crear directorios incoming, checkpoints y schemas.
4. Consultar permisos, usuario actual y propietarios.
5. Verificar disponibilidad de directorios.

## Salidas

`/Volumes/workspace/oceanwatch/ais_landing/incoming/`, `ais_data/checkpoints/` y `ais_data/schemas/` bajo la misma base; reportes administrativos.

## Validaciones y resultados

Consultas SHOW GRANTS e inventarios de existencia y acceso.

## Dependencias y consideraciones

Precede al Notebook 7 y al pipeline. La sección final de administración de permisos no contiene una concesión ejecutable; no documentar cambios de privilegios como realizados por este notebook.
