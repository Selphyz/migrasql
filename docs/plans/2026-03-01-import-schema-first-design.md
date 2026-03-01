# CSV Import Schema-First (Fail-Fast) Design

**Contexto**

El flujo actual de `import` infiere tipos desde una muestra del CSV y usa esos tipos para parsear todas las filas. Cuando la tabla destino ya existe, esta inferencia puede contradecir el esquema real de la base de datos y generar errores masivos de parseo (ej. `DIRCOMP` inferido como entero cuando contiene texto alfanumérico).

**Objetivo**

Respetar con máxima precisión el esquema original de la base de datos destino durante `import`, abortando en el primer error de incompatibilidad de tipo y reportando el contenido de la fila problemática.

## Requisitos Funcionales

- Si la tabla destino existe, el import debe usar los tipos reales de esa tabla como fuente de verdad.
- El parseo de cada celda CSV debe validarse contra el tipo real de su columna destino.
- Ante la primera incompatibilidad de tipo o restricción, el proceso debe abortar (`fail-fast`).
- El error debe incluir:
  - línea CSV
  - columna destino
  - tipo esperado
  - valor recibido
  - vista resumida de la fila completa
- Debe mantenerse compatibilidad con `--columns` (mapeo CSV->BD) y `--start-line`.

## Alcance y No-Alcance

**Incluye**

- Extensión de `DbSession` para leer metadatos de columnas de tabla existente.
- Implementación en motores `mysql` y `postgres`.
- Validación estricta de tipos en `import` usando esquema real.
- Mensajes de error enriquecidos para diagnóstico operativo.

**No incluye**

- Reintento automático con coerciones permisivas.
- Persistencia en disco de checkpoints de progreso.
- Soporte para `sqlserver` en `import` (no está habilitado en CLI actual).

## Opciones Evaluadas

1. Schema-first estricto (recomendada)
- Pro: coherencia con esquema real, errores deterministas y claros.
- Contra: requiere ampliar traits/implementaciones de engine.

2. Validación delegada a la BD (insert-first)
- Pro: menos lógica de parseo en app.
- Contra: menor precisión diagnóstica por columna y motor-dependiente.

3. Híbrido (inferencia + validación BD)
- Pro: detección temprana parcial.
- Contra: duplicación de reglas y mayor complejidad.

## Diseño Técnico

### 1) Contrato de metadatos de columnas

Agregar un método en `DbSession` para recuperar metadatos de columnas de una tabla existente (nombre, tipo lógico normalizado, nulabilidad).

Propuesta conceptual:
- `async fn describe_table_columns(&mut self, table: &str) -> Result<Vec<ColumnSchema>>;`

`ColumnSchema` contendrá:
- `name: String`
- `kind: ColumnKind` (normalizado: Int, Float, Bool, Date, Timestamp, String)
- `nullable: bool`
- `db_type_name: String` (tipo real para mensajes)

### 2) Estrategia de tipado en import

- Si la tabla no existe: se mantiene creación por inferencia para bootstrap inicial.
- Si la tabla existe:
  - leer esquema real desde BD
  - construir vector de tipos esperados en el orden del mapping CSV->BD
  - eliminar dependencia de inferencia para parseo de datos

### 3) Parseo estricto por columna

- `parse_row` debe recibir tipos de esquema real cuando estén disponibles.
- Mantener reglas de `NULL` para vacíos/`null`/`none`.
- Si columna `NOT NULL` recibe valor vacío, error explícito.
- Si valor no parsea al tipo esperado, error inmediato con contexto completo.

### 4) Fail-fast con reporte enriquecido

- Con `skip_errors = false` (configuración requerida para este caso), abortar en primer error.
- Estructurar mensaje con:
  - `line_number`
  - `column_name`
  - `expected_db_type`
  - `raw_value`
  - `row_preview`
- Mantener el `ImportProgressTracker` para señalar último insert exitoso.

### 5) Compatibilidad de mapping

Validaciones previas al import:
- Toda columna destino del mapping debe existir en la tabla.
- Detectar headers CSV sin mapping cuando no exista correspondencia directa.
- Reportar inconsistencias antes de iniciar inserciones.

## Riesgos Potenciales y Mitigaciones

- Mapeo incompleto o invertido: validación temprana con error descriptivo.
- Tipos complejos motor-específicos (`numeric`, `timestamp with time zone`): normalización explícita en `ColumnKind` + `db_type_name` para trazabilidad.
- Diferencias de casing/quoting de nombres: normalizar comparación de identificadores según motor.
- Coste por fila en errores detallados: construir `row_preview` sólo en rutas de error.

## Estrategia de Pruebas

- Unit tests (import):
  - tabla existente + columna string alfanumérica no debe parsearse como int
  - mismatch tipo entero con valor alfanumérico falla en primera fila
  - vacío en `NOT NULL` falla con mensaje claro
  - mapping hacia columna inexistente falla antes de importar
- Unit tests (engines):
  - `describe_table_columns` mapea correctamente tipos comunes MySQL/Postgres
- Smoke tests:
  - import exitoso respetando esquema existente
  - import fail-fast con reporte de fila y columna

## Criterios de Aceptación

- La inferencia de tipos no se usa para parsear filas cuando la tabla ya existe.
- El primer valor incompatible aborta el proceso.
- El error incluye columna, tipo esperado, valor y línea.
- Se conserva `start_line` y mapping de columnas.
- Tests nuevos cubren los escenarios críticos.
