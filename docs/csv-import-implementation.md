# Especificación de Implementación: Importación de CSV

## 1. Descripción General

Añadir un nuevo comando `import` a la CLI que permita a los usuarios importar datos de archivos CSV a una tabla en una base de datos MySQL o PostgreSQL. La funcionalidad incluye:

- **Mapeo automático** de columnas basado en headers del CSV
- **Mapeo manual** opcional mediante argumentos CLI
- **Inferencia automática** de tipos de datos
- **Creación automática** de tabla si no existe
- **Procesamiento por batches** para eficiencia
- **Tolerancia a errores** con reporte de filas fallidas
- **Indicador de progreso** visual

## 2. Flujo de Operación

```
CSV File
   ↓
Parse Header (obtener nombres de columnas)
   ↓
Read First N Rows (inferir tipos de datos)
   ↓
Determine Column Mapping (automático o manual)
   ↓
Check/Create Table (si no existe)
   ↓
Process Rows in Batches (skip errores, continuar)
   ↓
Insert Batch via DbSession::insert_batch()
   ↓
Report Summary (filas insertadas, errores, tiempo)
```

## 3. Comando CLI

### Sintaxis
```bash
migrasquiel import \
  --destination postgres://user:pass@localhost/db \
  --input data.csv \
  --table products \
  --provider postgres
```

### Argumentos

| Argumento | Tipo | Obligatorio | Default | Descripción |
|-----------|------|------------|---------|-------------|
| `--destination` / `-d` | String | * | N/A | URL de conexión a la BD destino |
| `--destination-env` | String | - | N/A | Variable de entorno con URL destino (alternativa) |
| `--input` / `-i` | String | * | N/A | Ruta del archivo CSV |
| `--table` / `-t` | String | * | N/A | Nombre de la tabla destino |
| `--provider` / `-p` | String | - | `mysql` | Tipo BD: `mysql` \| `postgres` |
| `--batch-rows` | usize | - | `1000` | Número de registros por batch |
| `--disable-fk-checks` | bool | - | `true` | Deshabilitar restricciones FK durante inserción |
| `--columns` | String | - | N/A | Mapeo manual: `csv_col1:db_col1,csv_col2:db_col2` |
| `--skip-errors` | bool | - | `true` | Continuar si hay errores en filas |

### Ejemplo de Uso Completo

**Importación automática (nombres coinciden):**
```bash
migrasquiel import \
  --destination mysql://root:password@localhost/mydb \
  --input products.csv \
  --table products
```

**Con mapeo manual:**
```bash
migrasquiel import \
  --destination postgres://user:pass@localhost/db \
  --input data.csv \
  --table customers \
  --columns "csv_id:id,csv_nombre:name,csv_email:email"
```

**Con opciones personalizadas:**
```bash
migrasquiel import \
  --destination mysql://root:password@localhost/mydb \
  --input large_file.csv \
  --table items \
  --batch-rows 5000 \
  --skip-errors false
```

## 4. Estructura de Datos: CSV

### Requisitos
- **Delimitador**: Coma (`,`) - estándar
- **Enclosures**: Comillas dobles (`"`)
- **First row**: Header con nombres de columnas
- **Encoding**: UTF-8

### Ejemplo
```csv
id,name,email,created_at,price
1,John Doe,john@example.com,2024-01-15,99.99
2,Jane Smith,jane@example.com,2024-01-16,149.99
3,Bob Johnson,bob@example.com,2024-01-17,199.99
```

## 5. Inferencia de Tipos

El motor analiza los primeros **100 registros** para determinar el tipo de cada columna.

### Algoritmo

```rust
for each column {
    type_scores = {Int: 0, Float: 0, Bool: 0, Date: 0, Timestamp: 0, String: 0}

    for each of first 100 rows {
        value = try_parse(cell)
        if value.matches(Int) {
            type_scores[Int] += 1
        } else if value.matches(Float) {
            type_scores[Float] += 1
        } else if value.matches(Bool) {
            type_scores[Bool] += 1
        } else if value.matches(Timestamp) {
            type_scores[Timestamp] += 1
        } else if value.matches(Date) {
            type_scores[Date] += 1
        } else {
            type_scores[String] += 1
        }
    }

    final_type = type_scores.max()
}
```

### Patrones de Tipo

| Tipo | Patrones | Ejemplos |
|------|----------|----------|
| **Int** | Entero sin punto decimal | `123`, `-45`, `0` |
| **Float** | Número con punto decimal | `3.14`, `-2.5`, `0.0` |
| **Bool** | true/false, yes/no, 1/0 | `true`, `false`, `yes`, `no` |
| **Timestamp** | ISO 8601 con hora | `2024-01-15T10:30:45Z`, `2024-01-15 10:30:45` |
| **Date** | ISO 8601 sin hora | `2024-01-15` |
| **String** | Cualquier otro valor | `abc`, `12abc`, vacío |

### Manejo de NULL

- Celda vacía → `NULL`
- String "NULL" / "null" / "None" → `NULL`

## 6. Mapeo de Columnas

### Modo Automático (Default)

Basado en headers del CSV:
1. Leer first row del CSV
2. Usar esos nombres como nombres de columnas de BD
3. Si no existen en la tabla, fallará al crear la tabla

**Ejemplo:**
```csv
id,name,email
1,John,john@example.com
```
→ Crea/mapea a columnas: `id`, `name`, `email`

### Modo Manual (--columns)

Especificar mapeo explícito:
```
--columns "csv_col1:db_col1,csv_col2:db_col2,csv_col3:db_col3"
```

**Ejemplo:**
```bash
--columns "id:user_id,name:full_name,email:email_address"
```

Mapea:
- Columna CSV `id` → Columna BD `user_id`
- Columna CSV `name` → Columna BD `full_name`
- Columna CSV `email` → Columna BD `email_address`

**Parseo del argumento:**
```rust
fn parse_column_mapping(mapping: &str) -> HashMap<String, String> {
    mapping
        .split(',')
        .filter_map(|pair| {
            let mut parts = pair.trim().split(':');
            match (parts.next(), parts.next()) {
                (Some(csv), Some(db)) => Some((csv.to_string(), db.to_string())),
                _ => None
            }
        })
        .collect()
}
```

## 7. Creación de Tabla

### Lógica

Si la tabla no existe:
1. Usar esquema inferido de los primeros 100 registros
2. Crear tabla con esos tipos
3. Usar columna `id` como PRIMARY KEY si existe y es INT

### SQL Generado

**MySQL:**
```sql
CREATE TABLE products (
    id INT PRIMARY KEY,
    name VARCHAR(255),
    email VARCHAR(255),
    created_at TIMESTAMP,
    price DECIMAL(10,2)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
```

**PostgreSQL:**
```sql
CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    name VARCHAR(255),
    email VARCHAR(255),
    created_at TIMESTAMP,
    price NUMERIC(10,2)
)
```

### Mapeo de Tipos

| SqlValue | MySQL | PostgreSQL |
|----------|-------|-----------|
| Int | INT | INTEGER |
| Float | FLOAT | REAL |
| Decimal | DECIMAL(10,2) | NUMERIC(10,2) |
| Bool | TINYINT(1) | BOOLEAN |
| String | VARCHAR(255) | VARCHAR(255) |
| Date | DATE | DATE |
| Time | TIME | TIME |
| Timestamp | TIMESTAMP | TIMESTAMP |
| Bytes | BLOB | BYTEA |

## 8. Inserción por Batches

### Proceso

```rust
let mut batch = Vec::new();
let mut row_number = 1; // Skip header

for row in csv_reader {
    row_number += 1;

    match parse_row(row, &column_mapping, &inferred_types) {
        Ok(values) => {
            batch.push(values);

            if batch.len() >= batch_rows {
                session.insert_batch(&table_name, &columns, &batch)
                    .await?;
                progress_bar.inc(batch.len() as u64);
                batch.clear();
            }
        }
        Err(e) => {
            error_rows.push((row_number, e.to_string()));
            if !skip_errors {
                return Err(e);
            }
        }
    }
}

// Insert remaining rows
if !batch.is_empty() {
    session.insert_batch(&table_name, &columns, &batch)
        .await?;
    progress_bar.inc(batch.len() as u64);
}
```

### Configuración

- **Tamaño batch default**: 1000 filas
- **Ajustable**: `--batch-rows 5000`
- **Transacción**: Cada batch es una transacción atómica

## 9. Manejo de Errores

### Estrategia: Skip + Reporte

- Si una fila tiene error → Registrar error y continuar
- Al final → Mostrar resumen de errores

### Errores Capturados

1. **Formato CSV**: Fila malformada
2. **Tipo de dato**: Valor incompatible con tipo inferido
3. **Inserción BD**: Violación de constrains, FK, etc.

### Reporte Final

```
═══════════════════════════════════════
CSV Import Summary
═══════════════════════════════════════
Source:        data.csv
Table:         products
Duration:      2.34s
Processed:     10,000 rows
Inserted:      9,950 rows ✓
Failed:        50 rows ✗

Failed rows:
  Line 145: Invalid email format
  Line 267: Duplicate key value
  Line 512: Invalid date format
  ... (show first 10)

Use --verbose to see all errors
═══════════════════════════════════════
```

## 10. Estructura de Archivos

### Nuevos archivos

```
src/import.rs              # Lógica principal de importación
docs/csv-import-implementation.md  # Este archivo
```

### Archivos modificados

```
Cargo.toml                 # Añadir dependencia csv
src/cli.rs                 # Añadir comando Import
src/main.rs                # Dispatch del comando
src/engine/mod.rs          # Extender trait DbSession
src/engine/mysql.rs        # Implementar create_table_from_columns
src/engine/postgres.rs     # Implementar create_table_from_columns
```

## 11. Dependencias

Añadir a `Cargo.toml`:

```toml
[dependencies]
csv = "1.3"
```

Ya están disponibles:
- `sqlx` (database operations)
- `tokio` (async runtime)
- `indicatif` (progress bars)
- `anyhow` (error handling)
- `chrono` (date/time parsing)

## 12. Ejemplo de Implementación: src/import.rs

```rust
use std::collections::HashMap;
use std::path::Path;
use anyhow::{anyhow, Result};
use csv::ReaderBuilder;
use indicatif::{ProgressBar, ProgressStyle};
use crate::engine::{DbEngine, SqlValue};

pub struct ImportOptions {
    pub input: String,
    pub table: String,
    pub batch_rows: usize,
    pub disable_fk_checks: bool,
    pub skip_errors: bool,
    pub column_mapping: Option<HashMap<String, String>>,
}

pub async fn import(
    engine: &dyn DbEngine,
    url: &str,
    options: ImportOptions,
) -> Result<()> {
    // 1. Connect to database
    let mut session = engine.connect(url).await?;

    // 2. Read CSV header
    let file = std::fs::File::open(&options.input)?;
    let mut csv_reader = ReaderBuilder::new()
        .from_reader(file);

    let headers = csv_reader.headers()?;
    let csv_columns: Vec<String> = headers
        .iter()
        .map(|h| h.to_string())
        .collect();

    // 3. Get column mapping
    let column_mapping = options.column_mapping.clone()
        .unwrap_or_else(|| {
            csv_columns.iter()
                .map(|c| (c.clone(), c.clone()))
                .collect()
        });

    let db_columns: Vec<String> = csv_columns.iter()
        .map(|csv_col| {
            column_mapping.get(csv_col)
                .cloned()
                .unwrap_or_else(|| csv_col.clone())
        })
        .collect();

    // 4. Infer types from first 100 rows
    let mut inferred_types = infer_column_types(&mut csv_reader, 100)?;

    // 5. Check/Create table
    let table_exists = session.list_tables(None, None)
        .await?
        .iter()
        .any(|t| t == &options.table);

    if !table_exists {
        create_table(&mut session, &options.table, &db_columns, &inferred_types)
            .await?;
    }

    // 6. Setup for insertion
    if options.disable_fk_checks {
        session.disable_constraints().await?;
    }

    // 7. Setup progress bar
    let progress = ProgressBar::new(0);
    progress.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] {msg} {pos}/{len} rows")
        .unwrap());
    progress.set_message("Importing");

    // 8. Process and insert rows
    let file = std::fs::File::open(&options.input)?;
    let mut csv_reader = ReaderBuilder::new()
        .from_reader(file);

    let mut batch = Vec::new();
    let mut error_rows = Vec::new();
    let mut row_number = 1; // Skip header

    for result in csv_reader.deserialize::<Vec<String>>() {
        row_number += 1;

        match result {
            Ok(row) => {
                match parse_row(&row, &csv_columns, &db_columns, &inferred_types) {
                    Ok(values) => {
                        batch.push(values);

                        if batch.len() >= options.batch_rows {
                            session.insert_batch(&options.table, &db_columns, &batch)
                                .await?;
                            progress.inc(batch.len() as u64);
                            batch.clear();
                        }
                    }
                    Err(e) => {
                        error_rows.push((row_number, e.to_string()));
                        if !options.skip_errors {
                            return Err(e);
                        }
                    }
                }
            }
            Err(e) => {
                error_rows.push((row_number, format!("CSV parse error: {}", e)));
                if !options.skip_errors {
                    return Err(anyhow!(e));
                }
            }
        }
    }

    // 9. Insert remaining batch
    if !batch.is_empty() {
        session.insert_batch(&options.table, &db_columns, &batch)
            .await?;
        progress.inc(batch.len() as u64);
    }

    // 10. Re-enable constraints
    if options.disable_fk_checks {
        session.enable_constraints().await?;
    }

    // 11. Commit
    session.commit().await?;

    // 12. Report summary
    progress.finish_with_message(format!(
        "✓ Imported {} rows (with {} errors)",
        row_number - 1 - error_rows.len(),
        error_rows.len()
    ));

    if !error_rows.is_empty() {
        eprintln!("\nFailed rows:");
        for (line, err) in error_rows.iter().take(10) {
            eprintln!("  Line {}: {}", line, err);
        }
        if error_rows.len() > 10 {
            eprintln!("  ... and {} more errors", error_rows.len() - 10);
        }
    }

    Ok(())
}

fn infer_column_types(
    csv_reader: &mut csv::Reader<std::fs::File>,
    sample_size: usize,
) -> Result<Vec<SqlValue>> {
    // Implementation: analyze first N rows and return inferred types
    todo!()
}

fn parse_row(
    row: &[String],
    csv_columns: &[String],
    db_columns: &[String],
    types: &[SqlValue],
) -> Result<Vec<SqlValue>> {
    // Implementation: convert CSV string values to SqlValue
    todo!()
}

async fn create_table(
    session: &mut dyn crate::engine::DbSession,
    table_name: &str,
    columns: &[String],
    types: &[SqlValue],
) -> Result<()> {
    session.create_table_from_columns(table_name, columns, types).await
}

fn parse_column_mapping(mapping: &str) -> Result<HashMap<String, String>> {
    mapping
        .split(',')
        .filter_map(|pair| {
            let mut parts = pair.trim().split(':');
            match (parts.next(), parts.next()) {
                (Some(csv), Some(db)) => Some(Ok((csv.to_string(), db.to_string()))),
                _ => None
            }
        })
        .collect()
}
```

## 13. Integración CLI

### Actualización en src/cli.rs

```rust
#[derive(Parser)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    // ... existing commands ...

    #[command(about = "Import CSV file to database table")]
    Import {
        #[arg(short, long)]
        destination: Option<String>,

        #[arg(long)]
        destination_env: Option<String>,

        #[arg(short, long)]
        input: String,

        #[arg(short, long)]
        table: String,

        #[arg(long, default_value = "mysql", value_parser = ["mysql", "postgres"])]
        provider: String,

        #[arg(long, default_value = "1000")]
        batch_rows: usize,

        #[arg(long, default_value = "true")]
        disable_fk_checks: bool,

        #[arg(long)]
        columns: Option<String>,

        #[arg(long, default_value = "true")]
        skip_errors: bool,
    },
}

impl Commands {
    pub fn get_import_options(&self) -> Result<ImportOptions> {
        if let Commands::Import {
            input,
            table,
            batch_rows,
            disable_fk_checks,
            columns,
            skip_errors,
            ..
        } = self {
            let column_mapping = columns.as_ref()
                .map(|c| parse_column_mapping(c))
                .transpose()?;

            Ok(ImportOptions {
                input: input.clone(),
                table: table.clone(),
                batch_rows: *batch_rows,
                disable_fk_checks: *disable_fk_checks,
                skip_errors: *skip_errors,
                column_mapping,
            })
        } else {
            Err(anyhow!("Not an import command"))
        }
    }
}
```

### Actualización en src/main.rs

```rust
Commands::Import { destination, destination_env, provider, .. } => {
    let url = Commands::get_url(&destination, &destination_env, "destination")?;
    let engine = engine::create_engine(&provider)?;
    let opts = cli.command.get_import_options()?;

    import::import(&*engine, &url, opts).await?;
}
```

## 14. Extensión de Traits

### Actualizar src/engine/mod.rs

```rust
#[async_trait]
pub trait DbSession {
    // ... existing methods ...

    async fn create_table_from_columns(
        &mut self,
        table_name: &str,
        columns: &[String],
        types: &[SqlValue],
    ) -> Result<()>;
}
```

### Implementar en src/engine/mysql.rs

```rust
async fn create_table_from_columns(
    &mut self,
    table_name: &str,
    columns: &[String],
    types: &[SqlValue],
) -> Result<()> {
    let mut sql = format!("CREATE TABLE {} (\n", self.dialect.quote_identifier(table_name));

    for (i, (col, typ)) in columns.iter().zip(types.iter()).enumerate() {
        let col_quoted = self.dialect.quote_identifier(col);
        let type_str = match typ {
            SqlValue::Int(_) => "INT",
            SqlValue::Float(_) => "FLOAT",
            SqlValue::Decimal(_) => "DECIMAL(10,2)",
            SqlValue::Bool(_) => "TINYINT(1)",
            SqlValue::String(_) => "VARCHAR(255)",
            SqlValue::Date(_) => "DATE",
            SqlValue::Timestamp(_) => "TIMESTAMP",
            SqlValue::Bytes(_) => "BLOB",
            SqlValue::Null => "VARCHAR(255)",
            _ => "VARCHAR(255)",
        };

        let pk = if col == "id" && matches!(typ, SqlValue::Int(_)) {
            " PRIMARY KEY"
        } else {
            ""
        };

        sql.push_str(&format!("  {} {}{}", col_quoted, type_str, pk));
        if i < columns.len() - 1 {
            sql.push(',');
        }
        sql.push('\n');
    }

    sql.push_str(") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");

    sqlx::query(&sql).execute(&mut *self.conn).await?;
    Ok(())
}
```

### Implementar en src/engine/postgres.rs

```rust
async fn create_table_from_columns(
    &mut self,
    table_name: &str,
    columns: &[String],
    types: &[SqlValue],
) -> Result<()> {
    let mut sql = format!("CREATE TABLE {} (\n", self.dialect.quote_identifier(table_name));

    for (i, (col, typ)) in columns.iter().zip(types.iter()).enumerate() {
        let col_quoted = self.dialect.quote_identifier(col);
        let type_str = match typ {
            SqlValue::Int(_) => "INTEGER",
            SqlValue::Float(_) => "REAL",
            SqlValue::Decimal(_) => "NUMERIC(10,2)",
            SqlValue::Bool(_) => "BOOLEAN",
            SqlValue::String(_) => "VARCHAR(255)",
            SqlValue::Date(_) => "DATE",
            SqlValue::Timestamp(_) => "TIMESTAMP",
            SqlValue::Bytes(_) => "BYTEA",
            SqlValue::Null => "VARCHAR(255)",
            _ => "VARCHAR(255)",
        };

        let pk = if col == "id" && matches!(typ, SqlValue::Int(_)) {
            " PRIMARY KEY"
        } else {
            ""
        };

        sql.push_str(&format!("  {} {}{}", col_quoted, type_str, pk));
        if i < columns.len() - 1 {
            sql.push(',');
        }
        sql.push('\n');
    }

    sql.push_str(")");

    sqlx::query(&sql).execute(&mut *self.conn).await?;
    Ok(())
}
```

## 15. Casos de Uso

### Caso 1: Importación Simple
```bash
migrasquiel import \
  --destination mysql://root:password@localhost/mydb \
  --input products.csv \
  --table products
```

**CSV:**
```
id,name,price,stock
1,Laptop,999.99,10
2,Mouse,29.99,50
3,Keyboard,79.99,30
```

**Resultado:** Tabla `products` creada con 3 columnas, 3 registros insertados

### Caso 2: Mapeo de Columnas
```bash
migrasquiel import \
  --destination postgres://user:pass@localhost/db \
  --input customers.csv \
  --table users \
  --columns "customer_id:id,customer_name:full_name,customer_email:email"
```

**CSV:**
```
customer_id,customer_name,customer_email
1,John Doe,john@example.com
2,Jane Smith,jane@example.com
```

**Resultado:** Columnas CSV mapeadas a `id`, `full_name`, `email` en tabla `users`

### Caso 3: Inserción con Errores
```bash
migrasquiel import \
  --destination mysql://root:password@localhost/mydb \
  --input data.csv \
  --table items
```

**CSV (con error en fila 3):**
```
id,name,date
1,Item A,2024-01-15
2,Item B,2024-01-16
3,Item C,invalid-date
4,Item D,2024-01-18
```

**Resultado:**
```
✓ Imported 3 rows (with 1 error)

Failed rows:
  Line 4: Invalid date format for column 'date'
```

Registros 1, 2, 4 insertados; registro 3 saltado

## 16. Testing

Ejemplos de pruebas unitarias para implementar:

- `test_infer_types()` - Verificar inferencia correcta de tipos
- `test_parse_row()` - Validar parseo de filas CSV
- `test_column_mapping()` - Mapeo automático y manual
- `test_error_handling()` - Manejo de filas con error
- `test_create_table()` - Creación de tabla basada en esquema

## 17. Limitaciones y Consideraciones

1. **Tamaño**: No hay límite hard de tamaño de archivo (streaming)
2. **Memoria**: Procesamiento por batches mantiene bajo uso de memoria
3. **Tipos**: Se infieren de los datos, no soporta especificación manual de tipos
4. **Comillas**: Soporta comillas simples y dobles en CSV
5. **Caracteres especiales**: UTF-8 validated
6. **NULL handling**: Valores vacíos y "NULL" se tratan como NULL

