# CSV Import Examples

Esta guía contiene ejemplos prácticos de cómo usar el comando `import` de migrasquiel.

## Ejemplo 1: Importación Simple

**Archivo CSV (`products.csv`):**
```csv
id,name,price,stock
1,Laptop,999.99,10
2,Mouse,29.99,50
3,Keyboard,79.99,30
4,Monitor,299.99,15
5,USB Cable,9.99,100
```

**Comando:**
```bash
migrasquiel import \
  --destination mysql://root:password@localhost/mydb \
  --input products.csv \
  --table products \
  --provider mysql
```

**Resultado:**
- Se crea la tabla `products` con las columnas inferidas de los headers
- Se insertan 5 registros
- La columna `id` se detecta como PRIMARY KEY

## Ejemplo 2: Importación con Mapeo de Columnas

Si los nombres de las columnas en el CSV no coinciden con los nombres en la BD:

**Archivo CSV (`customer_data.csv`):**
```csv
customer_id,full_name,email_address,registration_date
1,John Doe,john@example.com,2024-01-15
2,Jane Smith,jane@example.com,2024-01-16
3,Bob Johnson,bob@example.com,2024-01-17
```

**Comando:**
```bash
migrasquiel import \
  --destination postgres://user:password@localhost/mydatabase \
  --input customer_data.csv \
  --table users \
  --provider postgres \
  --columns "customer_id:id,full_name:name,email_address:email,registration_date:created_at"
```

**Resultado:**
- Columna CSV `customer_id` → Columna BD `id`
- Columna CSV `full_name` → Columna BD `name`
- Columna CSV `email_address` → Columna BD `email`
- Columna CSV `registration_date` → Columna BD `created_at`

## Ejemplo 3: Importación con Configuración de URL via Variable de Entorno

**Configurar variable de entorno:**
```bash
export DATABASE_URL="mysql://root:password@localhost/producción"
```

**Comando:**
```bash
migrasquiel import \
  --destination-env DATABASE_URL \
  --input sales.csv \
  --table orders
```

## Ejemplo 4: Importación Grande con Batch Personalizado

Para archivos muy grandes, ajusta el tamaño del batch:

```bash
migrasquiel import \
  --destination mysql://root:password@localhost/bigdata \
  --input large_dataset.csv \
  --table transactions \
  --batch-rows 5000 \
  --disable-fk-checks
```

- `--batch-rows 5000`: Inserta 5000 registros por transacción (default: 1000)
- `--disable-fk-checks`: Deshabilita validación de claves foráneas para más velocidad

## Ejemplo 5: Manejo de Errores

**CSV con algunos errores:**
```csv
id,amount,date,status
1,100.50,2024-01-15,completed
2,200.75,2024-01-16,completed
3,300.00,invalid-date,pending
4,400.25,2024-01-18,completed
```

**Comando con tolerancia a errores:**
```bash
migrasquiel import \
  --destination mysql://root:password@localhost/mydb \
  --input sales.csv \
  --table sales \
  --skip-errors
```

**Resultado:**
```
═══════════════════════════════════════
CSV Import Summary
═══════════════════════════════════════
Source:        sales.csv
Table:         sales
Total rows:    5 (including header)
Inserted:      3 rows ✓
Failed:        1 rows ✗
═══════════════════════════════════════

Failed rows:
  Line 4: Failed to parse 'invalid-date' as date (YYYY-MM-DD) for column 'date'
```

Los registros 1, 2 y 4 se insertan correctamente; el registro 3 se salta.

## Ejemplo 6: Tipos de Datos Soportados

El sistema detecta automáticamente los siguientes tipos:

**CSV con varios tipos (`mixed_types.csv`):**
```csv
id,name,price,active,created_at
1,Product A,99.99,true,2024-01-15 10:30:45
2,Product B,149.99,false,2024-01-16 11:45:30
3,Product C,199.99,yes,2024-01-17 14:20:15
```

**Tipos detectados:**
- `id` → INT (entero)
- `name` → VARCHAR (texto)
- `price` → FLOAT (decimal con punto)
- `active` → BOOLEAN (true/false/yes/no/1/0)
- `created_at` → TIMESTAMP (fecha y hora)

## Ejemplo 7: Columnas con NULL

**CSV con valores nulos:**
```csv
id,name,email,phone
1,John Doe,john@example.com,555-1234
2,Jane Smith,jane@example.com,
3,Bob Johnson,,555-5678
```

**Resultado:**
- Celdas vacías se insertan como NULL
- También se reconocen "NULL", "null", "None" como NULL
- Las columnas se crean como nullable

## Ejemplo 8: Valores Booleanos

Varias representaciones se soportan:

```csv
id,name,active,enabled,verified
1,User A,true,yes,1
2,User B,false,no,0
3,User C,TRUE,YES,1
```

Todas las siguientes se reconocen como `true`:
- `true`, `TRUE`, `True`
- `yes`, `YES`, `Yes`
- `1`

Y como `false`:
- `false`, `FALSE`, `False`
- `no`, `NO`, `No`
- `0`

## Ejemplo 9: Tabla ya Existe

Si la tabla ya existe, solo se insertan los datos:

```bash
# Primera importación: crea tabla
migrasquiel import \
  --destination mysql://root:password@localhost/mydb \
  --input batch1.csv \
  --table products

# Segunda importación: añade datos a tabla existente
migrasquiel import \
  --destination mysql://root:password@localhost/mydb \
  --input batch2.csv \
  --table products
```

Ambas importaciones usan la misma tabla.

## Ejemplo 10: PostgreSQL vs MySQL

**MySQL:**
```bash
migrasquiel import \
  --destination mysql://user:pass@localhost/db \
  --input data.csv \
  --table mytable \
  --provider mysql
```

**PostgreSQL:**
```bash
./migrasquiel import --destination postgres://pguser:pgpass@sqldb.postgres.database.azure.com/postgres --input /mnt/h/gi-herencias/ELE/ELEFIN_part_34.csv --table personas --provider postgres
./migrasquiel import --destination postgres://pguser:pgpass@sqldb.postgres.database.azure.com/postgres --input /mnt/h/gi-herencias/ELE/ELEFIN_part_1.csv --table personas --provider postgres &
./migrasquiel import --destination postgres://pguser:pgpass@sqldb.postgres.database.azure.com/postgres --input /mnt/h/gi-herencias/ELE/ELEFIN_part_2.csv --table personas --provider postgres &
./migrasquiel import --destination postgres://pguser:pgpass@sqldb.postgres.database.azure.com/postgres --input /mnt/h/gi-herencias/ELE/ELEFIN_part_3.csv --table personas --provider postgres &
./migrasquiel import --destination postgres://pguser:pgpass@sqldb.postgres.database.azure.com/postgres --input /mnt/h/gi-herencias/ELE/ELEFIN_part_4.csv --table personas --provider postgres &
./migrasquiel import --destination postgres://pguser:pgpass@sqldb.postgres.database.azure.com/postgres --input /mnt/h/gi-herencias/ELE/ELEFIN_part_5.csv --table personas --provider postgres
```

Ambos dialectos soportan:
- Creación automática de tabla
- Mapeo de columnas
- Inferencia de tipos
- Inserción por batches
- Manejo de errores

## Guía de Resolución de Problemas

### Error: "Failed to connect to database"
- Verifica que la URL de conexión es correcta
- Comprueba que el servidor de BD está ejecutándose
- Verifica credenciales de usuario

### Error: "Input file not found"
- Verifica que el archivo CSV existe
- Usa ruta absoluta o verifica el directorio actual

### Error: "Failed to parse '...' as ..."
- Usa `--skip-errors` para continuar con otras filas
- Verifica el formato de fechas (debe ser YYYY-MM-DD)
- Verifica el formato de timestamps (debe ser YYYY-MM-DD HH:MM:SS)

### Inserción lenta
- Aumenta `--batch-rows` (ej: 5000)
- Usa `--disable-fk-checks` para deshabilitar validaciones
- Verifica ancho de banda de red (para BD remota)

