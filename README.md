# Migrasquiel

A fast, pure-Rust database migration tool for MySQL/MariaDB and PostgreSQL with support for dumping, restoring, and direct server-to-server migrations.

## Features

- **Multi-engine Support**: One binary handles MySQL/MariaDB (`mysql_async`) and PostgreSQL (`sqlx`)
- **Streaming Architecture**: Handles databases of any size with minimal memory usage
- **Three Operation Modes**: Dump, restore, and direct migrate
- **Consistent Snapshots**: Optional REPEATABLE READ transactions for point-in-time consistency
- **Compression Support**: Optional gzip compression for dump files
- **Progress Indicators**: Real-time progress bars for all operations
- **Flexible Filtering**: Include/exclude specific tables
- **Schema and Data Control**: Choose to migrate schema-only, data-only, or both
- **Batched Inserts**: Configurable batch sizes for optimal performance
- **Foreign Key Handling**: Automatic constraint management during migrations (dialect aware)

## Installation

### From Source

```bash
git clone https://github.com/yourusername/migrasquiel.git
cd migrasquiel
cargo build --release
```

The binary will be available at `target/release/migrasquiel`.

### Using Cargo

```bash
cargo install migrasquiel
```

## Usage

### Basic Commands

#### Dump

Dump a database to a SQL file. The provider defaults to `mysql`; set `--provider` for PostgreSQL or SQL Server.

```bash
# MySQL / MariaDB
migrasquiel dump \
  --source "mysql://user:pass@localhost:3306/mydb" \
  --output backup.sql

# PostgreSQL
migrasquiel dump \
  --provider postgres \
  --source "postgres://user:pass@localhost:5432/mydb" \
  --output pg_backup.sql

# SQL Server
migrasquiel dump \
  --provider sqlserver \
  --source "mssql://user:pass@localhost:1433/mydb?encrypt=true&trustservercertificate=true" \
  --output mssql_backup.sql.gz \
  --gzip
```

Environment variables can still be used for connection strings:

```bash
export POSTGRES_SOURCE_URL="postgres://user:pass@localhost:5432/mydb"
migrasquiel dump \
  --provider postgres \
  --source-env POSTGRES_SOURCE_URL \
  --output backup.sql
```

#### Restore

Restore a database from a SQL file (automatically handles gzip files):

```bash
migrasquiel restore \
  --provider sqlserver \
  --destination "mssql://user:pass@localhost:1433/newdb?encrypt=true&trustservercertificate=true" \
  --input backup.sql.gz
```

#### Migrate

Direct server-to-server migration (no intermediate file):

```bash
migrasquiel migrate \
  --provider postgres \
  --source "postgres://user:pass@source.host:5432/sourcedb" \
  --destination "postgres://user:pass@dest.host:5432/destdb"
```

### Connection URL Formats

- **MySQL / MariaDB**: `mysql://user:pass@host:3306/database`
- **PostgreSQL**: `postgres://user:pass@host:5432/database?sslmode=require`
- **SQL Server**: `mssql://user:pass@host:1433/database?encrypt=true&trustservercertificate=true`

All providers accept environment variables (`--source-env`, `--destination-env`) and respect additional query parameters understood by their native clients.

## Advanced Options

### Table Filtering

Include only specific tables:

```bash
migrasquiel dump \
  --source "mysql://user:pass@localhost:3306/mydb" \
  --output backup.sql \
  --tables users,orders,products
```

Exclude specific tables:

```bash
migrasquiel dump \
  --source "mysql://user:pass@localhost:3306/mydb" \
  --output backup.sql \
  --exclude temp_data,logs
```

### Schema and Data Control

Dump schema only (no data):

```bash
migrasquiel dump \
  --source "mysql://user:pass@localhost:3306/mydb" \
  --output schema.sql \
  --schema-only
```

Dump data only (no schema):

```bash
migrasquiel dump \
  --source "mysql://user:pass@localhost:3306/mydb" \
  --output data.sql \
  --data-only
```

### Consistent Snapshots

For consistent point-in-time backups:

```bash
migrasquiel dump \
  --source "mysql://user:pass@localhost:3306/mydb" \
  --output backup.sql \
  --consistent-snapshot
```

This uses `START TRANSACTION WITH CONSISTENT SNAPSHOT` to ensure all tables are dumped at the same point in time.

### Performance Tuning

Adjust batch size for inserts (default: 1000 rows):

```bash
migrasquiel migrate \
  --source "mysql://user:pass@source:3306/db1" \
  --destination "mysql://user:pass@dest:3306/db2" \
  --batch-rows 5000
```

## Complete Examples

### Example 1: Full Database Backup

```bash
# Create a compressed, consistent backup
migrasquiel dump \
  --source "mysql://root:password@localhost:3306/production" \
  --output "production-$(date +%Y%m%d).sql.gz" \
  --consistent-snapshot \
  --gzip
```

### Example 2: Clone Database to Different Server

```bash
# Direct migration with progress tracking
migrasquiel migrate \
  --source "mysql://user:pass@prod.server:3306/myapp" \
  --destination "mysql://user:pass@staging.server:3306/myapp_test" \
  --consistent-snapshot
```

### Example 3: Migrate Specific Tables

```bash
# Migrate only user-related tables
migrasquiel migrate \
  --source "mysql://user:pass@old.server:3306/legacy" \
  --destination "mysql://user:pass@new.server:3306/modern" \
  --tables users,user_profiles,user_sessions \
  --batch-rows 2000
```

### Example 4: Development Database Setup

```bash
# Restore schema only for development
migrasquiel restore \
  --destination "mysql://root:dev@localhost:3306/dev_db" \
  --input production-schema.sql

# Then load sample data separately
migrasquiel restore \
  --destination "mysql://root:dev@localhost:3306/dev_db" \
  --input sample-data.sql
```

## Connection URLs

Connection URLs follow the standard MySQL format:

```
mysql://[user[:password]@][host][:port]/database
```

Examples:

- `mysql://root:password@localhost:3306/mydb`
- `mysql://user@192.168.1.100/testdb`
- `mysql://admin:secret@db.example.com:3307/production`

### Security Note

Credentials in command-line arguments may be visible in process listings. For production use, prefer environment variables:

```bash
export MYSQL_SOURCE_URL="mysql://user:password@host/db"
migrasquiel dump --source-env MYSQL_SOURCE_URL --output backup.sql
```

Passwords in URLs are automatically redacted in console output.

## Architecture

### Design Principles

- **Streaming-First**: All operations stream data to minimize memory usage
- **Provider-Agnostic**: Core abstractions (`DbEngine`, `DbSession`) keep engine-specific logic isolated
- **Zero Native Dependencies**: Pure Rust implementation backed by async drivers (`mysql_async`, `sqlx`, `tiberius`)
- **Fail-Fast with Context**: Clear error messages with context about what operation failed

### Performance Characteristics

- **Memory Usage**: O(batch_size) - only one batch of rows in memory at a time
- **Disk Usage**: 
  - Dump: O(database_size * compression_ratio)
  - Migrate: O(1) - no intermediate files
- **Network**: Fully streaming with configurable batch sizes

## Troubleshooting

### Connection Issues

```
Error: Failed to connect to <provider> database
```

**Solutions**:
- Verify the connection URL format (including TLS options for PostgreSQL/SQL Server)
- Confirm the database service is running and reachable from the migrasquiel host
- Ensure firewall rules permit inbound/outbound traffic on the target port
- Check credentials and database-level permissions for the selected provider

### Large Tables

For very large tables (>100M rows), consider:

- Increasing `--batch-rows` to reduce round trips (e.g., `--batch-rows 5000`)
- Using `--consistent-snapshot` only when necessary (adds overhead)
- Monitoring disk space when using `--gzip`

### Character Encoding

The tool uses UTF-8 by default. If you encounter encoding issues:

- Verify source database charset: `SHOW VARIABLES LIKE 'character_set%';`
- Check table definitions: `SHOW CREATE TABLE tablename;`

## Future Enhancements

Planned features for future versions:

- SQLite support  
- Parallel table processing
- Incremental backups
- `--continue-on-error` flag for partial migrations
- Custom SQL transformations during migration
- Dry-run mode

## Technical Details

### SQL Value Handling

Column values are converted into an engine-neutral `SqlValue` enum before being rendered through the target dialect. This guarantees consistent handling for:

- `NULL`, booleans, signed/unsigned integers, and floating point types (with NaN/±Inf support)
- Arbitrary-precision numerics (preserved as decimal strings)
- Strings with proper quoting/escaping for each provider
- Binary data (hex, `BYTEA`, or `0x` literals as appropriate)
- Date, time, and timestamp values with microsecond precision

### Transaction Handling

- Dumps: Optional consistent snapshot transaction
- Restores: Foreign key checks disabled during load
- Migrates: Consistent reads from source, safe writes to destination

### Generated SQL Format

Dump files are rendered through each engine's dialect so that identifiers, literals, and bulk INSERT statements match native tooling expectations:

- MySQL/MariaDB: session-preserving headers, backtick identifiers, hex vs text detection for blobs
- PostgreSQL: UTF-8/standard_conforming defaults, double-quoted identifiers, bytea literals
- SQL Server: ANSI quoted identifiers, constraint toggles, and `0x` binary literals

Every statement is terminated with `;\n` for reliable parsing during restore regardless of provider.

## Command Reference

### `dump`

| Flag | Description | Default |
|------|-------------|---------|
| `--source` | Source database URL | - |
| `--source-env` | Environment variable with source URL | - |
| `--output` | Output file path | - |
| `--provider` | Database provider (`mysql|postgres|sqlserver`) | `mysql` |
| `--tables` | Tables to include (comma-separated) | all |
| `--exclude` | Tables to exclude (comma-separated) | none |
| `--schema-only` | Dump schema only | `false` |
| `--data-only` | Dump data only | `false` |
| `--batch-rows` | Rows per INSERT batch | `1000` |
| `--consistent-snapshot` | Use consistent snapshot | `false` |
| `--gzip` | Compress output | `false` |

### `restore`

| Flag | Description | Default |
|------|-------------|---------|
| `--destination` | Destination database URL | - |
| `--destination-env` | Environment variable with destination URL | - |
| `--input` | Input file path | - |
| `--provider` | Database provider (`mysql|postgres|sqlserver`) | `mysql` |
| `--disable-fk-checks` | Disable foreign key checks | `true` |

### `migrate`

| Flag | Description | Default |
|------|-------------|---------|
| `--source` | Source database URL | - |
| `--source-env` | Environment variable with source URL | - |
| `--destination` | Destination database URL | - |
| `--destination-env` | Environment variable with destination URL | - |
| `--provider` | Database provider (`mysql|postgres|sqlserver`) | `mysql` |
| `--tables` | Tables to include (comma-separated) | all |
| `--exclude` | Tables to exclude (comma-separated) | none |
| `--schema-only` | Migrate schema only | `false` |
| `--data-only` | Migrate data only | `false` |
| `--batch-rows` | Rows per INSERT batch | `1000` |
| `--consistent-snapshot` | Use consistent snapshot | `false` |
| `--disable-fk-checks` | Disable foreign key checks | `true` |
