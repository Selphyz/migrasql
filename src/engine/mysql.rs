use super::{DbEngine, DbSession, RowStream};
use crate::engine::dialect::SqlDialect;
use crate::engine::value::SqlValue;
use crate::util::dialects::mysql::MYSQL_DIALECT;
use anyhow::{Context, Result};
use async_trait::async_trait;
use futures::stream;
use sqlx::mysql::MySqlConnection;
use sqlx::{Connection, Row};

pub struct MysqlEngine;

#[async_trait]
impl DbEngine for MysqlEngine {
    async fn connect(&self, url: &str) -> Result<Box<dyn DbSession>> {
        let conn = MySqlConnection::connect(url)
            .await
            .context("Failed to connect to MySQL database")?;

        Ok(Box::new(MysqlSession {
            conn,
            in_transaction: false,
        }))
    }
}

pub struct MysqlSession {
    conn: MySqlConnection,
    in_transaction: bool,
}

#[async_trait]
impl DbSession for MysqlSession {
    fn dialect(&self) -> &'static dyn SqlDialect {
        &MYSQL_DIALECT
    }

    async fn start_consistent_snapshot(&mut self) -> Result<()> {
        sqlx::query("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .execute(&mut self.conn)
            .await?;
        sqlx::query("START TRANSACTION WITH CONSISTENT SNAPSHOT")
            .execute(&mut self.conn)
            .await?;
        self.in_transaction = true;
        Ok(())
    }

    async fn list_tables(&mut self, include: &[String], exclude: &[String]) -> Result<Vec<String>> {
        let rows = sqlx::query("SHOW TABLES")
            .fetch_all(&mut self.conn)
            .await
            .context("Failed to list tables")?;

        let mut tables: Vec<String> = rows.iter().map(|row| row.get::<String, _>(0)).collect();

        if !include.is_empty() {
            tables.retain(|t| include.contains(t));
        }

        if !exclude.is_empty() {
            tables.retain(|t| !exclude.contains(t));
        }

        Ok(tables)
    }

    async fn show_create_table(&mut self, table: &str) -> Result<String> {
        let query = format!("SHOW CREATE TABLE `{}`", table.replace('`', "``"));
        let row = sqlx::query(&query)
            .fetch_optional(&mut self.conn)
            .await?
            .context("No CREATE TABLE result")?;

        let create_stmt: String = row.get(1);

        let minified = minify_create_table(&create_stmt);
        Ok(minified)
    }

    async fn stream_rows(&mut self, table: &str) -> Result<(Vec<String>, RowStream)> {
        let query = format!(
            "SELECT COLUMN_NAME FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{}' \
             ORDER BY ORDINAL_POSITION",
            table.replace('\'', "''")
        );
        let col_rows = sqlx::query(&query).fetch_all(&mut self.conn).await?;

        let columns: Vec<String> = col_rows.iter().map(|row| row.get::<String, _>(0)).collect();

        let data_query = format!("SELECT * FROM `{}`", table.replace('`', "``"));
        let rows = sqlx::query(&data_query).fetch_all(&mut self.conn).await?;

        let value_rows: Vec<Result<Vec<SqlValue>>> = rows
            .iter()
            .map(|row| {
                let mut values = Vec::with_capacity(columns.len());
                for i in 0..columns.len() {
                    values.push(convert_sqlx_value(row, i));
                }
                Ok(values)
            })
            .collect();

        let row_stream = stream::iter(value_rows);
        Ok((columns, Box::pin(row_stream)))
    }

    async fn approximate_row_count(&mut self, table: &str) -> Result<u64> {
        let query = format!(
            "SELECT TABLE_ROWS FROM information_schema.TABLES \
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{}'",
            table.replace('\'', "''")
        );

        let count: Option<u64> = sqlx::query_scalar(&query)
            .fetch_optional(&mut self.conn)
            .await?
            .flatten();
        Ok(count.unwrap_or(0))
    }

    async fn insert_batch(
        &mut self,
        table: &str,
        column_names: &[String],
        rows: &[Vec<SqlValue>],
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        let sql = MYSQL_DIALECT.insert_values_sql(table, column_names, rows);
        sqlx::query(&sql)
            .execute(&mut self.conn)
            .await
            .with_context(|| format!("Failed to insert batch into table '{}'", table))?;

        Ok(())
    }

    async fn disable_constraints(&mut self) -> Result<()> {
        sqlx::query("SET FOREIGN_KEY_CHECKS=0")
            .execute(&mut self.conn)
            .await?;
        sqlx::query("SET UNIQUE_CHECKS=0")
            .execute(&mut self.conn)
            .await?;
        Ok(())
    }

    async fn enable_constraints(&mut self) -> Result<()> {
        sqlx::query("SET FOREIGN_KEY_CHECKS=1")
            .execute(&mut self.conn)
            .await?;
        sqlx::query("SET UNIQUE_CHECKS=1")
            .execute(&mut self.conn)
            .await?;
        Ok(())
    }

    async fn execute(&mut self, sql: &str) -> Result<()> {
        sqlx::query(sql)
            .execute(&mut self.conn)
            .await
            .context("Failed to execute SQL statement")?;
        Ok(())
    }

    async fn commit(&mut self) -> Result<()> {
        if self.in_transaction {
            sqlx::query("COMMIT").execute(&mut self.conn).await?;
            self.in_transaction = false;
        }
        Ok(())
    }

    async fn create_table_from_columns(
        &mut self,
        table: &str,
        column_names: &[String],
        column_types: &[SqlValue],
    ) -> Result<()> {
        let mut sql = format!("CREATE TABLE `{}` (\n", table.replace('`', "``"));

        for (i, (col_name, col_type)) in column_names.iter().zip(column_types.iter()).enumerate() {
            let col_quoted = format!("`{}`", col_name.replace('`', "``"));
            let type_str = match col_type {
                SqlValue::Int(_) => "INT".to_string(),
                SqlValue::Float(_) => "FLOAT".to_string(),
                SqlValue::Decimal(_) => "DECIMAL(10,2)".to_string(),
                SqlValue::Bool(_) => "TINYINT(1)".to_string(),
                SqlValue::String(_) => "VARCHAR(255)".to_string(),
                SqlValue::Date { .. } => "DATE".to_string(),
                SqlValue::Time { .. } => "TIME".to_string(),
                SqlValue::Timestamp { .. } => "TIMESTAMP".to_string(),
                SqlValue::Bytes(_) => "BLOB".to_string(),
                SqlValue::Null => "VARCHAR(255)".to_string(),
            };

            let pk = if col_name == "id" && matches!(col_type, SqlValue::Int(_)) {
                " PRIMARY KEY AUTO_INCREMENT"
            } else {
                ""
            };

            sql.push_str(&format!("  {} {}{}", col_quoted, type_str, pk));
            if i < column_names.len() - 1 {
                sql.push(',');
            }
            sql.push('\n');
        }

        sql.push_str(") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");

        sqlx::query(&sql)
            .execute(&mut self.conn)
            .await
            .context("Failed to create table")?;

        Ok(())
    }
}

/// Convert a SQLx MySQL row value to SqlValue
fn convert_sqlx_value(row: &sqlx::mysql::MySqlRow, index: usize) -> SqlValue {
    use chrono::prelude::*;

    // Try each type in order of likelihood
    // First try integer types
    if let Ok(v) = row.try_get::<i64, _>(index) {
        return SqlValue::Int(v);
    }

    // Try bool
    if let Ok(v) = row.try_get::<bool, _>(index) {
        return SqlValue::Bool(v);
    }

    // Try float
    if let Ok(v) = row.try_get::<f64, _>(index) {
        return SqlValue::Float(v);
    }

    // Try NaiveDate
    if let Ok(v) = row.try_get::<NaiveDate, _>(index) {
        return SqlValue::Date {
            y: v.year(),
            m: v.month(),
            d: v.day(),
        };
    }

    // Try NaiveTime
    if let Ok(v) = row.try_get::<NaiveTime, _>(index) {
        return SqlValue::Time {
            neg: false,
            h: v.hour(),
            m: v.minute(),
            s: v.second(),
            us: v.nanosecond() / 1000,
        };
    }

    // Try NaiveDateTime
    if let Ok(v) = row.try_get::<NaiveDateTime, _>(index) {
        return SqlValue::Timestamp {
            y: v.year(),
            m: v.month(),
            d: v.day(),
            hh: v.hour(),
            mm: v.minute(),
            ss: v.second(),
            us: (v.nanosecond() / 1000),
        };
    }

    // Try string
    if let Ok(v) = row.try_get::<String, _>(index) {
        return SqlValue::String(v);
    }

    // Try bytes
    if let Ok(v) = row.try_get::<Vec<u8>, _>(index) {
        return SqlValue::Bytes(v);
    }

    // Default to null
    SqlValue::Null
}

/// Minify CREATE TABLE statement to single line and add IF NOT EXISTS
fn minify_create_table(create_stmt: &str) -> String {
    let single_line = create_stmt
        .lines()
        .map(|l| l.trim())
        .collect::<Vec<_>>()
        .join(" ");

    if single_line.starts_with("CREATE TABLE") {
        single_line.replacen("CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1)
    } else {
        single_line
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_minify_create_table() {
        let input = r#"CREATE TABLE `users` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"#;

        let output = minify_create_table(input);
        assert!(output.contains("CREATE TABLE IF NOT EXISTS"));
        assert!(!output.contains('\n'));
    }
}
