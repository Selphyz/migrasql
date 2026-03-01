pub mod dialect;
pub mod mysql;
pub mod postgres;
pub mod value;

use crate::engine::dialect::SqlDialect;
use crate::engine::value::SqlValue;
use anyhow::Result;
use async_trait::async_trait;
use futures::Stream;
use std::pin::Pin;

/// Stream of rows from a database query
pub type RowStream = Pin<Box<dyn Stream<Item = Result<Vec<SqlValue>>> + Send>>;

/// Database engine trait for provider abstraction
#[async_trait]
pub trait DbEngine: Send + Sync {
    /// Connect to a database using the provider's URL format
    async fn connect(&self, url: &str) -> Result<Box<dyn DbSession>>;
}

/// Active database session for executing queries
#[async_trait]
pub trait DbSession: Send {
    /// Dialect helper for identifier/literal formatting
    fn dialect(&self) -> &'static dyn SqlDialect;

    /// Start a consistent snapshot transaction (REPEATABLE READ)
    async fn start_consistent_snapshot(&mut self) -> Result<()>;

    /// List all tables matching include/exclude filters
    /// Empty include = all tables; exclude list is applied after
    async fn list_tables(&mut self, include: &[String], exclude: &[String]) -> Result<Vec<String>>;

    /// Get CREATE TABLE statement for a table (minified to single line)
    async fn show_create_table(&mut self, table: &str) -> Result<String>;

    /// Stream all rows from a table
    /// Returns rows as Vec<SqlValue> in column order
    async fn stream_rows(&mut self, table: &str) -> Result<(Vec<String>, RowStream)>;

    /// Get approximate row count for a table (for progress indication)
    async fn approximate_row_count(&mut self, table: &str) -> Result<u64>;

    /// Insert a batch of rows into a table
    async fn insert_batch(
        &mut self,
        table: &str,
        column_names: &[String],
        rows: &[Vec<SqlValue>],
    ) -> Result<()>;

    /// Disable foreign key checks
    async fn disable_constraints(&mut self) -> Result<()>;

    /// Enable foreign key checks
    async fn enable_constraints(&mut self) -> Result<()>;

    /// Execute a raw SQL statement
    async fn execute(&mut self, sql: &str) -> Result<()>;

    /// Commit current transaction
    async fn commit(&mut self) -> Result<()>;

    /// Create a table from column definitions
    async fn create_table_from_columns(
        &mut self,
        table: &str,
        column_names: &[String],
        column_types: &[SqlValue],
    ) -> Result<()>;
}

/// Factory for creating database engines
pub fn create_engine(provider: &str) -> Result<Box<dyn DbEngine>> {
    match provider.to_lowercase().as_str() {
        "mysql" => Ok(Box::new(mysql::MysqlEngine)),
        "postgres" => Ok(Box::new(postgres::PostgresEngine)),
        _ => Err(anyhow::anyhow!(
            "Unsupported database provider: {}",
            provider
        )),
    }
}
