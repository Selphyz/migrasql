use crate::engine::value::SqlValue;

/// SQL dialect abstraction for identifier and literal formatting.
pub trait SqlDialect: Send + Sync {
    /// Dialect display name (used in logs/comments).
    fn name(&self) -> &'static str;

    /// Quote an identifier (table/column name).
    fn quote_identifier(&self, name: &str) -> String;

    /// Convert a neutral `SqlValue` into a literal for this dialect.
    fn to_literal(&self, value: &SqlValue) -> String;

    /// Build an INSERT ... VALUES statement for the provided rows.
    fn insert_values_sql(&self, table: &str, columns: &[String], rows: &[Vec<SqlValue>]) -> String;

    /// Format a drop table statement using the dialect's identifier rules.
    fn drop_table_statement(&self, table: &str) -> String
    where
        Self: Sized,
    {
        format!(
            "DROP TABLE IF EXISTS {}",
            format_qualified_table(self, table)
        )
    }
}

/// Split a qualified table name into (schema, table) components.
pub fn split_table_name(name: &str) -> (Option<&str>, &str) {
    match name.split_once('.') {
        Some((schema, table)) if !schema.is_empty() => (Some(schema), table),
        _ => (None, name),
    }
}

/// Format a potentially schema-qualified table name using the dialect.
pub fn format_qualified_table(dialect: &dyn SqlDialect, name: &str) -> String {
    let (schema, table) = split_table_name(name);
    match schema {
        Some(schema_name) => format!(
            "{}.{}",
            dialect.quote_identifier(schema_name),
            dialect.quote_identifier(table)
        ),
        None => dialect.quote_identifier(table),
    }
}
