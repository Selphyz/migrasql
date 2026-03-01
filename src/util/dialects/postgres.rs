use crate::engine::dialect::{format_qualified_table, SqlDialect};
use crate::engine::value::SqlValue;

#[derive(Debug)]
pub struct PostgresDialect;

pub static POSTGRES_DIALECT: PostgresDialect = PostgresDialect;

impl SqlDialect for PostgresDialect {
    fn name(&self) -> &'static str {
        "PostgreSQL"
    }

    fn quote_identifier(&self, name: &str) -> String {
        format!("\"{}\"", name.replace('"', "\"\""))
    }

    fn to_literal(&self, value: &SqlValue) -> String {
        match value {
            SqlValue::Null => "NULL".to_string(),
            SqlValue::Bool(v) => {
                if *v {
                    "TRUE".to_string()
                } else {
                    "FALSE".to_string()
                }
            }
            SqlValue::Int(v) => v.to_string(),
            SqlValue::Float(v) => {
                if v.is_nan() {
                    "'NaN'::float8".to_string()
                } else if v.is_infinite() {
                    if v.is_sign_positive() {
                        "'Infinity'::float8".to_string()
                    } else {
                        "'-Infinity'::float8".to_string()
                    }
                } else {
                    v.to_string()
                }
            }
            SqlValue::Decimal(v) => v.clone(),
            SqlValue::String(v) => escape_single_quotes(v),
            SqlValue::Bytes(bytes) => format!("'\\\\x{}'::bytea", hex::encode(bytes)),
            SqlValue::Date { y, m, d } => format!("DATE '{:04}-{:02}-{:02}'", y, m, d),
            SqlValue::Time { neg, h, m, s, us } => {
                let sign = if *neg { "-" } else { "" };
                if *us == 0 {
                    format!("TIME '{}{:02}:{:02}:{:02}'", sign, h, m, s)
                } else {
                    format!("TIME '{}{:02}:{:02}:{:02}.{:06}'", sign, h, m, s, us)
                }
            }
            SqlValue::Timestamp {
                y,
                m,
                d,
                hh,
                mm,
                ss,
                us,
            } => {
                if *us == 0 {
                    format!(
                        "TIMESTAMP '{:04}-{:02}-{:02} {:02}:{:02}:{:02}'",
                        y, m, d, hh, mm, ss
                    )
                } else {
                    format!(
                        "TIMESTAMP '{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}'",
                        y, m, d, hh, mm, ss, us
                    )
                }
            }
        }
    }

    fn insert_values_sql(&self, table: &str, columns: &[String], rows: &[Vec<SqlValue>]) -> String {
        let mut sql = String::new();
        sql.push_str("INSERT INTO ");
        sql.push_str(&format_qualified_table(self, table));
        sql.push_str(" (");
        for (idx, col) in columns.iter().enumerate() {
            if idx > 0 {
                sql.push_str(", ");
            }
            sql.push_str(&self.quote_identifier(col));
        }
        sql.push_str(") VALUES ");

        for (row_idx, row) in rows.iter().enumerate() {
            if row_idx > 0 {
                sql.push_str(", ");
            }
            sql.push('(');
            for (col_idx, value) in row.iter().enumerate() {
                if col_idx > 0 {
                    sql.push_str(", ");
                }
                sql.push_str(&self.to_literal(value));
            }
            sql.push(')');
        }
        sql.push(';');
        sql
    }
}

fn escape_single_quotes(value: &str) -> String {
    let mut result = String::with_capacity(value.len() + 8);
    result.push('\'');
    for ch in value.chars() {
        if ch == '\'' {
            result.push('\'');
        }
        result.push(ch);
    }
    result.push('\'');
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quotes_identifiers() {
        assert_eq!(POSTGRES_DIALECT.quote_identifier("users"), "\"users\"");
        assert_eq!(
            POSTGRES_DIALECT.quote_identifier("user\"table"),
            "\"user\"\"table\""
        );
    }
}
