use crate::engine::dialect::{format_qualified_table, SqlDialect};
use crate::engine::value::SqlValue;

#[derive(Debug)]
pub struct MysqlDialect;

pub static MYSQL_DIALECT: MysqlDialect = MysqlDialect;

impl SqlDialect for MysqlDialect {
    fn name(&self) -> &'static str {
        "MySQL"
    }

    fn quote_identifier(&self, name: &str) -> String {
        format!("`{}`", name.replace('`', "``"))
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
            SqlValue::Float(v) => format_f64(*v),
            SqlValue::Decimal(v) => v.clone(),
            SqlValue::String(v) => escape_string(v),
            SqlValue::Bytes(bytes) => bytes_literal(bytes),
            SqlValue::Date { y, m, d } => format!("'{:04}-{:02}-{:02}'", y, m, d),
            SqlValue::Time { neg, h, m, s, us } => {
                let sign = if *neg { "-" } else { "" };
                if *us == 0 {
                    format!("'{}{:02}:{:02}:{:02}'", sign, h, m, s)
                } else {
                    format!("'{}{:02}:{:02}:{:02}.{:06}'", sign, h, m, s, us)
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
                    format!("'{:04}-{:02}-{:02} {:02}:{:02}:{:02}'", y, m, d, hh, mm, ss)
                } else {
                    format!(
                        "'{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}'",
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

fn bytes_literal(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        "''".to_string()
    } else if is_likely_text(bytes) {
        escape_bytes(bytes)
    } else {
        format!("0x{}", hex::encode(bytes))
    }
}

fn escape_string(value: &str) -> String {
    escape_bytes(value.as_bytes())
}

fn escape_bytes(bytes: &[u8]) -> String {
    let s = String::from_utf8_lossy(bytes);
    let mut result = String::with_capacity(s.len() + 16);
    result.push('\'');
    for ch in s.chars() {
        match ch {
            '\'' => result.push_str("''"),
            '\\' => result.push_str("\\\\"),
            '\0' => result.push_str("\\0"),
            '\n' => result.push_str("\\n"),
            '\r' => result.push_str("\\r"),
            '\t' => result.push_str("\\t"),
            _ => result.push(ch),
        }
    }
    result.push('\'');
    result
}

fn is_likely_text(bytes: &[u8]) -> bool {
    if std::str::from_utf8(bytes).is_err() {
        return false;
    }

    let printable = bytes
        .iter()
        .filter(|&&b| (32..127).contains(&b) || [b'\n', b'\r', b'\t'].contains(&b))
        .count();
    printable * 10 >= bytes.len() * 9
}

fn format_f64(value: f64) -> String {
    if value.is_nan() {
        "'NaN'".to_string()
    } else if value.is_infinite() {
        if value.is_sign_positive() {
            "'Infinity'".to_string()
        } else {
            "'-Infinity'".to_string()
        }
    } else {
        value.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quotes_identifiers() {
        assert_eq!(MYSQL_DIALECT.quote_identifier("users"), "`users`");
        assert_eq!(
            MYSQL_DIALECT.quote_identifier("user`table"),
            "`user``table`"
        );
    }

    #[test]
    fn escapes_strings() {
        assert_eq!(
            MYSQL_DIALECT.to_literal(&SqlValue::String("O'Reilly".into())),
            "'O''Reilly'"
        );
    }
}
