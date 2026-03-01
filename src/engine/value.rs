use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};

/// Neutral value representation used across database engines.
#[derive(Clone, Debug, PartialEq)]
pub enum SqlValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Decimal(String),
    String(String),
    Bytes(Vec<u8>),
    Date {
        y: i32,
        m: u32,
        d: u32,
    },
    Time {
        neg: bool,
        h: u32,
        m: u32,
        s: u32,
        us: u32,
    },
    Timestamp {
        y: i32,
        m: u32,
        d: u32,
        hh: u32,
        mm: u32,
        ss: u32,
        us: u32,
    },
}

impl SqlValue {
    /// Helper to construct a timestamp from chrono's NaiveDateTime components.
    pub fn from_datetime(dt: NaiveDateTime) -> Self {
        SqlValue::Timestamp {
            y: dt.date().year(),
            m: dt.date().month(),
            d: dt.date().day(),
            hh: dt.time().hour(),
            mm: dt.time().minute(),
            ss: dt.time().second(),
            us: dt.time().nanosecond() / 1_000,
        }
    }

    /// Helper to construct a date from chrono's NaiveDate.
    pub fn from_date(date: NaiveDate) -> Self {
        SqlValue::Date {
            y: date.year(),
            m: date.month(),
            d: date.day(),
        }
    }

    /// Helper to construct a time from chrono's NaiveTime.
    pub fn from_time(time: NaiveTime) -> Self {
        SqlValue::Time {
            neg: false,
            h: time.hour(),
            m: time.minute(),
            s: time.second(),
            us: time.nanosecond() / 1_000,
        }
    }
}

impl From<Option<bool>> for SqlValue {
    fn from(value: Option<bool>) -> Self {
        match value {
            Some(v) => SqlValue::Bool(v),
            None => SqlValue::Null,
        }
    }
}
