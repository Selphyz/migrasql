use crate::engine::value::SqlValue;
use crate::engine::{DbEngine, DbSession};
use anyhow::{bail, Context, Result};
use chrono::Datelike;
use csv::ReaderBuilder;
use indicatif::{ProgressBar, ProgressStyle};
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::time::sleep;

const MAX_ROW_INSERT_RETRIES: u32 = 2;

pub struct ImportOptions {
    pub input: String,
    pub table: String,
    pub batch_rows: usize,
    pub disable_fk_checks: bool,
    pub skip_errors: bool,
    pub column_mapping: Option<HashMap<String, String>>,
    pub details: bool,
    pub start_line: Option<usize>,
}

#[derive(Clone, Debug)]
pub struct ImportProgressTracker {
    inner: Arc<Mutex<Option<LastSuccessfulInsert>>>,
}

#[derive(Clone, Debug)]
pub struct LastSuccessfulInsert {
    pub line_number: usize,
    pub row_preview: String,
}

impl ImportProgressTracker {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(None)),
        }
    }

    pub fn record(&self, line_number: usize, row: &[SqlValue]) {
        if let Ok(mut guard) = self.inner.lock() {
            *guard = Some(LastSuccessfulInsert {
                line_number,
                row_preview: summarize_record(row),
            });
        }
    }

    pub fn snapshot(&self) -> Option<LastSuccessfulInsert> {
        self.inner.lock().ok().and_then(|guard| guard.clone())
    }

    pub fn format_last_success_for_output(&self) -> String {
        match self.snapshot() {
            Some(info) => format!(
                "Last successfully inserted line: {} | row: {}",
                info.line_number, info.row_preview
            ),
            None => "No rows were successfully inserted before the failure.".to_string(),
        }
    }

    pub fn print_last_success_for_failure(&self) {
        eprintln!("{}", self.format_last_success_for_output());
    }
}

pub async fn import(
    engine: &dyn DbEngine,
    url: &str,
    options: ImportOptions,
    tracker: Option<ImportProgressTracker>,
) -> Result<()> {
    let progress_tracker = tracker.unwrap_or_else(ImportProgressTracker::new);
    let start_line = validate_start_line(options.start_line)?;

    println!("Starting CSV import...");
    if options.details {
        println!(
            "Import settings: table='{}', batch_rows={}, skip_errors={}, disable_fk_checks={}, start_line={}",
            options.table,
            options.batch_rows,
            options.skip_errors,
            options.disable_fk_checks,
            start_line
        );
    }

    // Connect to database
    println!("Connecting to database...");
    let mut session = engine
        .connect(url)
        .await
        .context("Failed to connect to database")?;

    // Check if input file exists
    if !Path::new(&options.input).exists() {
        bail!("Input file not found: {}", options.input);
    }

    // Read CSV header
    println!("Reading CSV header...");
    let file = File::open(&options.input).context("Failed to open input file")?;
    let mut csv_reader = ReaderBuilder::new().from_reader(file);

    let headers = csv_reader.headers().context("Failed to read CSV headers")?;
    let csv_columns: Vec<String> = headers.iter().map(|h| h.to_string()).collect();

    if csv_columns.is_empty() {
        bail!("CSV file has no columns");
    }

    // Get column mapping
    let column_mapping = options
        .column_mapping
        .clone()
        .unwrap_or_else(|| csv_columns.iter().map(|c| (c.clone(), c.clone())).collect());

    let db_columns: Vec<String> = csv_columns
        .iter()
        .map(|csv_col| {
            column_mapping
                .get(csv_col)
                .cloned()
                .unwrap_or_else(|| csv_col.clone())
        })
        .collect();

    // Infer column types from first 100 rows
    println!("Inferring column types...");
    let file = File::open(&options.input).context("Failed to open input file")?;
    let mut csv_reader = ReaderBuilder::new().from_reader(file);

    let inferred_types = infer_column_types(&mut csv_reader, &csv_columns, 100)?;

    // Check if table exists
    let tables = session
        .list_tables(&[options.table.clone()], &[])
        .await
        .context("Failed to list tables")?;

    let table_exists = !tables.is_empty();

    // Create table if it doesn't exist
    if !table_exists {
        println!("Creating table '{}'...", options.table);
        session
            .create_table_from_columns(&options.table, &db_columns, &inferred_types)
            .await
            .context("Failed to create table")?;
    } else {
        println!(
            "Table '{}' already exists, inserting data...",
            options.table
        );
    }

    // Disable constraints if requested
    if options.disable_fk_checks {
        println!("Disabling foreign key checks...");
        session
            .disable_constraints()
            .await
            .context("Failed to disable constraints")?;
    }

    // Process and insert rows
    println!("Importing data...");
    let file = File::open(&options.input).context("Failed to open input file")?;
    let mut csv_reader = ReaderBuilder::new().from_reader(file);

    // Skip header
    let _headers = csv_reader.headers().context("Failed to read CSV headers")?;

    let mut batch: Vec<(usize, Vec<SqlValue>)> = Vec::new();
    let mut error_rows: Vec<(usize, String)> = Vec::new();
    let mut row_number = 1; // Header is row 1
    let mut total_inserted = 0u64;
    let mut last_progress_log_at = 1usize;
    let import_started = Instant::now();

    let progress = ProgressBar::new_spinner();
    progress.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} {msg}")
            .unwrap(),
    );
    progress.set_message("Processing rows...");

    for result in csv_reader.deserialize::<Vec<String>>() {
        row_number += 1;

        if should_skip_line(row_number, start_line) {
            continue;
        }

        match result {
            Ok(row) => match parse_row(&row, &csv_columns, &db_columns, &inferred_types) {
                Ok(values) => {
                    batch.push((row_number, values));

                    if batch.len() >= options.batch_rows {
                        if options.details {
                            println!(
                                "Batch threshold reached at CSV line {}. Inserting {} rows into '{}'...",
                                row_number,
                                batch.len(),
                                options.table
                            );
                        }
                        total_inserted += insert_batch_with_row_tracking(
                            &mut *session,
                            &options.table,
                            &db_columns,
                            &batch,
                            options.skip_errors,
                            options.details,
                            &mut error_rows,
                            &progress_tracker,
                        )
                        .await
                        .context("Failed to insert batch")
                        .map_err(|e| {
                            progress_tracker.print_last_success_for_failure();
                            e
                        })?;
                        if options.details {
                            println!(
                                "Batch insert completed. Total inserted so far: {} rows (elapsed: {:.2?})",
                                total_inserted,
                                import_started.elapsed()
                            );
                        }
                        progress.set_message(format!("Inserted {} rows...", total_inserted));
                        batch.clear();
                    }
                }
                Err(e) => {
                    error_rows.push((row_number, e.to_string()));
                    if !options.skip_errors {
                        progress_tracker.print_last_success_for_failure();
                        bail!("Error at row {}: {}", row_number, e);
                    }
                }
            },
            Err(e) => {
                error_rows.push((row_number, format!("CSV parse error: {}", e)));
                if !options.skip_errors {
                    progress_tracker.print_last_success_for_failure();
                    bail!("CSV parse error at row {}: {}", row_number, e);
                }
            }
        }

        if options.details && row_number.saturating_sub(last_progress_log_at) >= 10_000 {
            println!(
                "Progress checkpoint: line={}, queued_in_batch={}, inserted={}, errors={}, elapsed={:.2?}",
                row_number,
                batch.len(),
                total_inserted,
                error_rows.len(),
                import_started.elapsed()
            );
            last_progress_log_at = row_number;
        }
    }

    if is_start_line_beyond_eof(start_line, row_number) {
        progress_tracker.print_last_success_for_failure();
        bail!(
            "Requested start line {} exceeds CSV length {}",
            start_line,
            row_number
        );
    }

    // Insert remaining batch
    if !batch.is_empty() {
        if options.details {
            println!(
                "Inserting final partial batch of {} rows (current line: {})...",
                batch.len(),
                row_number
            );
        }
        total_inserted += insert_batch_with_row_tracking(
            &mut *session,
            &options.table,
            &db_columns,
            &batch,
            options.skip_errors,
            options.details,
            &mut error_rows,
            &progress_tracker,
        )
        .await
        .context("Failed to insert final batch")
        .map_err(|e| {
            progress_tracker.print_last_success_for_failure();
            e
        })?;
        if options.details {
            println!(
                "Final batch completed. Total inserted: {} rows (elapsed: {:.2?})",
                total_inserted,
                import_started.elapsed()
            );
        }
    }

    // Re-enable constraints
    if options.disable_fk_checks {
        println!("Re-enabling foreign key checks...");
        session
            .enable_constraints()
            .await
            .context("Failed to enable constraints")?;
    }

    // Commit transaction
    println!("Committing transaction...");
    session
        .commit()
        .await
        .context("Failed to commit transaction")?;

    progress.finish_and_clear();

    // Print summary
    println!("\n═══════════════════════════════════════");
    println!("CSV Import Summary");
    println!("═══════════════════════════════════════");
    println!("Source:        {}", options.input);
    println!("Table:         {}", options.table);
    println!("Total rows:    {} (including header)", row_number);
    println!("Inserted:      {} rows ✓", total_inserted);
    println!("Failed:        {} rows ✗", error_rows.len());
    println!("═══════════════════════════════════════");

    // Show failed rows
    if !error_rows.is_empty() {
        println!("\nFailed rows:");
        for (line, err) in error_rows.iter().take(10) {
            println!("  Line {}: {}", line, err);
        }
        if error_rows.len() > 10 {
            println!("  ... and {} more errors", error_rows.len() - 10);
        }
    }

    Ok(())
}

async fn insert_batch_with_row_tracking(
    session: &mut dyn DbSession,
    table: &str,
    columns: &[String],
    batch: &[(usize, Vec<SqlValue>)],
    skip_errors: bool,
    details: bool,
    error_rows: &mut Vec<(usize, String)>,
    progress_tracker: &ImportProgressTracker,
) -> Result<u64> {
    let batch_started = Instant::now();
    let first_row = batch.first().map(|(n, _)| *n).unwrap_or(0);
    let last_row = batch.last().map(|(n, _)| *n).unwrap_or(0);
    if details {
        println!(
            "Attempting batch insert: table='{}', rows={}, line_range={}..{}",
            table,
            batch.len(),
            first_row,
            last_row
        );
    }

    let rows: Vec<Vec<SqlValue>> = batch.iter().map(|(_, row)| row.clone()).collect();
    match session.insert_batch(table, columns, &rows).await {
        Ok(()) => {
            if let Some((line_number, row)) = batch.last() {
                progress_tracker.record(*line_number, row);
            }
            if details {
                println!(
                    "Batch insert succeeded for line_range={}..{} in {:.2?}",
                    first_row,
                    last_row,
                    batch_started.elapsed()
                );
            }
            Ok(batch.len() as u64)
        }
        Err(err) => {
            if is_fatal_connection_error(&err) {
                bail!(
                    "Fatal database connection error while inserting line_range={}..{}: {}",
                    first_row,
                    last_row,
                    err
                );
            }

            if details {
                println!(
                    "Batch insert failed for line_range={}..{} after {:.2?}: {}. Falling back to adaptive chunked mode.",
                    first_row,
                    last_row,
                    batch_started.elapsed(),
                    err
                );
            }
            let mut inserted = 0u64;
            let mut pending: Vec<Vec<(usize, Vec<SqlValue>)>> = vec![batch.to_vec()];

            while let Some(chunk) = pending.pop() {
                let chunk_first = chunk.first().map(|(n, _)| *n).unwrap_or(0);
                let chunk_last = chunk.last().map(|(n, _)| *n).unwrap_or(0);
                let chunk_rows: Vec<Vec<SqlValue>> =
                    chunk.iter().map(|(_, row)| row.clone()).collect();

                match session.insert_batch(table, columns, &chunk_rows).await {
                    Ok(()) => {
                        if let Some((line_number, row)) = chunk.last() {
                            progress_tracker.record(*line_number, row);
                        }
                        inserted += chunk.len() as u64;
                    }
                    Err(err) => {
                        if is_fatal_connection_error(&err) {
                            bail!(
                                "Fatal database connection error while inserting line_range={}..{}: {}",
                                chunk_first,
                                chunk_last,
                                err
                            );
                        }

                        if chunk.len() == 1 {
                            let (row_number, row) = &chunk[0];
                            match insert_single_row_with_retry(
                                session,
                                table,
                                columns,
                                row,
                                *row_number,
                            )
                            .await
                            {
                                Ok(()) => {
                                    progress_tracker.record(*row_number, row);
                                    inserted += 1;
                                }
                                Err(retry_error) => {
                                    let detail_msg = format!(
                                        "Insert error: {} | record: {}",
                                        retry_error,
                                        summarize_record(row)
                                    );

                                    if skip_errors {
                                        error_rows.push((*row_number, detail_msg));
                                        continue;
                                    }

                                    bail!("Error at row {}: {}", row_number, detail_msg);
                                }
                            }

                            continue;
                        }

                        let split_at = chunk.len() / 2;
                        let left = chunk[..split_at].to_vec();
                        let right = chunk[split_at..].to_vec();

                        if details {
                            println!(
                                "Batch split due to insert error at line_range={}..{} (rows={}): {}. Retrying chunks of {} and {} rows.",
                                chunk_first,
                                chunk_last,
                                chunk.len(),
                                err,
                                left.len(),
                                right.len()
                            );
                        }

                        // Stack is LIFO; push right first so left is processed first.
                        pending.push(right);
                        pending.push(left);
                    }
                }
            }

            if details {
                println!(
                    "Adaptive fallback completed for line_range={}..{}: inserted={}, failed={} (duration: {:.2?})",
                    first_row,
                    last_row,
                    inserted,
                    batch.len() as u64 - inserted,
                    batch_started.elapsed()
                );
            }
            Ok(inserted)
        }
    }
}

async fn insert_single_row_with_retry(
    session: &mut dyn DbSession,
    table: &str,
    columns: &[String],
    row: &[SqlValue],
    row_number: usize,
) -> Result<()> {
    let single_row = vec![row.to_vec()];
    let retry_delay = row_insert_retry_delay();
    let mut retries = 0u32;

    loop {
        match session.insert_batch(table, columns, &single_row).await {
            Ok(()) => return Ok(()),
            Err(err) => {
                if retries >= MAX_ROW_INSERT_RETRIES {
                    return Err(err);
                }

                retries += 1;
                println!(
                    "Retrying insert for table '{}' line {} in {}s ({}/{})...",
                    table,
                    row_number,
                    retry_delay.as_secs(),
                    retries,
                    MAX_ROW_INSERT_RETRIES
                );
                sleep(retry_delay).await;
            }
        }
    }
}

fn row_insert_retry_delay() -> Duration {
    #[cfg(test)]
    {
        Duration::ZERO
    }
    #[cfg(not(test))]
    {
        Duration::from_secs(60)
    }
}

fn summarize_record(row: &[SqlValue]) -> String {
    const MAX_LEN: usize = 200;
    let value = format!("{:?}", row);

    match value.char_indices().nth(MAX_LEN) {
        Some((byte_idx, _)) => format!("{}...", &value[..byte_idx]),
        None => value,
    }
}

fn is_fatal_connection_error(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        let msg = cause.to_string().to_lowercase();
        msg.contains("unexpected-eof")
            || msg.contains("peer closed connection")
            || msg.contains("broken pipe")
            || msg.contains("connection reset")
            || msg.contains("connection refused")
            || msg.contains("server has gone away")
            || msg.contains("lost connection")
            || msg.contains("connection closed")
            || msg.contains("not connected")
    })
}

/// Infer column types from CSV data
fn infer_column_types(
    csv_reader: &mut csv::Reader<File>,
    csv_columns: &[String],
    sample_size: usize,
) -> Result<Vec<SqlValue>> {
    let mut type_scores: Vec<HashMap<String, usize>> = vec![HashMap::new(); csv_columns.len()];

    let mut row_count = 0;
    for result in csv_reader.deserialize::<Vec<String>>() {
        if row_count >= sample_size {
            break;
        }

        match result {
            Ok(row) => {
                for (col_idx, value) in row.iter().enumerate() {
                    if col_idx >= csv_columns.len() {
                        break;
                    }

                    let detected_type = detect_value_type(value);
                    *type_scores[col_idx].entry(detected_type).or_insert(0) += 1;
                }
                row_count += 1;
            }
            Err(_) => {
                // Skip malformed rows during type inference
                continue;
            }
        }
    }

    // Determine final type for each column
    let mut inferred_types = Vec::new();
    for scores in type_scores {
        let final_type = if let Some((type_name, _)) = scores.iter().max_by_key(|(_, &count)| count)
        {
            match type_name.as_str() {
                "int" => SqlValue::Int(0),
                "float" => SqlValue::Float(0.0),
                "bool" => SqlValue::Bool(false),
                "timestamp" => SqlValue::Timestamp {
                    y: 2024,
                    m: 1,
                    d: 1,
                    hh: 0,
                    mm: 0,
                    ss: 0,
                    us: 0,
                },
                "date" => SqlValue::Date {
                    y: 2024,
                    m: 1,
                    d: 1,
                },
                _ => SqlValue::String(String::new()),
            }
        } else {
            SqlValue::String(String::new())
        };

        inferred_types.push(final_type);
    }

    Ok(inferred_types)
}

/// Detect the type of a value
fn detect_value_type(value: &str) -> String {
    use chrono::NaiveDate;

    let trimmed = value.trim();

    // Check for empty/null
    if trimmed.is_empty()
        || trimmed.eq_ignore_ascii_case("null")
        || trimmed.eq_ignore_ascii_case("none")
    {
        return "string".to_string();
    }

    // Check for boolean
    if trimmed.eq_ignore_ascii_case("true")
        || trimmed.eq_ignore_ascii_case("false")
        || trimmed.eq_ignore_ascii_case("yes")
        || trimmed.eq_ignore_ascii_case("no")
        || trimmed == "1"
        || trimmed == "0"
    {
        if trimmed == "1" || trimmed == "0" {
            // Could be int or bool, prefer int
            return "int".to_string();
        }
        return "bool".to_string();
    }

    // Check for timestamp (with time)
    if trimmed.matches(':').count() >= 2 && trimmed.contains('-') {
        return "timestamp".to_string();
    }

    // Check for date (without time)
    if let Ok(_) = NaiveDate::parse_from_str(trimmed, "%Y-%m-%d") {
        return "date".to_string();
    }

    // Check for float
    if let Ok(_) = trimmed.parse::<f64>() {
        if trimmed.contains('.') {
            return "float".to_string();
        }
    }

    // Check for integer
    if let Ok(_) = trimmed.parse::<i64>() {
        return "int".to_string();
    }

    // Default to string
    "string".to_string()
}

/// Parse a CSV row into SqlValues
fn parse_row(
    row: &[String],
    _csv_columns: &[String],
    db_columns: &[String],
    types: &[SqlValue],
) -> Result<Vec<SqlValue>> {
    use chrono::NaiveDate;

    let mut values = Vec::new();

    for (col_idx, db_col) in db_columns.iter().enumerate() {
        let value = if col_idx < row.len() {
            row[col_idx].trim()
        } else {
            ""
        };

        let sql_value = if value.is_empty()
            || value.eq_ignore_ascii_case("null")
            || value.eq_ignore_ascii_case("none")
        {
            SqlValue::Null
        } else {
            match &types[col_idx] {
                SqlValue::Int(_) => {
                    let int_val = value.parse::<i64>().context(format!(
                        "Failed to parse '{}' as integer for column '{}'",
                        value, db_col
                    ))?;
                    SqlValue::Int(int_val)
                }
                SqlValue::Float(_) => {
                    let float_val = value.parse::<f64>().context(format!(
                        "Failed to parse '{}' as float for column '{}'",
                        value, db_col
                    ))?;
                    SqlValue::Float(float_val)
                }
                SqlValue::Bool(_) => {
                    let bool_val = match value.to_lowercase().as_str() {
                        "true" | "yes" | "1" => true,
                        "false" | "no" | "0" => false,
                        _ => bail!(
                            "Failed to parse '{}' as boolean for column '{}'",
                            value,
                            db_col
                        ),
                    };
                    SqlValue::Bool(bool_val)
                }
                SqlValue::Date { .. } => {
                    let date = NaiveDate::parse_from_str(value, "%Y-%m-%d").context(format!(
                        "Failed to parse '{}' as date (YYYY-MM-DD) for column '{}'",
                        value, db_col
                    ))?;
                    SqlValue::Date {
                        y: date.year(),
                        m: date.month(),
                        d: date.day(),
                    }
                }
                SqlValue::Timestamp { .. } => {
                    let parts: Vec<&str> = value.split(' ').collect();
                    if parts.len() < 2 {
                        bail!(
                            "Failed to parse '{}' as timestamp for column '{}': invalid format",
                            value,
                            db_col
                        );
                    }

                    let date_parts: Vec<&str> = parts[0].split('-').collect();
                    let time_parts: Vec<&str> = parts[1].split(':').collect();

                    if date_parts.len() != 3 || time_parts.len() < 2 {
                        bail!(
                            "Failed to parse '{}' as timestamp for column '{}'",
                            value,
                            db_col
                        );
                    }

                    let y = date_parts[0]
                        .parse::<i32>()
                        .context("Invalid year in timestamp")?;
                    let m = date_parts[1]
                        .parse::<u32>()
                        .context("Invalid month in timestamp")?;
                    let d = date_parts[2]
                        .parse::<u32>()
                        .context("Invalid day in timestamp")?;
                    let hh = time_parts[0]
                        .parse::<u32>()
                        .context("Invalid hour in timestamp")?;
                    let mm = time_parts[1]
                        .parse::<u32>()
                        .context("Invalid minute in timestamp")?;
                    let ss = if time_parts.len() > 2 {
                        time_parts[2]
                            .parse::<u32>()
                            .context("Invalid second in timestamp")?
                    } else {
                        0
                    };

                    SqlValue::Timestamp {
                        y,
                        m,
                        d,
                        hh,
                        mm,
                        ss,
                        us: 0,
                    }
                }
                _ => SqlValue::String(value.to_string()),
            }
        };

        values.push(sql_value);
    }

    Ok(values)
}

/// Parse column mapping from string format: "col1:db_col1,col2:db_col2"
pub fn parse_column_mapping(mapping: &str) -> Result<HashMap<String, String>> {
    let mut result = HashMap::new();

    for pair in mapping.split(',') {
        let parts: Vec<&str> = pair.trim().split(':').collect();
        if parts.len() != 2 {
            bail!("Invalid column mapping format. Expected 'csv_col:db_col,csv_col2:db_col2'");
        }

        result.insert(parts[0].to_string(), parts[1].to_string());
    }

    Ok(result)
}

fn validate_start_line(start_line: Option<usize>) -> Result<usize> {
    let line = start_line.unwrap_or(2);
    if line < 2 {
        bail!("--start-line must be >= 2 (header is line 1)");
    }
    Ok(line)
}

fn should_skip_line(row_number: usize, start_line: usize) -> bool {
    row_number < start_line
}

fn is_start_line_beyond_eof(start_line: usize, last_csv_line: usize) -> bool {
    start_line > last_csv_line
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn start_line_validation_rejects_zero_or_header() {
        assert!(validate_start_line(Some(0)).is_err());
        assert!(validate_start_line(Some(1)).is_err());
    }

    #[test]
    fn start_line_validation_accepts_first_data_row() {
        assert_eq!(validate_start_line(None).unwrap(), 2);
        assert_eq!(validate_start_line(Some(2)).unwrap(), 2);
    }

    #[test]
    fn last_success_formatter_with_value() {
        let tracker = ImportProgressTracker::new();
        tracker.record(42, &[SqlValue::String("abc".to_string())]);
        let text = tracker.format_last_success_for_output();
        assert!(text.contains("42"));
        assert!(text.contains("abc"));
    }

    #[test]
    fn last_success_formatter_without_value() {
        let tracker = ImportProgressTracker::new();
        let text = tracker.format_last_success_for_output();
        assert!(text.contains("No rows were successfully inserted"));
    }

    #[test]
    fn tracker_advances_to_latest_successful_chunk() {
        let tracker = ImportProgressTracker::new();
        tracker.record(10, &[SqlValue::Int(1)]);
        tracker.record(15, &[SqlValue::Int(2)]);
        let snapshot = tracker.snapshot().expect("snapshot should exist");
        assert_eq!(snapshot.line_number, 15);
    }
}
