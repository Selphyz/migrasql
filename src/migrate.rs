use crate::engine::dialect::SqlDialect;
use crate::engine::value::SqlValue;
use crate::engine::{DbEngine, DbSession};
use anyhow::{bail, Context, Result};
use futures::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use tokio::time::{sleep, Duration};

const MAX_ROW_INSERT_RETRIES: u32 = 2;

pub struct MigrateOptions {
    pub tables: Vec<String>,
    pub exclude: Vec<String>,
    pub schema_only: bool,
    pub data_only: bool,
    pub batch_rows: usize,
    pub consistent_snapshot: bool,
    pub disable_fk_checks: bool,
    pub skip_errors: bool,
}

pub async fn migrate(
    engine: &dyn DbEngine,
    source_url: &str,
    destination_url: &str,
    opts: MigrateOptions,
) -> Result<()> {
    println!("Starting database migration...");

    // Connect to source and destination
    println!("Connecting to source database...");
    let mut source = engine
        .connect(source_url)
        .await
        .context("Failed to connect to source database")?;

    println!("Connecting to destination database...");
    let mut dest = engine
        .connect(destination_url)
        .await
        .context("Failed to connect to destination database")?;

    let src_dialect = source.dialect();
    let dest_dialect = dest.dialect();

    if src_dialect.name() != dest_dialect.name() {
        bail!("Cross-engine migrations are not supported in this release");
    }

    // Start consistent snapshot on source if requested
    if opts.consistent_snapshot {
        println!("Starting consistent snapshot on source...");
        source.start_consistent_snapshot().await?;
    }

    // Disable constraints on destination if requested
    if opts.disable_fk_checks {
        println!("Disabling foreign key checks on destination...");
        dest.disable_constraints().await?;
    }

    // Get list of tables from source
    let tables = source.list_tables(&opts.tables, &opts.exclude).await?;
    println!("Found {} table(s) to migrate", tables.len());

    // Migrate each table
    for (idx, table) in tables.iter().enumerate() {
        println!(
            "\n[{}/{}] Migrating table '{}'...",
            idx + 1,
            tables.len(),
            table
        );

        migrate_table(&mut *source, &mut *dest, table, dest_dialect, &opts)
            .await
            .with_context(|| format!("Failed to migrate table '{}'", table))?;
    }

    // Re-enable constraints on destination
    if opts.disable_fk_checks {
        println!("\nRe-enabling foreign key checks on destination...");
        dest.enable_constraints().await?;
    }

    // Commit both sessions
    println!("Committing transactions...");
    source.commit().await?;
    dest.commit().await?;

    println!("\nMigration completed successfully!");

    Ok(())
}

async fn migrate_table(
    source: &mut dyn DbSession,
    dest: &mut dyn DbSession,
    table: &str,
    _dest_dialect: &dyn SqlDialect,
    opts: &MigrateOptions,
) -> Result<()> {
    // Migrate schema
    if !opts.data_only {
        println!("  Creating table schema...");
        let create_stmt = source.show_create_table(table).await?;

        // Drop table first if it exists
        let drop_stmt = format!("DROP TABLE IF EXISTS `{}`;", table);
        dest.execute(&drop_stmt).await?;

        // Create table
        let normalized_create = create_stmt.trim_end_matches(';');
        dest.execute(normalized_create).await?;
    }

    // Migrate data
    if !opts.schema_only {
        println!("  Migrating data...");

        // Get approximate row count for progress
        let approx_count = source.approximate_row_count(table).await?;

        // Create progress bar
        let pb = if approx_count > 0 {
            let pb = ProgressBar::new(approx_count);
            pb.set_style(
                ProgressStyle::default_bar()
                    .template("  {spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} rows ({per_sec})")
                    .unwrap()
                    .progress_chars("#>-"),
            );
            Some(pb)
        } else {
            None
        };

        // Stream rows from source
        let (columns, mut row_stream) = source.stream_rows(table).await?;

        let mut batch: Vec<(u64, Vec<SqlValue>)> = Vec::with_capacity(opts.batch_rows);
        let mut total_rows = 0u64;
        let mut failed_rows: Vec<(u64, String)> = Vec::new();
        let mut source_row_number = 0u64;

        while let Some(row_result) = row_stream.next().await {
            let row = row_result?;
            source_row_number += 1;
            batch.push((source_row_number, row));

            // Insert batch when full
            if batch.len() >= opts.batch_rows {
                let inserted = insert_batch_with_fallback(
                    dest,
                    table,
                    &columns,
                    &batch,
                    opts,
                    &mut failed_rows,
                )
                .await?;
                total_rows += inserted;

                if let Some(pb) = &pb {
                    pb.set_position(total_rows);
                }

                batch.clear();
            }
        }

        // Insert remaining rows
        if !batch.is_empty() {
            let inserted =
                insert_batch_with_fallback(dest, table, &columns, &batch, opts, &mut failed_rows)
                    .await?;
            total_rows += inserted;
        }

        if let Some(pb) = &pb {
            pb.finish_with_message(format!("Migrated {} rows", total_rows));
        } else {
            println!("  Migrated {} rows", total_rows);
        }

        if !failed_rows.is_empty() {
            println!("  Failed to insert {} row(s)", failed_rows.len());
            for (row_number, err) in failed_rows.iter().take(10) {
                println!("    Source row {}: {}", row_number, err);
            }
            if failed_rows.len() > 10 {
                println!("    ... and {} more errors", failed_rows.len() - 10);
            }
        }
    }

    Ok(())
}

async fn insert_batch_with_fallback(
    dest: &mut dyn DbSession,
    table: &str,
    columns: &[String],
    batch: &[(u64, Vec<SqlValue>)],
    opts: &MigrateOptions,
    failed_rows: &mut Vec<(u64, String)>,
) -> Result<u64> {
    let rows: Vec<Vec<SqlValue>> = batch.iter().map(|(_, row)| row.clone()).collect();

    match dest.insert_batch(table, columns, &rows).await {
        Ok(()) => Ok(batch.len() as u64),
        Err(_batch_error) => {
            let mut inserted = 0u64;

            for (row_number, row) in batch {
                match insert_single_row_with_retry(dest, table, columns, row, *row_number).await {
                    Ok(()) => inserted += 1,
                    Err(row_error) => {
                        let record = summarize_record(row);
                        let error_message =
                            format!("insert failed ({}) | record: {}", row_error, record);

                        if opts.skip_errors {
                            failed_rows.push((*row_number, error_message));
                            continue;
                        }

                        bail!(
                            "insert error on source row {} in table '{}': {}",
                            row_number,
                            table,
                            error_message
                        );
                    }
                }
            }

            Ok(inserted)
        }
    }
}

async fn insert_single_row_with_retry(
    dest: &mut dyn DbSession,
    table: &str,
    columns: &[String],
    row: &[SqlValue],
    row_number: u64,
) -> Result<()> {
    let single_row = vec![row.to_vec()];
    let retry_delay = row_insert_retry_delay();
    let mut retries = 0u32;

    loop {
        match dest.insert_batch(table, columns, &single_row).await {
            Ok(()) => return Ok(()),
            Err(err) => {
                if retries >= MAX_ROW_INSERT_RETRIES {
                    return Err(err);
                }

                retries += 1;
                println!(
                    "Retrying insert for table '{}' source row {} in {}s ({}/{})...",
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
    let full = format!("{:?}", row);

    match full.char_indices().nth(MAX_LEN) {
        Some((byte_idx, _)) => format!("{}...", &full[..byte_idx]),
        None => full,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::RowStream;
    use crate::util::dialects::mysql::MYSQL_DIALECT;
    use anyhow::{anyhow, Result};
    use async_trait::async_trait;
    use futures::stream;
    use std::collections::VecDeque;

    struct MockSession {
        insert_results: VecDeque<Result<()>>,
        insert_calls: usize,
    }

    impl MockSession {
        fn new(insert_results: Vec<Result<()>>) -> Self {
            Self {
                insert_results: insert_results.into(),
                insert_calls: 0,
            }
        }
    }

    #[async_trait]
    impl DbSession for MockSession {
        fn dialect(&self) -> &'static dyn SqlDialect {
            &MYSQL_DIALECT
        }

        async fn start_consistent_snapshot(&mut self) -> Result<()> {
            Ok(())
        }

        async fn list_tables(
            &mut self,
            _include: &[String],
            _exclude: &[String],
        ) -> Result<Vec<String>> {
            Ok(Vec::new())
        }

        async fn show_create_table(&mut self, _table: &str) -> Result<String> {
            Ok(String::new())
        }

        async fn stream_rows(&mut self, _table: &str) -> Result<(Vec<String>, RowStream)> {
            Ok((Vec::new(), Box::pin(stream::empty())))
        }

        async fn approximate_row_count(&mut self, _table: &str) -> Result<u64> {
            Ok(0)
        }

        async fn insert_batch(
            &mut self,
            _table: &str,
            _column_names: &[String],
            _rows: &[Vec<SqlValue>],
        ) -> Result<()> {
            self.insert_calls += 1;
            self.insert_results
                .pop_front()
                .unwrap_or_else(|| Err(anyhow!("unexpected insert call")))
        }

        async fn disable_constraints(&mut self) -> Result<()> {
            Ok(())
        }

        async fn enable_constraints(&mut self) -> Result<()> {
            Ok(())
        }

        async fn execute(&mut self, _sql: &str) -> Result<()> {
            Ok(())
        }

        async fn commit(&mut self) -> Result<()> {
            Ok(())
        }

        async fn create_table_from_columns(
            &mut self,
            _table: &str,
            _column_names: &[String],
            _column_types: &[SqlValue],
        ) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn row_insert_is_retried_after_single_failure() {
        let mut dest = MockSession::new(vec![
            Err(anyhow!("batch failed")),
            Err(anyhow!("transient row failure")),
            Ok(()),
        ]);
        let columns = vec!["id".to_string()];
        let batch = vec![(1u64, vec![SqlValue::Int(1)])];
        let opts = MigrateOptions {
            tables: Vec::new(),
            exclude: Vec::new(),
            schema_only: false,
            data_only: false,
            batch_rows: 1000,
            consistent_snapshot: false,
            disable_fk_checks: false,
            skip_errors: false,
        };
        let mut failed_rows = Vec::new();

        let inserted = insert_batch_with_fallback(
            &mut dest,
            "users",
            &columns,
            &batch,
            &opts,
            &mut failed_rows,
        )
        .await
        .expect("row insert should succeed after retry");

        assert_eq!(inserted, 1);
        assert_eq!(dest.insert_calls, 3);
        assert!(failed_rows.is_empty());
    }
}
