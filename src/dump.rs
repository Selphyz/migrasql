use crate::engine::dialect::SqlDialect;
use crate::engine::value::SqlValue;
use crate::engine::{DbEngine, DbSession};
use anyhow::{Context, Result};
use flate2::write::GzEncoder;
use flate2::Compression;
use futures::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use std::fs::File;
use std::io::{BufWriter, Write};

pub struct DumpOptions {
    pub tables: Vec<String>,
    pub exclude: Vec<String>,
    pub schema_only: bool,
    pub data_only: bool,
    pub batch_rows: usize,
    pub consistent_snapshot: bool,
    pub gzip: bool,
}

pub async fn dump(
    engine: &dyn DbEngine,
    source_url: &str,
    output_path: &str,
    opts: DumpOptions,
) -> Result<()> {
    println!("Starting database dump...");

    // Connect to source
    let mut session = engine
        .connect(source_url)
        .await
        .context("Failed to connect to source database")?;

    let dialect = session.dialect();

    // Start consistent snapshot if requested
    if opts.consistent_snapshot {
        println!("Starting consistent snapshot...");
        session.start_consistent_snapshot().await?;
    }

    // Create output writer
    let mut writer: Box<dyn Write> = if opts.gzip {
        println!("Output will be gzip compressed");
        Box::new(GzEncoder::new(
            BufWriter::new(File::create(output_path)?),
            Compression::default(),
        ))
    } else {
        Box::new(BufWriter::new(File::create(output_path)?))
    };

    // Write header
    write_dump_header(&mut writer, dialect)?;

    // Get list of tables
    let tables = session.list_tables(&opts.tables, &opts.exclude).await?;
    println!("Found {} table(s) to dump", tables.len());

    // Dump each table
    for (idx, table) in tables.iter().enumerate() {
        println!(
            "\n[{}/{}] Dumping table '{}'...",
            idx + 1,
            tables.len(),
            table
        );

        dump_table(&mut *session, &mut writer, table, dialect, &opts)
            .await
            .with_context(|| format!("Failed to dump table '{}'", table))?;
    }

    // Write footer
    write_dump_footer(&mut writer, dialect)?;

    // Commit transaction if opened
    session.commit().await?;

    // Flush and close
    writer.flush()?;

    println!("\nDump completed successfully!");
    println!("Output: {}", output_path);

    Ok(())
}

async fn dump_table(
    session: &mut dyn DbSession,
    writer: &mut Box<dyn Write>,
    table: &str,
    dialect: &dyn SqlDialect,
    opts: &DumpOptions,
) -> Result<()> {
    // Dump schema
    if !opts.data_only {
        let create_stmt = session.show_create_table(table).await?;
        writeln!(writer)?;
        writeln!(writer, "-- Table structure for {}", table)?;
        writeln!(writer, "DROP TABLE IF EXISTS `{}`;", table)?;
        let normalized_create = create_stmt.trim_end_matches(';');
        writeln!(writer, "{};", normalized_create)?;
        writer.flush()?;
    }

    // Dump data
    if !opts.schema_only {
        writeln!(writer)?;
        writeln!(writer, "-- Data for table `{}`", table)?;

        // Get approximate row count for progress
        let approx_count = session.approximate_row_count(table).await?;

        // Create progress bar
        let pb = if approx_count > 0 {
            let pb = ProgressBar::new(approx_count);
            pb.set_style(
                ProgressStyle::default_bar()
                    .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} rows ({per_sec})")
                    .unwrap()
                    .progress_chars("#>-"),
            );
            Some(pb)
        } else {
            None
        };

        // Stream rows
        let (columns, mut row_stream) = session.stream_rows(table).await?;

        let mut batch: Vec<Vec<SqlValue>> = Vec::with_capacity(opts.batch_rows);
        let mut total_rows = 0u64;

        while let Some(row_result) = row_stream.next().await {
            let row = row_result?;
            batch.push(row);

            // Write batch when full
            if batch.len() >= opts.batch_rows {
                write_insert_batch(writer, table, dialect, &columns, &batch)?;
                total_rows += batch.len() as u64;

                if let Some(pb) = &pb {
                    pb.set_position(total_rows);
                }

                batch.clear();
            }
        }

        // Write remaining rows
        if !batch.is_empty() {
            write_insert_batch(writer, table, dialect, &columns, &batch)?;
            total_rows += batch.len() as u64;
        }

        if let Some(pb) = &pb {
            pb.finish_with_message(format!("Dumped {} rows", total_rows));
        } else {
            println!("  Dumped {} rows", total_rows);
        }

        writer.flush()?;
    }

    Ok(())
}

fn write_insert_batch(
    writer: &mut Box<dyn Write>,
    table: &str,
    dialect: &dyn SqlDialect,
    columns: &[String],
    rows: &[Vec<SqlValue>],
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }

    let sql = dialect.insert_values_sql(table, columns, rows);
    writeln!(writer, "{}", sql)?;

    Ok(())
}

fn write_dump_header(writer: &mut Box<dyn Write>, dialect: &dyn SqlDialect) -> Result<()> {
    writeln!(writer, "-- {} Database Dump", dialect.name())?;
    writeln!(writer, "-- Generated by migrasquiel")?;
    writeln!(writer, "-- Date: {}", chrono::Utc::now().to_rfc3339())?;
    writeln!(writer)?;

    match dialect.name() {
        "MySQL" => {
            writeln!(
                writer,
                "/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;"
            )?;
            writeln!(
                writer,
                "/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;"
            )?;
            writeln!(
                writer,
                "/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;"
            )?;
            writeln!(writer, "/*!40101 SET NAMES utf8mb4 */;")?;
            writeln!(
                writer,
                "/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;"
            )?;
            writeln!(writer, "/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;")?;
            writeln!(
                writer,
                "/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;"
            )?;
        }
        "PostgreSQL" => {
            writeln!(writer, "SET client_encoding = 'UTF8';")?;
            writeln!(writer, "SET standard_conforming_strings = on;")?;
        }
        _ => {}
    }

    writeln!(writer)?;

    Ok(())
}

fn write_dump_footer(writer: &mut Box<dyn Write>, dialect: &dyn SqlDialect) -> Result<()> {
    writeln!(writer)?;
    match dialect.name() {
        "MySQL" => {
            writeln!(writer, "/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;")?;
            writeln!(
                writer,
                "/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;"
            )?;
            writeln!(writer, "/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;")?;
            writeln!(
                writer,
                "/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;"
            )?;
            writeln!(
                writer,
                "/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;"
            )?;
            writeln!(
                writer,
                "/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;"
            )?;
        }
        "PostgreSQL" => {
            writeln!(writer, "RESET ALL;")?;
        }
        _ => {}
    }

    Ok(())
}
