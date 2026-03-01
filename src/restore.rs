use crate::engine::DbEngine;
use anyhow::{Context, Result};
use flate2::read::GzDecoder;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};

pub struct RestoreOptions {
    pub disable_fk_checks: bool,
}

pub async fn restore(
    engine: &dyn DbEngine,
    destination_url: &str,
    input_path: &str,
    opts: RestoreOptions,
) -> Result<()> {
    println!("Starting database restore...");

    // Connect to destination
    let mut session = engine
        .connect(destination_url)
        .await
        .context("Failed to connect to destination database")?;

    // Disable constraints if requested
    if opts.disable_fk_checks {
        println!("Disabling foreign key checks...");
        session.disable_constraints().await?;
    }

    // Open input file
    let reader: Box<dyn Read> = if input_path.ends_with(".gz") {
        println!("Decompressing gzip input...");
        Box::new(GzDecoder::new(File::open(input_path)?))
    } else {
        Box::new(File::open(input_path)?)
    };

    let buf_reader = BufReader::new(reader);

    // Execute SQL statements line by line
    let mut statement_count = 0u64;
    let mut current_statement = String::new();
    let mut line_count = 0u64;

    println!("Executing SQL statements...");

    for line_result in buf_reader.lines() {
        let line = line_result?;
        line_count += 1;

        // Skip empty lines and comments (except special MySQL comments)
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        if trimmed.starts_with("--") && !trimmed.starts_with("-- ") {
            // Keep special comments like --
            continue;
        }

        // Skip pure comment lines (starting with --)
        if trimmed.starts_with("-- ") {
            continue;
        }

        // Add line to current statement
        current_statement.push_str(&line);
        current_statement.push(' ');

        // Check if statement is complete (ends with ;)
        if trimmed.ends_with(';') {
            // Execute the statement
            let stmt = current_statement.trim();
            if !stmt.is_empty() {
                session.execute(stmt).await.with_context(|| {
                    format!(
                        "Failed to execute statement at line {}: {}",
                        line_count, stmt
                    )
                })?;

                statement_count += 1;

                if statement_count % 100 == 0 {
                    print!("\rExecuted {} statements...", statement_count);
                    use std::io::Write;
                    std::io::stdout().flush()?;
                }
            }

            current_statement.clear();
        }
    }

    // Execute any remaining statement
    if !current_statement.trim().is_empty() {
        session.execute(current_statement.trim()).await?;
        statement_count += 1;
    }

    println!("\rExecuted {} statements total", statement_count);

    // Re-enable constraints
    if opts.disable_fk_checks {
        println!("Re-enabling foreign key checks...");
        session.enable_constraints().await?;
    }

    // Commit
    session.commit().await?;

    println!("\nRestore completed successfully!");

    Ok(())
}
