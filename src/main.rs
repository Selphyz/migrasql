mod cli;
mod dump;
mod engine;
mod import;
mod migrate;
mod restore;
mod util;

use anyhow::Result;
use clap::Parser;
use cli::{Cli, Commands};
use futures::FutureExt;
use std::panic::AssertUnwindSafe;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Dump {
            source,
            source_env,
            output,
            provider,
            tables,
            exclude,
            schema_only,
            data_only,
            batch_rows,
            consistent_snapshot,
            gzip,
        } => {
            let source_url = Commands::get_url(&source, &source_env, "source")?;

            println!("Connecting to: {}", Commands::redact_url(&source_url));

            let engine = engine::create_engine(&provider)?;

            let opts = dump::DumpOptions {
                tables,
                exclude,
                schema_only,
                data_only,
                batch_rows,
                consistent_snapshot,
                gzip,
            };

            dump::dump(&*engine, &source_url, &output, opts).await?;
        }

        Commands::Restore {
            destination,
            destination_env,
            input,
            provider,
            disable_fk_checks,
        } => {
            let dest_url = Commands::get_url(&destination, &destination_env, "destination")?;

            println!("Connecting to: {}", Commands::redact_url(&dest_url));

            let engine = engine::create_engine(&provider)?;

            let opts = restore::RestoreOptions { disable_fk_checks };

            restore::restore(&*engine, &dest_url, &input, opts).await?;
        }

        Commands::Migrate {
            source,
            source_env,
            destination,
            destination_env,
            provider,
            tables,
            exclude,
            schema_only,
            data_only,
            batch_rows,
            consistent_snapshot,
            disable_fk_checks,
            skip_errors,
        } => {
            let source_url = Commands::get_url(&source, &source_env, "source")?;
            let dest_url = Commands::get_url(&destination, &destination_env, "destination")?;

            println!("Source: {}", Commands::redact_url(&source_url));
            println!("Destination: {}", Commands::redact_url(&dest_url));

            let engine = engine::create_engine(&provider)?;

            let opts = migrate::MigrateOptions {
                tables,
                exclude,
                schema_only,
                data_only,
                batch_rows,
                consistent_snapshot,
                disable_fk_checks,
                skip_errors,
            };

            migrate::migrate(&*engine, &source_url, &dest_url, opts).await?;
        }

        Commands::Import {
            destination,
            destination_env,
            input,
            table,
            provider,
            batch_rows,
            disable_fk_checks,
            columns,
            skip_errors,
            details,
            start_line,
        } => {
            let dest_url = Commands::get_url(&destination, &destination_env, "destination")?;

            println!("Destination: {}", Commands::redact_url(&dest_url));

            let engine = engine::create_engine(&provider)?;

            let column_mapping = columns
                .as_ref()
                .map(|c| import::parse_column_mapping(c))
                .transpose()?;

            let opts = import::ImportOptions {
                input,
                table,
                batch_rows,
                disable_fk_checks,
                skip_errors,
                column_mapping,
                details,
                start_line,
            };

            let progress = import::ImportProgressTracker::new();
            let import_result = AssertUnwindSafe(import::import(
                &*engine,
                &dest_url,
                opts,
                Some(progress.clone()),
            ))
            .catch_unwind()
            .await;

            match import_result {
                Ok(result) => result?,
                Err(_) => {
                    eprintln!(
                        "\nImport panicked unexpectedly.\n{}",
                        progress.format_last_success_for_output()
                    );
                    return Err(anyhow::anyhow!(
                        "Import aborted due to panic. Check logs above."
                    ));
                }
            }
        }
    }

    Ok(())
}
