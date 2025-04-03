use anyhow::Result;
use colored::*;
use dotenv::dotenv;
use futures_util::future::try_join;
use std::time::Instant;

mod models;
mod ui;
mod database;
mod sync;

// Use the correct direct imports instead of re-exports
use crate::database::schema::{add_columns_if_not_exist_sql_server, add_columns_if_not_exist_mysql};
use crate::database::connection::connect_databases;
use crate::sync::engine::optimized_sync_databases;
use crate::ui::{create_spinner, init_ui};

#[tokio::main]
async fn main() -> Result<()> {
    let start_time = Instant::now();

    init_ui();

    dotenv().ok();

    let connection_spinner = create_spinner("Establishing database connections...");

    // Connect to both databases
    let (mut sql_client, mut mysql_conn, mysql_db_name) =
        connect_databases(&connection_spinner).await?;

    // Ensure the additional columns exist on both databases - run in parallel
    let prepare_futures = try_join(
        add_columns_if_not_exist_sql_server(&mut sql_client),
        add_columns_if_not_exist_mysql(&mut mysql_conn, &mysql_db_name),
    );
    prepare_futures.await?;

    connection_spinner.finish_with_message("✅ Databases connected and columns verified!");

    let fetch_spinner = create_spinner("Preparing to fetch logs...");

    let sync_result = optimized_sync_databases(&mut sql_client, &mut mysql_conn, fetch_spinner).await;

    // Display execution time
    let duration = start_time.elapsed();
    println!(
        "\n{}",
        format!("⏱️ Total execution time: {:.2?}", duration).cyan()
    );

    match sync_result {
        Ok(_) => {
            println!(
                "{}",
                "🎉 Database synchronization completed successfully! 🎉"
                    .green()
                    .bold()
            );
        }
        Err(e) => {
            eprintln!("{} {}", "❌ Sync failed:".red().bold(), e);
        }
    }

    Ok(())
}