// Updated src/main.rs

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
use crate::database::schema::{
    add_columns_if_not_exist_sql_server, 
    add_columns_if_not_exist_mysql,
    add_scheduling_columns_if_not_exist_sql_server,
    add_scheduling_columns_if_not_exist_mysql
};
use crate::database::connection::connect_databases;
use crate::sync::engine::{optimized_sync_databases, sync_scheduling_records};
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

    // Ensure the additional columns exist on both databases for both tables - run in parallel
    let prepare_att_logs_futures = try_join(
        add_columns_if_not_exist_sql_server(&mut sql_client),
        add_columns_if_not_exist_mysql(&mut mysql_conn, &mysql_db_name),
    );
    prepare_att_logs_futures.await?;
    
    // Also prepare scheduling tables
    let prepare_scheduling_futures = try_join(
        add_scheduling_columns_if_not_exist_sql_server(&mut sql_client),
        add_scheduling_columns_if_not_exist_mysql(&mut mysql_conn, &mysql_db_name),
    );
    prepare_scheduling_futures.await?;

    connection_spinner.finish_with_message("✅ Databases connected and columns verified!");

    // First, sync attendance logs
    let fetch_spinner = create_spinner("Preparing to fetch attendance logs...");
    println!("\n{}", "🔄 SYNCING ATTENDANCE LOGS".bold().blue());
    
    let sync_result = optimized_sync_databases(&mut sql_client, &mut mysql_conn, fetch_spinner).await;

    match sync_result {
        Ok(_) => {
            println!(
                "{}",
                "✅ Attendance logs synchronization completed successfully!"
                    .green()
            );
        }
        Err(e) => {
            eprintln!("{} {}", "❌ Attendance logs sync failed:".red().bold(), e);
            return Err(e);
        }
    }

    // Then, sync scheduling records
    println!("\n{}", "🔄 SYNCING SCHEDULING RECORDS".bold().blue());
    
    let scheduling_sync_result = sync_scheduling_records(&mut sql_client, &mut mysql_conn).await;
    
    match scheduling_sync_result {
        Ok(_) => {
            println!(
                "{}",
                "✅ Scheduling records synchronization completed successfully!"
                    .green()
            );
        }
        Err(e) => {
            eprintln!("{} {}", "❌ Scheduling records sync failed:".red().bold(), e);
            return Err(e);
        }
    }

    // Display execution time
    let duration = start_time.elapsed();
    println!(
        "\n{}",
        format!("⏱️ Total execution time: {:.2?}", duration).cyan()
    );

    println!(
        "{}",
        "🎉 Database synchronization completed successfully! 🎉"
            .green()
            .bold()
    );

    Ok(())
}