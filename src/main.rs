use anyhow::Result;
use colored::*;
use dotenv::dotenv;
use futures_util::future::try_join;
use std::time::Instant;

mod models;
mod ui;
mod database;
mod sync;

// Use the optimized imports
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

    // Initialize UI with beautiful banner
    init_ui();

    // Load environment variables
    dotenv().ok();
    println!("{}", "✨ Environment loaded successfully".green());

    // Establish database connections with visual feedback
    let connection_spinner = create_spinner("🔌 Establishing database connections...");

    // Connect to both databases
    let (mut sql_client, mut mysql_conn, mysql_db_name) =
        connect_databases(&connection_spinner).await?;

    // Ensure the additional columns exist on both databases for both tables - run in parallel
    let schema_spinner = create_spinner("🏗️  Verifying database schemas...");
    
    let prepare_att_logs_futures = try_join(
        add_columns_if_not_exist_sql_server(&mut sql_client),
        add_columns_if_not_exist_mysql(&mut mysql_conn, &mysql_db_name),
    );
    prepare_att_logs_futures.await?;
    
    schema_spinner.set_message("🏗️  Verifying scheduling table schemas...".to_string());
    
    // Also prepare scheduling tables
    let prepare_scheduling_futures = try_join(
        add_scheduling_columns_if_not_exist_sql_server(&mut sql_client),
        add_scheduling_columns_if_not_exist_mysql(&mut mysql_conn, &mysql_db_name),
    );
    prepare_scheduling_futures.await?;

    schema_spinner.finish_with_message("✅ Database schemas verified and ready!".green().to_string());
    connection_spinner.finish_with_message("✅ Database connections established!".green().to_string());

    // Print separator before starting sync operations
    println!("{}", "───────────────────────────────────────────────".bright_black());
    
    // First, sync attendance logs using optimized approach
    let fetch_spinner = create_spinner("⚡ Preparing for optimized sync (pending/NULL records only)...");
    println!("{}", "┌─────────────────────────────────────────────────┐".bright_blue());
    println!("{} {} {}",
        "│".bright_blue(),
        " 🔄 SYNCING ATTENDANCE LOGS (OPTIMIZED)          ".bold().white().on_blue(),
        "│".bright_blue()
    );
    println!("{}", "└─────────────────────────────────────────────────┘".bright_blue());
    
    let sync_result = optimized_sync_databases(&mut sql_client, &mut mysql_conn, fetch_spinner).await;

    match sync_result {
        Ok(_) => {
            println!(
                "{}",
                "✅ Attendance logs synchronization completed successfully!"
                    .green().bold()
            );
        }
        Err(e) => {
            eprintln!("{} {}", "❌ Attendance logs sync failed:".red().bold(), e);
            return Err(e);
        }
    }

    // Print separator between sync operations
    println!("{}", "───────────────────────────────────────────────".bright_black());
    
    // Then, sync scheduling records using optimized approach
    println!("{}", "┌─────────────────────────────────────────────────┐".bright_blue());
    println!("{} {} {}",
        "│".bright_blue(),
        " 🔄 SYNCING SCHEDULING RECORDS (OPTIMIZED)       ".bold().white().on_blue(),
        "│".bright_blue()
    );
    println!("{}", "└─────────────────────────────────────────────────┘".bright_blue());
    
    let scheduling_sync_result = sync_scheduling_records(&mut sql_client, &mut mysql_conn).await;
    
    match scheduling_sync_result {
        Ok(_) => {
            println!(
                "{}",
                "✅ Scheduling records synchronization completed successfully!"
                    .green().bold()
            );
        }
        Err(e) => {
            eprintln!("{} {}", "❌ Scheduling records sync failed:".red().bold(), e);
            return Err(e);
        }
    }

    // Display execution time with nice formatting
    let duration = start_time.elapsed();
    
    println!("{}", "───────────────────────────────────────────────".bright_black());
    println!("{}", "┌─────────────────────────────────────────────────┐".bright_blue());
    println!("{} {} {}",
        "│".bright_blue(),
        " 📈 SYNCHRONIZATION PERFORMANCE                 ".bold().white().on_blue(),
        "│".bright_blue()
    );
    println!("{}", "├─────────────────────────────────────────────────┤".bright_blue());
    println!("{} {:<30} {:<15} {}",
        "│".bright_blue(),
        "Total execution time:".bold(),
        format!("{:.2?}", duration).yellow().bold(),
        "│".bright_blue()
    );
    println!("{}", "└─────────────────────────────────────────────────┘".bright_blue());

    println!("{}", "───────────────────────────────────────────────".bright_black());
    println!(
        "{}",
        "🎉 Database synchronization completed successfully! 🎉"
            .green()
            .bold()
    );

    Ok(())
}