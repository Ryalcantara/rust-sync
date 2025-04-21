// Optimized src/sync/engine.rs with direct status filtering

use anyhow::Result;
use colored::*;
use futures_util::future::try_join;
use indicatif::{MultiProgress, ProgressBar};
use std::collections::{HashMap, HashSet};
use chrono::Utc;
use mysql_async::prelude::*; 
use crate::models::SchedulingRecord;
use std::time::Instant;

use crate::database::connection::SqlServerClient;
use crate::database::mysql::{get_mysql_record_count, fetch_mysql_logs};
use crate::database::mysql::{fetch_mysql_scheduling, get_mysql_scheduling_count};
use crate::database::sql_server::fetch_sql_server_logs;
use crate::database::sql_server::fetch_sql_server_scheduling;
use crate::models::AttendanceLog;
use crate::sync::sql_to_mysql::batch_sync_sql_to_mysql;
use crate::sync::mysql_to_sql::batch_sync_mysql_to_sql;
use crate::sync::sql_to_mysql::batch_sync_scheduling_sql_to_mysql;
use crate::sync::mysql_to_sql::batch_sync_scheduling_mysql_to_sql;
use crate::ui::{create_spinner, create_progress_bar, create_multi_progress};

// Main synchronization function with direct status filtering
pub async fn optimized_sync_databases(
    sql_client: &mut SqlServerClient,
    mysql_conn: &mut mysql_async::Conn,
    fetch_spinner: ProgressBar,
) -> Result<()> {
    let overall_start = Instant::now();
    
    // 1. OPTIMIZED: Skip timestamp checks and directly filter by status
    fetch_spinner.set_message("⚡ Optimized sync: Directly targeting pending/NULL records...".to_string());
    
    // OPTIMIZATION: Only get records with NULL or pending status
    // For SQL Server
    let sql_query = 
        "SELECT log_id, employee_id, \
         CONVERT(VARCHAR(23), log_dtime, 120) as log_dtime_str, \
         ISNULL(add_by, 0) as add_by_safe, \
         CONVERT(VARCHAR(23), add_dtime, 120) as add_dtime_str, \
         insert_dtr_log_pic, hr_approval, dtr_type, remarks \
         FROM tbl_att_logs \
         WHERE sync_status IS NULL OR sync_status = 'pending' \
         ORDER BY log_id";

    // For MySQL
    let mysql_query = 
        "SELECT log_id, employee_id, log_dtime, add_by, add_dtime, \
         insert_dtr_log_pic, hr_approval, dtr_type, remarks \
         FROM tbl_att_logs \
         WHERE sync_status IS NULL OR sync_status = 'pending' \
         ORDER BY log_id";

    // Create a multi-progress display
    let multi_progress = create_multi_progress();
    
    // Fetch logs from SQL Server and MySQL - parallel fetch
    let fetch_sql_spinner = multi_progress.add(create_spinner("📥 Fetching pending logs from SQL Server..."));
    let fetch_mysql_spinner = multi_progress.add(create_spinner("📥 Fetching pending logs from MySQL..."));
    
    // Run both fetch operations in parallel
    let sql_logs_future = fetch_sql_server_logs(sql_client, &sql_query);
    let mysql_logs_future = fetch_mysql_logs(mysql_conn, &mysql_query);
    
    // Execute both fetch operations concurrently
    let (sql_logs_result, mysql_logs_result) = try_join(
        async {
            let start = Instant::now();
            let logs = sql_logs_future.await?;
            let elapsed = start.elapsed();
            fetch_sql_spinner.finish_with_message(format!(
                "✅ Fetched {} pending SQL Server logs in {:.2}s", 
                logs.len().to_string().blue().bold(),
                elapsed.as_secs_f64().to_string().yellow()
            ));
            Ok::<_, anyhow::Error>(logs)
        },
        async {
            let start = Instant::now();
            let logs = mysql_logs_future.await?;
            let elapsed = start.elapsed();
            fetch_mysql_spinner.finish_with_message(format!(
                "✅ Fetched {} pending MySQL logs in {:.2}s", 
                logs.len().to_string().green().bold(),
                elapsed.as_secs_f64().to_string().yellow()
            ));
            Ok::<_, anyhow::Error>(logs)
        }
    ).await?;
    
    let sql_logs = sql_logs_result;
    let mysql_logs = mysql_logs_result;
    
    // Calculate what logs need to be synced in each direction
    let sql_log_ids: HashSet<i32> = sql_logs.keys().copied().collect();
    let mysql_log_ids: HashSet<i32> = mysql_logs.keys().copied().collect();

    // IDs to insert in each database (records that exist in one database but not the other)
    let sql_only_ids: Vec<i32> = sql_log_ids.difference(&mysql_log_ids).copied().collect();
    let mysql_only_ids: Vec<i32> = mysql_log_ids.difference(&sql_log_ids).copied().collect();

    // IDs that exist in both databases
    let common_ids: HashSet<i32> = sql_log_ids.intersection(&mysql_log_ids).copied().collect();

    // Find logs that need updates (are different between databases)
    let need_update_in_mysql: Vec<i32> = common_ids
        .iter()
        .filter(|id| {
            let sql_log = &sql_logs[id];
            let mysql_log = &mysql_logs[id];
            sql_log != mysql_log
        })
        .copied()
        .collect();

    let need_update_in_sql: Vec<i32> = common_ids
        .iter()
        .filter(|id| {
            let sql_log = &sql_logs[id];
            let mysql_log = &mysql_logs[id];
            sql_log != mysql_log
        })
        .copied()
        .collect();
    
    // Count of identical records in both databases (these don't need syncing)
    let identical_records = common_ids.len() - need_update_in_mysql.len();
    
    if identical_records > 0 {
        println!("{} {} {}", 
            "↪".cyan(),
            format!("{} records already identical - marking as synced", identical_records).yellow().bold(),
            "↩".cyan()
        );
    }

    // Display sync summary with enhanced visuals
    fetch_spinner.finish_with_message(format!(
        "✅ Analysis complete: {} SQL Server pending logs, {} MySQL pending logs",
        sql_logs.len().to_string().blue().bold(),
        mysql_logs.len().to_string().green().bold()
    ));
    
    // Early return if no updates needed
    if sql_logs.is_empty() && mysql_logs.is_empty() {
        println!("{}", "⚡ No pending records found, databases are in sync!".green().bold());
        return Ok(());
    }
    
    // Show what needs to be synced with a nice table format
    println!("{}", "┌─────────────────────────────────────────────────┐".bright_blue());
    println!("{} {} {}",
        "│".bright_blue(),
        " 🔄 SYNC REQUIREMENTS SUMMARY                    ".bold().white().on_blue(),
        "│".bright_blue()
    );
    println!("{}", "├─────────────────────────────────────────────────┤".bright_blue());
    println!("{} {:<20} {:<10} {:<10} {}",
        "│".bright_blue(),
        "Direction".bold(),
        "New".bold(),
        "Updates".bold(),
        "│".bright_blue()
    );
    println!("{}", "├─────────────────────────────────────────────────┤".bright_blue());
    println!("{} {:<20} {:<10} {:<10} {}",
        "│".bright_blue(),
        "SQL Server → MySQL".cyan(),
        sql_only_ids.len().to_string().yellow().bold(),
        need_update_in_mysql.len().to_string().yellow().bold(),
        "│".bright_blue()
    );
    println!("{} {:<20} {:<10} {:<10} {}",
        "│".bright_blue(),
        "MySQL → SQL Server".green(),
        mysql_only_ids.len().to_string().yellow().bold(),
        need_update_in_sql.len().to_string().yellow().bold(),
        "│".bright_blue()
    );
    println!("{}", "└─────────────────────────────────────────────────┘".bright_blue());

    // Current timestamp for sync_datetime
    let sync_timestamp = Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
    
    // Mark existing identical records as synced
    if identical_records > 0 {
        update_identical_records(
            sql_client, 
            mysql_conn, 
            &sql_logs, 
            &mysql_logs, 
            &common_ids, 
            &sync_timestamp
        ).await?;
    }
    
    // Progress bar for sync
    let total_operations = sql_only_ids.len() + mysql_only_ids.len() + need_update_in_mysql.len() + need_update_in_sql.len();
    
    // If no operations to perform, exit early
    if total_operations == 0 {
        return Ok(());
    }
    
    println!("{}", "┌─────────────────────────────────────────────────┐".bright_blue());
    println!("{} {} {}",
        "│".bright_blue(),
        " 🚀 STARTING DATABASE SYNCHRONIZATION            ".bold().white().on_blue(),
        "│".bright_blue()
    );
    println!("{}", "└─────────────────────────────────────────────────┘".bright_blue());
    
    // Create multi-progress display for sync operations
    let sync_multi = create_multi_progress();
    
    // Create progress bars for both sync directions
    let sql_to_mysql_progress = sync_multi.add(create_progress_bar(
        (sql_only_ids.len() + need_update_in_mysql.len()) as u64,
        &format!("🔄 {} → MySQL", "SQL Server".blue())
    ));
    
    let mysql_to_sql_progress = sync_multi.add(create_progress_bar(
        (mysql_only_ids.len() + need_update_in_sql.len()) as u64,
        &format!("🔄 {} → SQL Server", "MySQL".green())
    ));

    // 3. Perform batch operations for better performance - now with parallel execution
    // Run both sync operations in parallel
    let sql_to_mysql_future = batch_sync_sql_to_mysql(
        mysql_conn,
        &sql_logs,
        &sql_only_ids,
        &need_update_in_mysql,
        &sync_timestamp,
        &sql_to_mysql_progress,
    );
    
    let mysql_to_sql_future = batch_sync_mysql_to_sql(
        sql_client,
        &mysql_logs,
        &mysql_only_ids,
        &need_update_in_sql,
        &sync_timestamp,
        &mysql_to_sql_progress,
    );
    
    // Execute both sync operations concurrently
    let (sql_to_mysql_result, mysql_to_sql_result) = try_join(
        sql_to_mysql_future,
        mysql_to_sql_future,
    ).await?;

    // Display final summary with enhanced visuals
    let overall_elapsed = overall_start.elapsed();
    
    println!("{}", "┌─────────────────────────────────────────────────┐".bright_blue());
    println!("{} {} {}",
        "│".bright_blue(),
        " ✅ SYNC RESULTS SUMMARY                         ".bold().white().on_green(),
        "│".bright_blue()
    );
    println!("{}", "├─────────────────────────────────────────────────┤".bright_blue());
    println!("{} {:<15} {:<15} {:<15} {}",
        "│".bright_blue(),
        "Direction".bold(),
        "Inserted".bold(),
        "Updated".bold(),
        "│".bright_blue()
    );
    println!("{}", "├─────────────────────────────────────────────────┤".bright_blue());
    println!("{} {:<15} {:<15} {:<15} {}",
        "│".bright_blue(),
        "SQL → MySQL".cyan(),
        sql_to_mysql_result.0.to_string().green().bold(),
        sql_to_mysql_result.1.to_string().green().bold(),
        "│".bright_blue()
    );
    println!("{} {:<15} {:<15} {:<15} {}",
        "│".bright_blue(),
        "MySQL → SQL".green(),
        mysql_to_sql_result.0.to_string().green().bold(),
        mysql_to_sql_result.1.to_string().green().bold(),
        "│".bright_blue()
    );
    println!("{}", "├─────────────────────────────────────────────────┤".bright_blue());
    println!("{} {:<30} {:<15} {}",
        "│".bright_blue(),
        "Total time:".bold(),
        format!("{:.2}s", overall_elapsed.as_secs_f64()).yellow().bold(),
        "│".bright_blue()
    );
    println!("{}", "└─────────────────────────────────────────────────┘".bright_blue());

    Ok(())
}

// Helper function to mark identical records as synced - enhanced visual feedback
async fn update_identical_records(
    sql_client: &mut SqlServerClient,
    mysql_conn: &mut mysql_async::Conn,
    sql_logs: &HashMap<i32, AttendanceLog>,
    mysql_logs: &HashMap<i32, AttendanceLog>,
    common_ids: &HashSet<i32>,
    sync_timestamp: &str,
) -> Result<()> {
    if common_ids.is_empty() {
        return Ok(());
    }

    // Get IDs of records that are identical
    let identical_ids: Vec<i32> = common_ids.iter()
        .filter(|id| {
            let sql_log = &sql_logs[id];
            let mysql_log = &mysql_logs[id];
            // Consider them identical if their content matches, 
            // even if one has 'pending' status
            sql_log == mysql_log
        })
        .copied()
        .collect();
        
    // If there are identical records, we still want to mark them as synced
    if !identical_ids.is_empty() {
        let chunk_size = 100;
        let update_spinner = create_spinner(&format!(
            "🔄 Marking {} identical records as synced...", 
            identical_ids.len().to_string().cyan()
        ));
        
        let start_time = Instant::now();
        
        // Update SQL Server
        for (i, chunk) in identical_ids.chunks(chunk_size).enumerate() {
            if !chunk.is_empty() {
                let placeholders = chunk.iter()
                    .map(|id| id.to_string())
                    .collect::<Vec<String>>()
                    .join(",");
                    
                let sql_query = format!(
                    "UPDATE tbl_att_logs SET sync_status = 'SYNCED', sync_datetime = '{}' \
                     WHERE log_id IN ({})",
                    sync_timestamp, placeholders
                );
                
                // Execute SQL Server update
                sql_client.execute(&sql_query, &[]).await?;
                
                update_spinner.set_message(format!(
                    "🔄 Marking identical records: SQL Server batch {}/{} processed", 
                    i + 1,
                    identical_ids.chunks(chunk_size).len()
                ));
            }
        }
        
        // Update MySQL
        let mut transaction = mysql_conn.start_transaction(Default::default()).await?;
        
        for (i, chunk) in identical_ids.chunks(chunk_size).enumerate() {
            if !chunk.is_empty() {
                // For MySQL, we need to create a separate parameter for each ID
                let placeholders = chunk.iter()
                    .map(|_| "?".to_string())
                    .collect::<Vec<String>>()
                    .join(",");
                
                let mysql_query = format!(
                    "UPDATE tbl_att_logs SET sync_status = 'SYNCED', sync_datetime = '{}' \
                     WHERE log_id IN ({})",
                    sync_timestamp, placeholders
                );
                
                // Convert i32 chunk to Vec<i32> for MySQL parameters
                let params = chunk.iter()
                    .map(|&id| id)
                    .collect::<Vec<i32>>();
                
                // Execute MySQL update
                transaction.exec_drop(mysql_query, params).await?;
                
                update_spinner.set_message(format!(
                    "🔄 Marking identical records: MySQL batch {}/{} processed", 
                    i + 1,
                    identical_ids.chunks(chunk_size).len()
                ));
            }
        }
        
        transaction.commit().await?;
        
        let elapsed = start_time.elapsed();
        update_spinner.finish_with_message(format!(
            "✓ Marked {} identical records as synced in {:.2}s", 
            identical_ids.len().to_string().green().bold(),
            elapsed.as_secs_f64().to_string().yellow()
        ));
    }
    
    Ok(())
}

// Optimized synchronization function for scheduling records with direct status filtering
pub async fn sync_scheduling_records(
    sql_client: &mut SqlServerClient,
    mysql_conn: &mut mysql_async::Conn,
) -> Result<()> {
    let overall_start = Instant::now();
    
    let fetch_spinner = create_spinner("📊 Preparing to fetch scheduling records...");

    // OPTIMIZATION: Skip timestamp checks and directly filter by status
    fetch_spinner.set_message("⚡ Optimized sync: Directly targeting pending/NULL scheduling records...".to_string());
    
    // For SQL Server - directly target records with NULL or pending status
    let sql_query = 
        "SELECT scheduling_id, 
         CONVERT(VARCHAR(10), date_start, 120) as date_start_str, 
         CONVERT(VARCHAR(10), date_end, 120) as date_end_str, 
         remarks, station, employee_id, department, time_start, time_end, 
         CONVERT(VARCHAR(23), updated_at, 120) as updated_at_str, 
         CONVERT(VARCHAR(23), created_at, 120) as created_at_str, 
         [case], remark_2nd, display_order, display_order_2nd 
         FROM tbl_scheduling 
         WHERE sync_status IS NULL OR sync_status = 'pending' 
         ORDER BY scheduling_id";

    // For MySQL - directly target records with NULL or pending status
    let mysql_query = 
        "SELECT scheduling_id, 
         DATE_FORMAT(date_start, '%Y-%m-%d') as date_start, 
         DATE_FORMAT(date_end, '%Y-%m-%d') as date_end, 
         remarks, station, employee_id, department, time_start, time_end, 
         updated_at, created_at, `case`, remark_2nd, display_order, display_order_2nd 
         FROM tbl_scheduling 
         WHERE sync_status IS NULL OR sync_status = 'pending' 
         ORDER BY scheduling_id";

    // Create a multi-progress display
    let multi_progress = create_multi_progress();
    
    // Fetch logs from SQL Server and MySQL - parallel fetch
    let fetch_sql_spinner = multi_progress.add(create_spinner("📥 Fetching pending scheduling records from SQL Server..."));
    let fetch_mysql_spinner = multi_progress.add(create_spinner("📥 Fetching pending scheduling records from MySQL..."));
    
    // Run both fetch operations in parallel
    let sql_records_future = fetch_sql_server_scheduling(sql_client, &sql_query);
    let mysql_records_future = fetch_mysql_scheduling(mysql_conn, &mysql_query);
    
    // Execute both fetch operations concurrently
    let (sql_records_result, mysql_records_result) = try_join(
        async {
            let start = Instant::now();
            let records = sql_records_future.await?;
            let elapsed = start.elapsed();
            fetch_sql_spinner.finish_with_message(format!(
                "✅ Fetched {} pending SQL Server scheduling records in {:.2}s", 
                records.len().to_string().blue().bold(),
                elapsed.as_secs_f64().to_string().yellow()
            ));
            Ok::<_, anyhow::Error>(records)
        },
        async {
            let start = Instant::now();
            let records = mysql_records_future.await?;
            let elapsed = start.elapsed();
            fetch_mysql_spinner.finish_with_message(format!(
                "✅ Fetched {} pending MySQL scheduling records in {:.2}s", 
                records.len().to_string().green().bold(),
                elapsed.as_secs_f64().to_string().yellow()
            ));
            Ok::<_, anyhow::Error>(records)
        }
    ).await?;
    
    let sql_records = sql_records_result;
    let mysql_records = mysql_records_result;
    
    // Calculate what records need to be synced in each direction
    let sql_record_ids: HashSet<i32> = sql_records.keys().copied().collect();
    let mysql_record_ids: HashSet<i32> = mysql_records.keys().copied().collect();

    // IDs to insert in each database (records that exist in one database but not the other)
    let sql_only_ids: Vec<i32> = sql_record_ids.difference(&mysql_record_ids).copied().collect();
    let mysql_only_ids: Vec<i32> = mysql_record_ids.difference(&sql_record_ids).copied().collect();

    // IDs that exist in both databases
    let common_ids: HashSet<i32> = sql_record_ids.intersection(&mysql_record_ids).copied().collect();

    // Find records that need updates (are different between databases)
    let need_update_in_mysql: Vec<i32> = common_ids
        .iter()
        .filter(|id| {
            let sql_record = &sql_records[id];
            let mysql_record = &mysql_records[id];
            sql_record != mysql_record
        })
        .copied()
        .collect();

    let need_update_in_sql: Vec<i32> = common_ids
        .iter()
        .filter(|id| {
            let sql_record = &sql_records[id];
            let mysql_record = &mysql_records[id];
            sql_record != mysql_record
        })
        .copied()
        .collect();
    
    // Count of identical records in both databases (these don't need syncing)
    let identical_records = common_ids.len() - need_update_in_mysql.len();
    
    if identical_records > 0 {
        println!("{} {} {}", 
            "↪".cyan(),
            format!("{} scheduling records already identical - marking as synced", identical_records).yellow().bold(),
            "↩".cyan()
        );
    }

    // Display sync summary with enhanced visuals
    fetch_spinner.finish_with_message(format!(
        "✅ Analysis complete: {} SQL Server pending scheduling records, {} MySQL pending scheduling records",
        sql_records.len().to_string().blue().bold(),
        mysql_records.len().to_string().green().bold()
    ));
    
    // Early return if no updates needed
    if sql_records.is_empty() && mysql_records.is_empty() {
        println!("{}", "⚡ No pending scheduling records found, both databases are in sync!".green().bold());
        return Ok(());
    }
    
    // Show what needs to be synced with a nice table format
    println!("{}", "┌─────────────────────────────────────────────────┐".bright_blue());
    println!("{} {} {}",
        "│".bright_blue(),
        " 🔄 SCHEDULING SYNC REQUIREMENTS                 ".bold().white().on_blue(),
        "│".bright_blue()
    );
    println!("{}", "├─────────────────────────────────────────────────┤".bright_blue());
    println!("{} {:<20} {:<10} {:<10} {}",
        "│".bright_blue(),
        "Direction".bold(),
        "New".bold(),
        "Updates".bold(),
        "│".bright_blue()
    );
    println!("{}", "├─────────────────────────────────────────────────┤".bright_blue());
    println!("{} {:<20} {:<10} {:<10} {}",
        "│".bright_blue(),
        "SQL Server → MySQL".cyan(),
        sql_only_ids.len().to_string().yellow().bold(),
        need_update_in_mysql.len().to_string().yellow().bold(),
        "│".bright_blue()
    );
    println!("{} {:<20} {:<10} {:<10} {}",
        "│".bright_blue(),
        "MySQL → SQL Server".green(),
        mysql_only_ids.len().to_string().yellow().bold(),
        need_update_in_sql.len().to_string().yellow().bold(),
        "│".bright_blue()
    );
    println!("{}", "└─────────────────────────────────────────────────┘".bright_blue());

    // Current timestamp for sync_datetime
    let sync_timestamp = Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
    
    // Mark existing identical records as synced
    if identical_records > 0 {
        update_identical_scheduling_records(
            sql_client, 
            mysql_conn, 
            &sql_records, 
            &mysql_records, 
            &common_ids, 
            &sync_timestamp
        ).await?;
    }
    
    // Progress bar for sync
    let total_operations = sql_only_ids.len() + mysql_only_ids.len() + need_update_in_mysql.len() + need_update_in_sql.len();
    
    // If no operations to perform, exit early
    if total_operations == 0 {
        return Ok(());
    }
    
    println!("{}", "┌─────────────────────────────────────────────────┐".bright_blue());
    println!("{} {} {}",
        "│".bright_blue(),
        " 🚀 STARTING SCHEDULING SYNCHRONIZATION         ".bold().white().on_blue(),
        "│".bright_blue()
    );
    println!("{}", "└─────────────────────────────────────────────────┘".bright_blue());
    
    // Create multi-progress display for sync operations
    let sync_multi = create_multi_progress();
    
    // Create progress bars for both sync directions
    let sql_to_mysql_progress = sync_multi.add(create_progress_bar(
        (sql_only_ids.len() + need_update_in_mysql.len()) as u64,
        &format!("🔄 {} → MySQL (Scheduling)", "SQL Server".blue())
    ));
    
    let mysql_to_sql_progress = sync_multi.add(create_progress_bar(
        (mysql_only_ids.len() + need_update_in_sql.len()) as u64,
        &format!("🔄 {} → SQL Server (Scheduling)", "MySQL".green())
    ));

    // 3. Perform batch operations for better performance - now with parallel execution
    // Run both sync operations in parallel for scheduling
    let sql_to_mysql_future = batch_sync_scheduling_sql_to_mysql(
        mysql_conn,
        &sql_records,
        &sql_only_ids,
        &need_update_in_mysql,
        &sync_timestamp,
        &sql_to_mysql_progress,
    );
    
    let mysql_to_sql_future = batch_sync_scheduling_mysql_to_sql(
        sql_client,
        &mysql_records,
        &mysql_only_ids,
        &need_update_in_sql,
        &sync_timestamp,
        &mysql_to_sql_progress,
    );
    
    // Execute both sync operations concurrently
    let (sql_to_mysql_result, mysql_to_sql_result) = try_join(
        sql_to_mysql_future,
        mysql_to_sql_future,
    ).await?;

    // Display final summary with enhanced visuals
    let overall_elapsed = overall_start.elapsed();
    
    println!("{}", "┌─────────────────────────────────────────────────┐".bright_blue());
    println!("{} {} {}",
        "│".bright_blue(),
        " ✅ SCHEDULING SYNC RESULTS                      ".bold().white().on_green(),
        "│".bright_blue()
    );
    println!("{}", "├─────────────────────────────────────────────────┤".bright_blue());
    println!("{} {:<15} {:<15} {:<15} {}",
        "│".bright_blue(),
        "Direction".bold(),
        "Inserted".bold(),
        "Updated".bold(),
        "│".bright_blue()
    );
    println!("{}", "├─────────────────────────────────────────────────┤".bright_blue());
    println!("{} {:<15} {:<15} {:<15} {}",
        "│".bright_blue(),
        "SQL → MySQL".cyan(),
        sql_to_mysql_result.0.to_string().green().bold(),
        sql_to_mysql_result.1.to_string().green().bold(),
        "│".bright_blue()
    );
    println!("{} {:<15} {:<15} {:<15} {}",
        "│".bright_blue(),
        "MySQL → SQL".green(),
        mysql_to_sql_result.0.to_string().green().bold(),
        mysql_to_sql_result.1.to_string().green().bold(),
        "│".bright_blue()
    );
    println!("{}", "├─────────────────────────────────────────────────┤".bright_blue());
    println!("{} {:<30} {:<15} {}",
        "│".bright_blue(),
        "Total time:".bold(),
        format!("{:.2}s", overall_elapsed.as_secs_f64()).yellow().bold(),
        "│".bright_blue()
    );
    println!("{}", "└─────────────────────────────────────────────────┘".bright_blue());

    Ok(())
}

// Helper function to mark identical scheduling records as synced with enhanced visuals
async fn update_identical_scheduling_records(
    sql_client: &mut SqlServerClient,
    mysql_conn: &mut mysql_async::Conn,
    sql_records: &HashMap<i32, SchedulingRecord>,
    mysql_records: &HashMap<i32, SchedulingRecord>,
    common_ids: &HashSet<i32>,
    sync_timestamp: &str,
) -> Result<()> {
    if common_ids.is_empty() {
        return Ok(());
    }

    // Get IDs of records that are identical
    let identical_ids: Vec<i32> = common_ids.iter()
        .filter(|id| {
            let sql_record = &sql_records[id];
            let mysql_record = &mysql_records[id];
            // Consider them identical if their content matches
            sql_record == mysql_record
        })
        .copied()
        .collect();
        
    // If there are identical records, we still want to mark them as synced
    if !identical_ids.is_empty() {
        let chunk_size = 100;
        let update_spinner = create_spinner(&format!(
            "🔄 Marking {} identical scheduling records as synced...", 
            identical_ids.len().to_string().cyan()
        ));
        
        let start_time = Instant::now();
        
        // Update SQL Server
        for (i, chunk) in identical_ids.chunks(chunk_size).enumerate() {
            if !chunk.is_empty() {
                let placeholders = chunk.iter()
                    .map(|id| id.to_string())
                    .collect::<Vec<String>>()
                    .join(",");
                    
                let sql_query = format!(
                    "UPDATE tbl_scheduling SET sync_status = 'SYNCED', sync_datetime = '{}' \
                     WHERE scheduling_id IN ({})",
                    sync_timestamp, placeholders
                );
                
                // Execute SQL Server update
                sql_client.execute(&sql_query, &[]).await?;
                
                update_spinner.set_message(format!(
                    "🔄 Marking identical scheduling records: SQL Server batch {}/{} processed", 
                    i + 1,
                    identical_ids.chunks(chunk_size).len()
                ));
            }
        }
        
        // Update MySQL
        let mut transaction = mysql_conn.start_transaction(Default::default()).await?;
        
        for (i, chunk) in identical_ids.chunks(chunk_size).enumerate() {
            if !chunk.is_empty() {
                // For MySQL, we need to create a separate parameter for each ID
                let placeholders = chunk.iter()
                    .map(|_| "?".to_string())
                    .collect::<Vec<String>>()
                    .join(",");
                
                let mysql_query = format!(
                    "UPDATE tbl_scheduling SET sync_status = 'SYNCED', sync_datetime = '{}' \
                     WHERE scheduling_id IN ({})",
                    sync_timestamp, placeholders
                );
                
                // Convert i32 chunk to Vec<i32> for MySQL parameters
                let params = chunk.iter()
                    .map(|&id| id)
                    .collect::<Vec<i32>>();
                
                // Execute MySQL update
                transaction.exec_drop(mysql_query, params).await?;
                
                update_spinner.set_message(format!(
                    "🔄 Marking identical scheduling records: MySQL batch {}/{} processed", 
                    i + 1,
                    identical_ids.chunks(chunk_size).len()
                ));
            }
        }
        
        transaction.commit().await?;
        
        let elapsed = start_time.elapsed();
        update_spinner.finish_with_message(format!(
            "✓ Marked {} identical scheduling records as synced in {:.2}s", 
            identical_ids.len().to_string().green().bold(),
            elapsed.as_secs_f64().to_string().yellow()
        ));
    }
    
    Ok(())
}