use anyhow::Result;
use colored::*;
use futures_util::future::try_join;
use indicatif::ProgressBar;
use std::collections::{HashMap, HashSet};
use chrono::Utc;
use mysql_async::prelude::*; 

use crate::database::connection::SqlServerClient;
use crate::database::mysql::{get_mysql_record_count, fetch_mysql_logs};
use crate::database::sql_server::fetch_sql_server_logs;
use crate::models::AttendanceLog;
use crate::sync::sql_to_mysql::batch_sync_sql_to_mysql;
use crate::sync::mysql_to_sql::batch_sync_mysql_to_sql;
use crate::ui::{create_spinner, create_progress_bar};

// Main synchronization function
pub async fn optimized_sync_databases(
    sql_client: &mut SqlServerClient,
    mysql_conn: &mut mysql_async::Conn,
    fetch_spinner: ProgressBar,
) -> Result<()> {
    // 1. Get the latest sync timestamp from both databases
    fetch_spinner.set_message("Determining last sync time...".to_string());
    
    // Get the most recent sync time from SQL Server - handle NULL explicitly in SQL
    let sql_last_sync: Option<String> = {
        // Convert NULL to a string representation in SQL Server to avoid Rust conversion issues
        // Include both SYNCED and pending records in the sync logic
        let query = "SELECT CONVERT(VARCHAR(23), MAX(sync_datetime), 120) FROM tbl_att_logs WHERE sync_status IN ('SYNCED', 'pending')";
        let result = sql_client.query(query, &[]).await?;
        
        // Process the result safely
        match result.into_row().await? {
            Some(row) => {
                // Get as Option<&str> to properly handle NULL values
                let str_val: Option<&str> = row.get(0);
                match str_val {
                    Some(s) if !s.is_empty() => Some(s.to_string()),
                    _ => None
                }
            },
            None => None
        }
    };

    // Get the most recent sync time from MySQL - handle NULL with COALESCE
    let mysql_last_sync: Option<String> = {
        // Use COALESCE to convert NULL to empty string in MySQL
        // Include both SYNCED and pending records in the sync logic
        let query = "SELECT COALESCE(MAX(sync_datetime), '') FROM tbl_att_logs WHERE sync_status IN ('SYNCED', 'pending')";
        let result: String = mysql_conn.query_first(query).await?.unwrap_or_default();
        
        // Only use non-empty results
        if !result.is_empty() {
            Some(result)
        } else {
            None
        }
    };

    // Determine the overall last sync time (use the earlier one if they differ)
    let last_sync_str = match (sql_last_sync.as_deref(), mysql_last_sync.as_deref()) {
        (Some(sql), Some(mysql)) => {
            if sql < mysql { Some(sql.to_string()) } else { Some(mysql.to_string()) }
        },
        (Some(sql), None) => Some(sql.to_string()),
        (None, Some(mysql)) => Some(mysql.to_string()),
        (None, None) => None,
    };

    // Format for SQL queries - now including records with 'pending' status
    let last_sync_clause = match &last_sync_str {
        Some(timestamp) => format!("AND (sync_status IS NULL OR sync_status = 'pending' OR sync_datetime > '{}')", timestamp),
        None => String::from(""),
    };

    fetch_spinner.set_message(format!(
        "Fetching unsynchronized logs since {}...",
        last_sync_str.as_deref().unwrap_or("beginning")
    ));

    // 2. Fetch only the logs that need synchronization
    // For SQL Server
    let sql_query = format!(
        "SELECT log_id, employee_id, \
         CONVERT(VARCHAR(23), log_dtime, 120) as log_dtime_str, \
         ISNULL(add_by, 0) as add_by_safe, \
         CONVERT(VARCHAR(23), add_dtime, 120) as add_dtime_str, \
         insert_dtr_log_pic, hr_approval, dtr_type, remarks \
         FROM tbl_att_logs \
         WHERE 1=1 {} \
         ORDER BY log_id",
        last_sync_clause
    );

    // For MySQL
    let mysql_query = format!(
        "SELECT log_id, employee_id, log_dtime, add_by, add_dtime, \
         insert_dtr_log_pic, hr_approval, dtr_type, remarks \
         FROM tbl_att_logs \
         WHERE 1=1 {} \
         ORDER BY log_id",
        last_sync_clause
    );

    // Fetch logs from SQL Server and MySQL - parallel fetch
    let fetch_sql_spinner = create_spinner("Fetching logs from SQL Server...");
    let fetch_mysql_spinner = create_spinner("Fetching logs from MySQL...");
    
    // Run both fetch operations in parallel
    let sql_logs_future = fetch_sql_server_logs(sql_client, &sql_query);
    let mysql_logs_future = fetch_mysql_logs(mysql_conn, &mysql_query);
    
    // Execute both fetch operations concurrently
    let (sql_logs_result, mysql_logs_result) = try_join(
        async {
            let logs = sql_logs_future.await?;
            fetch_sql_spinner.finish_with_message(format!(
                "✅ Fetched {} SQL Server logs", logs.len()
            ));
            Ok::<_, anyhow::Error>(logs)
        },
        async {
            let logs = mysql_logs_future.await?;
            fetch_mysql_spinner.finish_with_message(format!(
                "✅ Fetched {} MySQL logs", logs.len()
            ));
            Ok::<_, anyhow::Error>(logs)
        }
    ).await?;
    
    let sql_logs = sql_logs_result;
    let mysql_logs = mysql_logs_result;
    
    // Double-check for MySQL records by querying a count of all records
    let all_mysql_count = get_mysql_record_count(mysql_conn).await?;
    if all_mysql_count > 0 && mysql_logs.is_empty() {
        println!("{}", format!("⚠️  Warning: Found {} records in MySQL but none matched sync criteria", all_mysql_count).yellow());
    }

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
            format!("{} records already in sync - skipping these", identical_records).yellow(),
            "↩".cyan()
        );
    }

    // Display sync summary
    fetch_spinner.finish_with_message(format!(
        "✅ Analysis complete: {} SQL Server logs, {} MySQL logs",
        sql_logs.len().to_string().blue(),
        mysql_logs.len().to_string().blue()
    ));
    
    // Show what needs to be synced
    println!("Sync needed:");
    println!("  → SQL Server to MySQL: {} new, {} updates", 
        sql_only_ids.len().to_string().yellow(),
        need_update_in_mysql.len().to_string().yellow()
    );
    println!("  → MySQL to SQL Server: {} new, {} updates", 
        mysql_only_ids.len().to_string().yellow(),
        need_update_in_sql.len().to_string().yellow()
    );

    // Early return if no updates needed
    if sql_logs.is_empty() && mysql_logs.is_empty() {
        println!("{}", "⚡ No changes detected, both databases are in sync!".green());
        return Ok(());
    }
    
    // Early return if all records exist in both databases and are identical
    if sql_only_ids.is_empty() && mysql_only_ids.is_empty() && 
       need_update_in_mysql.is_empty() && need_update_in_sql.is_empty() {
        println!("{}", "✓ All existing records are identical - no synchronization needed!".green());
        return Ok(());
    }

    // Current timestamp for sync_datetime
    let sync_timestamp = Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
    
    // Mark existing identical records as synced
    update_identical_records(
        sql_client, 
        mysql_conn, 
        &sql_logs, 
        &mysql_logs, 
        &common_ids, 
        &sync_timestamp
    ).await?;
    
    // Progress bar for sync
    let total_operations = sql_only_ids.len() + mysql_only_ids.len() + need_update_in_mysql.len() + need_update_in_sql.len();
    
    // If no operations to perform, exit early
    if total_operations == 0 {
        return Ok(());
    }
    
    let sync_progress = create_progress_bar(total_operations as u64, "Synchronizing databases...");

    // 3. Perform batch operations for better performance
    // Sync SQL → MySQL (inserts and updates)
    let sql_to_mysql_result = batch_sync_sql_to_mysql(
        mysql_conn,
        &sql_logs,
        &sql_only_ids,
        &need_update_in_mysql,
        &sync_timestamp,
        &sync_progress,
    ).await?;

    // Sync MySQL → SQL (inserts and updates)
    let mysql_to_sql_result = batch_sync_mysql_to_sql(
        sql_client,
        &mysql_logs,
        &mysql_only_ids,
        &need_update_in_sql,
        &sync_timestamp,
        &sync_progress,
    ).await?;

    sync_progress.finish_with_message(format!(
        "✅ Sync Complete! \n  SQL Server → MySQL: {} \n  MySQL → SQL Server: {}",
        format!("Inserted {}, Updated {}", sql_to_mysql_result.0, sql_to_mysql_result.1).green(),
        format!("Inserted {}, Updated {}", mysql_to_sql_result.0, mysql_to_sql_result.1).green()
    ));

    Ok(())
}

// Helper function to mark identical records as synced
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
            "Marking {} identical records as synced...", 
            identical_ids.len()
        ));
        
        // Update SQL Server
        for chunk in identical_ids.chunks(chunk_size) {
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
            }
        }
        
        // Update MySQL
        let mut transaction = mysql_conn.start_transaction(Default::default()).await?;
        
        for chunk in identical_ids.chunks(chunk_size) {
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
            }
        }
        
        transaction.commit().await?;
        update_spinner.finish_with_message(format!(
            "✓ Marked {} identical records as synced", identical_ids.len()
        ));
    }
    
    Ok(())
}