// Enhanced src/sync/sql_to_mysql.rs file with real-time transfer visualization

use anyhow::Result;
use indicatif::ProgressBar;
use std::collections::HashMap;
use mysql_async::prelude::*;
use colored::*;
use std::time::{Instant, Duration};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use crate::models::AttendanceLog;
use crate::models::SchedulingRecord;

// Helper to format transfer speed
fn format_transfer_speed(records: u32, elapsed_secs: f64) -> String {
    if elapsed_secs < 0.001 {
        return "N/A".to_string();
    }
    let records_per_sec = records as f64 / elapsed_secs;
    if records_per_sec >= 1000.0 {
        format!("{:.2} K records/s", records_per_sec / 1000.0)
    } else {
        format!("{:.2} records/s", records_per_sec)
    }
}

// Batch synchronize from SQL Server to MySQL with enhanced visual feedback
pub async fn batch_sync_sql_to_mysql(
    mysql_conn: &mut mysql_async::Conn,
    sql_logs: &HashMap<i32, AttendanceLog>,
    ids_to_insert: &[i32],
    ids_to_update: &[i32],
    sync_timestamp: &str,
    progress_bar: &ProgressBar,
) -> Result<(u32, u32)> {
    // Start a transaction for atomicity
    let mut transaction = mysql_conn.start_transaction(Default::default()).await?;
    
    // Settings for batch operations
    const BATCH_SIZE: usize = 200;
    
    // Metrics for real-time reporting
    let start_time = Instant::now();
    let total_time_insert = Arc::new(std::sync::Mutex::new(Duration::from_secs(0)));
    let total_time_update = Arc::new(std::sync::Mutex::new(Duration::from_secs(0)));
    
    // 1. Handle inserts in batches
    let mut inserted = 0;
    
    if !ids_to_insert.is_empty() {
        progress_bar.set_message(format!("🔄 {} → MySQL (INSERT)", "SQL Server".blue()));
        
        // Process in chunks for better performance
        for (chunk_idx, chunk) in ids_to_insert.chunks(BATCH_SIZE).enumerate() {
            if chunk.is_empty() {
                continue;
            }
            
            let batch_start = Instant::now();
            
            // Build batch insert with multiple VALUES clauses
            let mut placeholders = Vec::new();
            let mut params: Vec<mysql_async::Value> = Vec::new();
            
            for &id in chunk {
                let log = &sql_logs[&id];
                
                // Add placeholders for this record
                placeholders.push("(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
                
                // Add all values for this record
                params.push(log.log_id.into());
                params.push(log.employee_id.clone().into());
                params.push(log.log_dtime.clone().into());
                params.push(log.add_by.into());
                params.push(log.add_dtime.clone().into());
                params.push(log.insert_dtr_log_pic.clone().into());
                params.push(log.hr_approval.clone().into());
                params.push(log.dtr_type.clone().into());
                params.push(log.remarks.clone().into());
                // Always mark as fully synced when syncing
                params.push("SYNCED".to_string().into());
                params.push(sync_timestamp.to_string().into());
            }
            
            // Execute batch insert
            let stmt = format!(
                "REPLACE INTO tbl_att_logs 
                (log_id, employee_id, log_dtime, add_by, add_dtime, 
                 insert_dtr_log_pic, hr_approval, dtr_type, remarks, sync_status, sync_datetime) 
                VALUES {}",
                placeholders.join(",")
            );
            
            transaction.exec_drop(stmt, params).await?;
            
            let batch_elapsed = batch_start.elapsed();
            *total_time_insert.lock().unwrap() += batch_elapsed;
            
            inserted += chunk.len() as u32;
            progress_bar.inc(chunk.len() as u64);
            
            if (chunk_idx + 1) % 5 == 0 || (chunk_idx + 1) == ids_to_insert.len().div_ceil(BATCH_SIZE) {
                let elapsed = start_time.elapsed();
                let transfer_speed = format_transfer_speed(inserted, elapsed.as_secs_f64());
                progress_bar.set_message(format!("🔄 {} → MySQL (INSERT) | Batch {}/{} | {} | {}",
                    "SQL Server".blue(),
                    chunk_idx + 1, 
                    ids_to_insert.len().div_ceil(BATCH_SIZE),
                    format!("{}/{} records", inserted, ids_to_insert.len()).yellow(),
                    transfer_speed.green()
                ));
            }
        }
    }

    // 2. Handle updates in batches too
    let mut updated = 0;
    
    if !ids_to_update.is_empty() {
        progress_bar.set_message(format!("🔄 {} → MySQL (UPDATE)", "SQL Server".blue()));
        let update_start = Instant::now();
        
        // Batch updates for efficiency
        for (chunk_idx, chunk) in ids_to_update.chunks(BATCH_SIZE).enumerate() {
            if chunk.is_empty() {
                continue;
            }
            
            let batch_start = Instant::now();
            
            // For MySQL, we need individual updates for now
            for &id in chunk {
                let log = &sql_logs[&id];
                
                let stmt = "UPDATE tbl_att_logs 
                           SET employee_id = ?, log_dtime = ?, add_by = ?, add_dtime = ?, 
                               insert_dtr_log_pic = ?, hr_approval = ?, dtr_type = ?, remarks = ?,
                               sync_status = ?, sync_datetime = ?
                           WHERE log_id = ?";
                
                transaction.exec_drop(
                    stmt,
                    (
                        &log.employee_id,
                        &log.log_dtime,
                        log.add_by,
                        &log.add_dtime,
                        &log.insert_dtr_log_pic,
                        &log.hr_approval,
                        &log.dtr_type,
                        &log.remarks,
                        "SYNCED",
                        sync_timestamp,
                        log.log_id,
                    ),
                ).await?;
            }
            
            let batch_elapsed = batch_start.elapsed();
            *total_time_update.lock().unwrap() += batch_elapsed;
            
            updated += chunk.len() as u32;
            progress_bar.inc(chunk.len() as u64);
            
            if (chunk_idx + 1) % 5 == 0 || (chunk_idx + 1) == ids_to_update.len().div_ceil(BATCH_SIZE) {
                let update_elapsed = update_start.elapsed();
                let transfer_speed = format_transfer_speed(updated, update_elapsed.as_secs_f64());
                progress_bar.set_message(format!("🔄 {} → MySQL (UPDATE) | Batch {}/{} | {} | {}",
                    "SQL Server".blue(),
                    chunk_idx + 1, 
                    ids_to_update.len().div_ceil(BATCH_SIZE),
                    format!("{}/{} records", updated, ids_to_update.len()).yellow(),
                    transfer_speed.green()
                ));
            }
        }
    }

    // Commit the transaction
    transaction.commit().await?;
    
    // Final statistics
    let total_elapsed = start_time.elapsed();
    let total_transfers = inserted + updated;
    let avg_speed = format_transfer_speed(total_transfers, total_elapsed.as_secs_f64());
    
    progress_bar.set_message(format!("✅ SQL → MySQL | {} inserted, {} updated | Avg speed: {}",
        inserted.to_string().green(),
        updated.to_string().green(),
        avg_speed.bright_blue()
    ));
    
    Ok((inserted, updated))
}

// Batch synchronize scheduling records from SQL Server to MySQL with enhanced visuals
pub async fn batch_sync_scheduling_sql_to_mysql(
    mysql_conn: &mut mysql_async::Conn,
    sql_records: &HashMap<i32, SchedulingRecord>,
    ids_to_insert: &[i32],
    ids_to_update: &[i32],
    sync_timestamp: &str,
    progress_bar: &ProgressBar,
) -> Result<(u32, u32)> {
    // Start a transaction for atomicity
    let mut transaction = mysql_conn.start_transaction(Default::default()).await?;
    
    // Settings for batch operations
    const BATCH_SIZE: usize = 200;
    
    // Metrics for real-time reporting
    let start_time = Instant::now();
    
    // 1. Handle inserts in batches
    let mut inserted = 0;
    
    if !ids_to_insert.is_empty() {
        progress_bar.set_message(format!("🔄 {} → MySQL (Scheduling INSERT)", "SQL Server".blue()));
        
        // Process in chunks for better performance
        for (chunk_idx, chunk) in ids_to_insert.chunks(BATCH_SIZE).enumerate() {
            if chunk.is_empty() {
                continue;
            }
            
            // For each record in this chunk, create an individual insert
            for &id in chunk {
                let record = &sql_records[&id];
                
                // Break the insert into smaller chunks to avoid parameter limits
                // First chunk
                let stmt1 = "INSERT INTO tbl_scheduling 
                    (scheduling_id, date_start, date_end, remarks, station, employee_id) 
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON DUPLICATE KEY UPDATE 
                    date_start = VALUES(date_start), 
                    date_end = VALUES(date_end),
                    remarks = VALUES(remarks),
                    station = VALUES(station),
                    employee_id = VALUES(employee_id)";
                
                transaction.exec_drop(stmt1, (
                    record.scheduling_id,
                    &record.date_start,
                    &record.date_end,
                    &record.remarks,
                    &record.station,
                    &record.employee_id,
                )).await?;
                
                // Second chunk
                let stmt2 = "UPDATE tbl_scheduling SET
                    department = ?,
                    time_start = ?,
                    time_end = ?,
                    updated_at = ?,
                    created_at = ?,
                    `case` = ?
                    WHERE scheduling_id = ?";
                
                transaction.exec_drop(stmt2, (
                    &record.department,
                    &record.time_start,
                    &record.time_end,
                    &record.updated_at,
                    &record.created_at,
                    &record.case,
                    record.scheduling_id,
                )).await?;
                
                // Third chunk
                let stmt3 = "UPDATE tbl_scheduling SET
                    remark_2nd = ?,
                    display_order = ?,
                    display_order_2nd = ?,
                    sync_status = ?,
                    sync_datetime = ?
                    WHERE scheduling_id = ?";
                
                transaction.exec_drop(stmt3, (
                    &record.remark_2nd,
                    &record.display_order,
                    &record.display_order_2nd,
                    "SYNCED",
                    sync_timestamp,
                    record.scheduling_id,
                )).await?;
            }
            
            inserted += chunk.len() as u32;
            progress_bar.inc(chunk.len() as u64);
            
            if (chunk_idx + 1) % 5 == 0 || (chunk_idx + 1) == ids_to_insert.len().div_ceil(BATCH_SIZE) {
                let elapsed = start_time.elapsed();
                let transfer_speed = format_transfer_speed(inserted, elapsed.as_secs_f64());
                progress_bar.set_message(format!("🔄 {} → MySQL (Scheduling INSERT) | Batch {}/{} | {} | {}",
                    "SQL Server".blue(),
                    chunk_idx + 1, 
                    ids_to_insert.len().div_ceil(BATCH_SIZE),
                    format!("{}/{} records", inserted, ids_to_insert.len()).yellow(),
                    transfer_speed.green()
                ));
            }
        }
    }

    // 2. Handle updates in batches too
    let mut updated = 0;
    let update_start = Instant::now();
    
    if !ids_to_update.is_empty() {
        progress_bar.set_message(format!("🔄 {} → MySQL (Scheduling UPDATE)", "SQL Server".blue()));
        
        // Batch updates for efficiency
        for (chunk_idx, chunk) in ids_to_update.chunks(BATCH_SIZE).enumerate() {
            if chunk.is_empty() {
                continue;
            }
            
            // For MySQL, we need individual updates for now
            for &id in chunk {
                let record = &sql_records[&id];
                
                // Break down the parameters into smaller groupings to avoid exceeding MySQL's parameter limit
                // First update the main fields
                let stmt1 = "UPDATE tbl_scheduling 
                           SET date_start = ?, date_end = ?, remarks = ?, station = ?, 
                               employee_id = ?, department = ?, time_start = ?, time_end = ?
                           WHERE scheduling_id = ?";
                
                transaction.exec_drop(
                    stmt1,
                    (
                        &record.date_start,
                        &record.date_end,
                        &record.remarks,
                        &record.station,
                        &record.employee_id,
                        &record.department,
                        &record.time_start,
                        &record.time_end,
                        record.scheduling_id,
                    ),
                ).await?;
                
                // Then update the remaining fields
                let stmt2 = "UPDATE tbl_scheduling 
                           SET updated_at = ?, created_at = ?, `case` = ?, remark_2nd = ?,
                               display_order = ?, display_order_2nd = ?,
                               sync_status = ?, sync_datetime = ?
                           WHERE scheduling_id = ?";
                
                transaction.exec_drop(
                    stmt2,
                    (
                        &record.updated_at,
                        &record.created_at,
                        &record.case,
                        &record.remark_2nd,
                        &record.display_order,
                        &record.display_order_2nd,
                        "SYNCED",
                        sync_timestamp,
                        record.scheduling_id,
                    ),
                ).await?;
            }
            
            updated += chunk.len() as u32;
            progress_bar.inc(chunk.len() as u64);
            
            if (chunk_idx + 1) % 5 == 0 || (chunk_idx + 1) == ids_to_update.len().div_ceil(BATCH_SIZE) {
                let update_elapsed = update_start.elapsed();
                let transfer_speed = format_transfer_speed(updated, update_elapsed.as_secs_f64());
                progress_bar.set_message(format!("🔄 {} → MySQL (Scheduling UPDATE) | Batch {}/{} | {} | {}",
                    "SQL Server".blue(),
                    chunk_idx + 1, 
                    ids_to_update.len().div_ceil(BATCH_SIZE),
                    format!("{}/{} records", updated, ids_to_update.len()).yellow(),
                    transfer_speed.green()
                ));
            }
        }
    }

    // Commit the transaction
    transaction.commit().await?;
    
    // Final statistics
    let total_elapsed = start_time.elapsed();
    let total_transfers = inserted + updated;
    let avg_speed = format_transfer_speed(total_transfers, total_elapsed.as_secs_f64());
    
    progress_bar.set_message(format!("✅ SQL → MySQL (Scheduling) | {} inserted, {} updated | Avg speed: {}",
        inserted.to_string().green(),
        updated.to_string().green(),
        avg_speed.bright_blue()
    ));
    
    Ok((inserted, updated))
}