// Optimized src/sync/sql_to_mysql.rs for faster performance

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

// Optimized batch synchronize from SQL Server to MySQL with enhanced visual feedback
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
    
    // OPTIMIZATION: Increased batch size for better performance
    const BATCH_SIZE: usize = 300; // Was 200, increased for better throughput
    
    // Metrics for real-time reporting
    let start_time = Instant::now();
    
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
            
            // OPTIMIZATION: Use extended INSERT VALUES instead of REPLACE for better performance
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
            
            // OPTIMIZATION: Use INSERT ... ON DUPLICATE KEY UPDATE for better performance than REPLACE
            let stmt = format!(
                "INSERT INTO tbl_att_logs 
                (log_id, employee_id, log_dtime, add_by, add_dtime, 
                 insert_dtr_log_pic, hr_approval, dtr_type, remarks, sync_status, sync_datetime) 
                VALUES {}
                ON DUPLICATE KEY UPDATE
                 employee_id = VALUES(employee_id),
                 log_dtime = VALUES(log_dtime),
                 add_by = VALUES(add_by),
                 add_dtime = VALUES(add_dtime),
                 insert_dtr_log_pic = VALUES(insert_dtr_log_pic),
                 hr_approval = VALUES(hr_approval),
                 dtr_type = VALUES(dtr_type),
                 remarks = VALUES(remarks),
                 sync_status = VALUES(sync_status),
                 sync_datetime = VALUES(sync_datetime)",
                placeholders.join(",")
            );
            
            transaction.exec_drop(stmt, params).await?;
            
            let batch_elapsed = batch_start.elapsed();
            
            inserted += chunk.len() as u32;
            progress_bar.inc(chunk.len() as u64);
            
            // OPTIMIZATION: Reduce progress message updates for better performance
            if (chunk_idx + 1) % 5 == 0 || (chunk_idx + 1) == ids_to_insert.len().div_ceil(BATCH_SIZE) {
                let elapsed = start_time.elapsed();
                let transfer_speed = format_transfer_speed(inserted, elapsed.as_secs_f64());
                let batch_speed = format_transfer_speed(chunk.len() as u32, batch_elapsed.as_secs_f64());
                
                progress_bar.set_message(format!("🔄 {} → MySQL (INSERT) | Batch {}/{} | {} | {} | Last: {}",
                    "SQL Server".blue(),
                    chunk_idx + 1, 
                    ids_to_insert.len().div_ceil(BATCH_SIZE),
                    format!("{}/{} records", inserted, ids_to_insert.len()).yellow(),
                    transfer_speed.green(),
                    batch_speed.cyan()
                ));
            }
        }
    }

    // 2. Handle updates in batches too - though we use INSERT ON DUPLICATE KEY UPDATE above
    let mut updated = 0;
    
    if !ids_to_update.is_empty() {
        progress_bar.set_message(format!("🔄 {} → MySQL (UPDATE)", "SQL Server".blue()));
        let update_start = Instant::now();
        
        // OPTIMIZATION: Use the same approach for updates - with INSERT ON DUPLICATE KEY UPDATE
        for (chunk_idx, chunk) in ids_to_update.chunks(BATCH_SIZE).enumerate() {
            if chunk.is_empty() {
                continue;
            }
            
            let batch_start = Instant::now();
            
            // Build batch update with multiple VALUES clauses
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
            
            // Same statement as for inserts, but with a different semantic meaning
            let stmt = format!(
                "INSERT INTO tbl_att_logs 
                (log_id, employee_id, log_dtime, add_by, add_dtime, 
                 insert_dtr_log_pic, hr_approval, dtr_type, remarks, sync_status, sync_datetime) 
                VALUES {}
                ON DUPLICATE KEY UPDATE
                 employee_id = VALUES(employee_id),
                 log_dtime = VALUES(log_dtime),
                 add_by = VALUES(add_by),
                 add_dtime = VALUES(add_dtime),
                 insert_dtr_log_pic = VALUES(insert_dtr_log_pic),
                 hr_approval = VALUES(hr_approval),
                 dtr_type = VALUES(dtr_type),
                 remarks = VALUES(remarks),
                 sync_status = VALUES(sync_status),
                 sync_datetime = VALUES(sync_datetime)",
                placeholders.join(",")
            );
            
            transaction.exec_drop(stmt, params).await?;
            
            let batch_elapsed = batch_start.elapsed();
            
            updated += chunk.len() as u32;
            progress_bar.inc(chunk.len() as u64);
            
            // OPTIMIZATION: Reduce progress message updates for better performance
            if (chunk_idx + 1) % 5 == 0 || (chunk_idx + 1) == ids_to_update.len().div_ceil(BATCH_SIZE) {
                let update_elapsed = update_start.elapsed();
                let transfer_speed = format_transfer_speed(updated, update_elapsed.as_secs_f64());
                let batch_speed = format_transfer_speed(chunk.len() as u32, batch_elapsed.as_secs_f64());
                
                progress_bar.set_message(format!("🔄 {} → MySQL (UPDATE) | Batch {}/{} | {} | {} | Last: {}",
                    "SQL Server".blue(),
                    chunk_idx + 1, 
                    ids_to_update.len().div_ceil(BATCH_SIZE),
                    format!("{}/{} records", updated, ids_to_update.len()).yellow(),
                    transfer_speed.green(),
                    batch_speed.cyan()
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

// Optimized batch synchronize scheduling records from SQL Server to MySQL with enhanced visuals
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
    
    // OPTIMIZATION: Increased batch size for better performance
    const BATCH_SIZE: usize = 300; // Increased from 200
    
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
            
            let batch_start = Instant::now();
            
            // OPTIMIZATION: Process fields in smaller groups to avoid MySQL parameter limits
            // First batch - primary fields
            let mut placeholders1 = Vec::new();
            let mut params1: Vec<mysql_async::Value> = Vec::new();
            
            for &id in chunk {
                let record = &sql_records[&id];
                
                // Add placeholders for this record's primary fields
                placeholders1.push("(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
                
                // Add primary field values for this record
                params1.push(record.scheduling_id.into());
                params1.push(record.date_start.clone().into());
                params1.push(record.date_end.clone().into());
                params1.push(record.remarks.clone().into());
                params1.push(record.station.clone().into());
                params1.push(record.employee_id.clone().into());
                params1.push(record.department.clone().into());
                params1.push(record.time_start.clone().into());
                params1.push(record.time_end.clone().into());
                params1.push("SYNCED".to_string().into());
                params1.push(sync_timestamp.to_string().into());
            }
            
            // OPTIMIZATION: Use INSERT ... ON DUPLICATE KEY UPDATE for better performance
            let stmt1 = format!(
                "INSERT INTO tbl_scheduling 
                (scheduling_id, date_start, date_end, remarks, station, 
                 employee_id, department, time_start, time_end, 
                 sync_status, sync_datetime) 
                VALUES {}
                ON DUPLICATE KEY UPDATE
                 date_start = VALUES(date_start),
                 date_end = VALUES(date_end),
                 remarks = VALUES(remarks),
                 station = VALUES(station),
                 employee_id = VALUES(employee_id),
                 department = VALUES(department),
                 time_start = VALUES(time_start),
                 time_end = VALUES(time_end),
                 sync_status = VALUES(sync_status),
                 sync_datetime = VALUES(sync_datetime)",
                placeholders1.join(",")
            );
            
            transaction.exec_drop(stmt1, params1).await?;
            
            // Second batch - remaining fields via UPDATE
            // This avoids parameter limit issues by updating a different set of fields
            for &id in chunk {
                let record = &sql_records[&id];
                
                let stmt2 = "UPDATE tbl_scheduling SET
                    updated_at = ?,
                    created_at = ?,
                    `case` = ?,
                    remark_2nd = ?,
                    display_order = ?,
                    display_order_2nd = ?
                    WHERE scheduling_id = ?";
                
                transaction.exec_drop(stmt2, (
                    &record.updated_at,
                    &record.created_at,
                    &record.case,
                    &record.remark_2nd,
                    &record.display_order,
                    &record.display_order_2nd,
                    record.scheduling_id,
                )).await?;
            }
            
            let batch_elapsed = batch_start.elapsed();
            
            inserted += chunk.len() as u32;
            progress_bar.inc(chunk.len() as u64);
            
            // OPTIMIZATION: Reduce progress message updates for better performance
            if (chunk_idx + 1) % 5 == 0 || (chunk_idx + 1) == ids_to_insert.len().div_ceil(BATCH_SIZE) {
                let elapsed = start_time.elapsed();
                let transfer_speed = format_transfer_speed(inserted, elapsed.as_secs_f64());
                let batch_speed = format_transfer_speed(chunk.len() as u32, batch_elapsed.as_secs_f64());
                
                progress_bar.set_message(format!("🔄 {} → MySQL (Scheduling INSERT) | Batch {}/{} | {} | {} | Last: {}",
                    "SQL Server".blue(),
                    chunk_idx + 1, 
                    ids_to_insert.len().div_ceil(BATCH_SIZE),
                    format!("{}/{} records", inserted, ids_to_insert.len()).yellow(),
                    transfer_speed.green(),
                    batch_speed.cyan()
                ));
            }
        }
    }

    // 2. Handle updates in batches too
    let mut updated = 0;
    let update_start = Instant::now();
    
    if !ids_to_update.is_empty() {
        progress_bar.set_message(format!("🔄 {} → MySQL (Scheduling UPDATE)", "SQL Server".blue()));
        
        // Batch updates - using the same approach as for inserts
        for (chunk_idx, chunk) in ids_to_update.chunks(BATCH_SIZE).enumerate() {
            if chunk.is_empty() {
                continue;
            }
            
            let batch_start = Instant::now();
            
            // OPTIMIZATION: Process fields in smaller groups to avoid MySQL parameter limits
            // First batch - primary fields
            let mut placeholders1 = Vec::new();
            let mut params1: Vec<mysql_async::Value> = Vec::new();
            
            for &id in chunk {
                let record = &sql_records[&id];
                
                // Add placeholders for this record's primary fields
                placeholders1.push("(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
                
                // Add primary field values for this record
                params1.push(record.scheduling_id.into());
                params1.push(record.date_start.clone().into());
                params1.push(record.date_end.clone().into());
                params1.push(record.remarks.clone().into());
                params1.push(record.station.clone().into());
                params1.push(record.employee_id.clone().into());
                params1.push(record.department.clone().into());
                params1.push(record.time_start.clone().into());
                params1.push(record.time_end.clone().into());
                params1.push("SYNCED".to_string().into());
                params1.push(sync_timestamp.to_string().into());
            }
            
            // OPTIMIZATION: Use INSERT ... ON DUPLICATE KEY UPDATE for better performance
            let stmt1 = format!(
                "INSERT INTO tbl_scheduling 
                (scheduling_id, date_start, date_end, remarks, station, 
                 employee_id, department, time_start, time_end, 
                 sync_status, sync_datetime) 
                VALUES {}
                ON DUPLICATE KEY UPDATE
                 date_start = VALUES(date_start),
                 date_end = VALUES(date_end),
                 remarks = VALUES(remarks),
                 station = VALUES(station),
                 employee_id = VALUES(employee_id),
                 department = VALUES(department),
                 time_start = VALUES(time_start),
                 time_end = VALUES(time_end),
                 sync_status = VALUES(sync_status),
                 sync_datetime = VALUES(sync_datetime)",
                placeholders1.join(",")
            );
            
            transaction.exec_drop(stmt1, params1).await?;
            
            // Second batch - remaining fields via UPDATE
            // This avoids parameter limit issues by updating a different set of fields
            for &id in chunk {
                let record = &sql_records[&id];
                
                let stmt2 = "UPDATE tbl_scheduling SET
                    updated_at = ?,
                    created_at = ?,
                    `case` = ?,
                    remark_2nd = ?,
                    display_order = ?,
                    display_order_2nd = ?
                    WHERE scheduling_id = ?";
                
                transaction.exec_drop(stmt2, (
                    &record.updated_at,
                    &record.created_at,
                    &record.case,
                    &record.remark_2nd,
                    &record.display_order,
                    &record.display_order_2nd,
                    record.scheduling_id,
                )).await?;
            }
            
            let batch_elapsed = batch_start.elapsed();
            
            updated += chunk.len() as u32;
            progress_bar.inc(chunk.len() as u64);
            
            // OPTIMIZATION: Reduce progress message updates for better performance
            if (chunk_idx + 1) % 5 == 0 || (chunk_idx + 1) == ids_to_update.len().div_ceil(BATCH_SIZE) {
                let update_elapsed = update_start.elapsed();
                let transfer_speed = format_transfer_speed(updated, update_elapsed.as_secs_f64());
                let batch_speed = format_transfer_speed(chunk.len() as u32, batch_elapsed.as_secs_f64());
                
                progress_bar.set_message(format!("🔄 {} → MySQL (Scheduling UPDATE) | Batch {}/{} | {} | {} | Last: {}",
                    "SQL Server".blue(),
                    chunk_idx + 1, 
                    ids_to_update.len().div_ceil(BATCH_SIZE),
                    format!("{}/{} records", updated, ids_to_update.len()).yellow(),
                    transfer_speed.green(),
                    batch_speed.cyan()
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