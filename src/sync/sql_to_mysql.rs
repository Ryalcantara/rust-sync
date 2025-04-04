// Corrected src/sync/sql_to_mysql.rs file

use anyhow::Result;
use indicatif::ProgressBar;
use std::collections::HashMap;
use mysql_async::prelude::*;
use crate::models::AttendanceLog;
use crate::models::SchedulingRecord;

// Batch synchronize from SQL Server to MySQL
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
    
    // 1. Handle inserts in batches
    let mut inserted = 0;
    
    if !ids_to_insert.is_empty() {
        progress_bar.set_message(format!("Inserting {} SQL records to MySQL...", ids_to_insert.len()));
        
        // Process in chunks for better performance
        for chunk in ids_to_insert.chunks(BATCH_SIZE) {
            if chunk.is_empty() {
                continue;
            }
            
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
            
            inserted += chunk.len() as u32;
            progress_bar.inc(chunk.len() as u64);
        }
    }

    // 2. Handle updates in batches too
    let mut updated = 0;
    
    if !ids_to_update.is_empty() {
        progress_bar.set_message(format!("Updating {} SQL records in MySQL...", ids_to_update.len()));
        
        // Batch updates for efficiency
        for chunk in ids_to_update.chunks(BATCH_SIZE) {
            if chunk.is_empty() {
                continue;
            }
            
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
            
            updated += chunk.len() as u32;
            progress_bar.inc(chunk.len() as u64);
        }
    }

    // Commit the transaction
    transaction.commit().await?;
    
    Ok((inserted, updated))
}

// Batch synchronize scheduling records from SQL Server to MySQL
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
    
    // 1. Handle inserts in batches
    let mut inserted = 0;
    
    if !ids_to_insert.is_empty() {
        progress_bar.set_message(format!("Inserting {} SQL scheduling records to MySQL...", ids_to_insert.len()));
        
        // Process in chunks for better performance
        for chunk in ids_to_insert.chunks(BATCH_SIZE) {
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
        }
    }

    // 2. Handle updates in batches too
    let mut updated = 0;
    
    if !ids_to_update.is_empty() {
        progress_bar.set_message(format!("Updating {} SQL scheduling records in MySQL...", ids_to_update.len()));
        
        // Batch updates for efficiency
        for chunk in ids_to_update.chunks(BATCH_SIZE) {
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
        }
    }

    // Commit the transaction
    transaction.commit().await?;
    
    Ok((inserted, updated))
}