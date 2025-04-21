// Fixed src/sync/mysql_to_sql.rs with corrected temp table handling

use anyhow::Result;
use indicatif::ProgressBar;
use std::collections::HashMap;
use std::time::{Instant, Duration};
use colored::*;
use crate::database::connection::SqlServerClient;
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

// Optimized batch synchronize from MySQL to SQL Server with enhanced visuals
pub async fn batch_sync_mysql_to_sql(
    sql_client: &mut SqlServerClient,
    mysql_logs: &HashMap<i32, AttendanceLog>,
    ids_to_insert: &[i32],
    ids_to_update: &[i32],
    sync_timestamp: &str,
    progress_bar: &ProgressBar,
) -> Result<(u32, u32)> {
    let mut inserted = 0;
    let mut updated = 0;
    
    // Metrics for real-time reporting
    let start_time = Instant::now();
    
    // OPTIMIZATION: Increased batch size for better performance
    const BATCH_SIZE: usize = 75; // Increased from 50
    
    if !ids_to_insert.is_empty() {
        progress_bar.set_message(format!("🔄 {} → SQL Server (INSERT)", "MySQL".green()));
        
        // Process in chunks for better performance
        for (chunk_idx, ids_chunk) in ids_to_insert.chunks(BATCH_SIZE).enumerate() {
            let batch_start = Instant::now();
            
            if !ids_chunk.is_empty() {
                // FIX: Process each record directly instead of using temp tables
                for &id in ids_chunk {
                    let log = &mysql_logs[&id];
                    
                    // Format optional values properly for SQL
                    let insert_dtr_log_pic_sql = match &log.insert_dtr_log_pic {
                        Some(val) => format!("'{}'", val.replace("'", "''")),
                        None => "NULL".to_string()
                    };
                    
                    let hr_approval_sql = match &log.hr_approval {
                        Some(val) => format!("'{}'", val.replace("'", "''")),
                        None => "NULL".to_string()
                    };
                    
                    let dtr_type_sql = match &log.dtr_type {
                        Some(val) => format!("'{}'", val.replace("'", "''")),
                        None => "NULL".to_string()
                    };
                    
                    let remarks_sql = match &log.remarks {
                        Some(val) => format!("'{}'", val.replace("'", "''")),
                        None => "NULL".to_string()
                    };
                    
                    // Use a direct approach with SET IDENTITY_INSERT
                    let sql = format!(
                        "SET IDENTITY_INSERT [dbo].[tbl_att_logs] ON;
                         
                         MERGE INTO [dbo].[tbl_att_logs] AS target
                         USING (SELECT {} AS log_id) AS source
                         ON target.log_id = source.log_id
                         WHEN MATCHED THEN
                            UPDATE SET
                                employee_id = '{}',
                                log_dtime = '{}',
                                add_by = {},
                                add_dtime = '{}',
                                insert_dtr_log_pic = {},
                                hr_approval = {},
                                dtr_type = {},
                                remarks = {},
                                sync_status = 'SYNCED',
                                sync_datetime = '{}'
                         WHEN NOT MATCHED THEN
                            INSERT (
                                log_id, employee_id, log_dtime, add_by, add_dtime,
                                insert_dtr_log_pic, hr_approval, dtr_type, remarks,
                                sync_status, sync_datetime
                            )
                            VALUES (
                                {}, '{}', '{}', {}, '{}',
                                {}, {}, {}, {},
                                'SYNCED', '{}'
                            );
                         
                         SET IDENTITY_INSERT [dbo].[tbl_att_logs] OFF;",
                        log.log_id,
                        log.employee_id.replace("'", "''"),
                        log.log_dtime.replace("'", "''"),
                        log.add_by,
                        log.add_dtime.replace("'", "''"),
                        insert_dtr_log_pic_sql,
                        hr_approval_sql,
                        dtr_type_sql,
                        remarks_sql,
                        sync_timestamp,
                        log.log_id,
                        log.employee_id.replace("'", "''"),
                        log.log_dtime.replace("'", "''"),
                        log.add_by,
                        log.add_dtime.replace("'", "''"),
                        insert_dtr_log_pic_sql,
                        hr_approval_sql,
                        dtr_type_sql,
                        remarks_sql,
                        sync_timestamp
                    );
                    
                    sql_client.execute(&sql, &[]).await?;
                }
                
                inserted += ids_chunk.len() as u32;
                progress_bar.inc(ids_chunk.len() as u64);
            }
            
            let batch_elapsed = batch_start.elapsed();
            let batch_speed = format_transfer_speed(ids_chunk.len() as u32, batch_elapsed.as_secs_f64());
            
            // Update progress message with detailed information
            if (chunk_idx + 1) % 2 == 0 || (chunk_idx + 1) == ids_to_insert.len().div_ceil(BATCH_SIZE) {
                let elapsed = start_time.elapsed();
                let overall_speed = format_transfer_speed(inserted, elapsed.as_secs_f64());
                
                progress_bar.set_message(format!(
                    "🔄 {} → SQL Server (INSERT) | Batch {}/{} | {} | {} | Last batch: {}",
                    "MySQL".green(),
                    chunk_idx + 1,
                    ids_to_insert.len().div_ceil(BATCH_SIZE),
                    format!("{}/{} records", inserted, ids_to_insert.len()).yellow(),
                    overall_speed.bright_blue(),
                    batch_speed.cyan()
                ));
            }
        }
    }

    // Handle updates - already included in the MERGE statement above
    if !ids_to_update.is_empty() {
        let update_start = Instant::now();
        progress_bar.set_message(format!("🔄 {} → SQL Server (UPDATE)", "MySQL".green()));
        
        // Process updates directly with MERGE - same approach as inserts above
        for (chunk_idx, ids_chunk) in ids_to_update.chunks(BATCH_SIZE).enumerate() {
            let batch_start = Instant::now();
            
            if !ids_chunk.is_empty() {
                // Use the same direct approach with MERGE
                for &id in ids_chunk {
                    let log = &mysql_logs[&id];
                    
                    // Format optional values properly for SQL
                    let insert_dtr_log_pic_sql = match &log.insert_dtr_log_pic {
                        Some(val) => format!("'{}'", val.replace("'", "''")),
                        None => "NULL".to_string()
                    };
                    
                    let hr_approval_sql = match &log.hr_approval {
                        Some(val) => format!("'{}'", val.replace("'", "''")),
                        None => "NULL".to_string()
                    };
                    
                    let dtr_type_sql = match &log.dtr_type {
                        Some(val) => format!("'{}'", val.replace("'", "''")),
                        None => "NULL".to_string()
                    };
                    
                    let remarks_sql = match &log.remarks {
                        Some(val) => format!("'{}'", val.replace("'", "''")),
                        None => "NULL".to_string()
                    };
                    
                    let sql = format!(
                        "MERGE INTO [dbo].[tbl_att_logs] AS target
                         USING (SELECT {} AS log_id) AS source
                         ON target.log_id = source.log_id
                         WHEN MATCHED THEN
                            UPDATE SET
                                employee_id = '{}',
                                log_dtime = '{}',
                                add_by = {},
                                add_dtime = '{}',
                                insert_dtr_log_pic = {},
                                hr_approval = {},
                                dtr_type = {},
                                remarks = {},
                                sync_status = 'SYNCED',
                                sync_datetime = '{}'
                         ;",
                        log.log_id,
                        log.employee_id.replace("'", "''"),
                        log.log_dtime.replace("'", "''"),
                        log.add_by,
                        log.add_dtime.replace("'", "''"),
                        insert_dtr_log_pic_sql,
                        hr_approval_sql,
                        dtr_type_sql,
                        remarks_sql,
                        sync_timestamp
                    );
                    
                    sql_client.execute(&sql, &[]).await?;
                }
                
                updated += ids_chunk.len() as u32;
                progress_bar.inc(ids_chunk.len() as u64);
            }
            
            let batch_elapsed = batch_start.elapsed();
            let batch_speed = format_transfer_speed(ids_chunk.len() as u32, batch_elapsed.as_secs_f64());
            
            // Update progress message with detailed information
            if (chunk_idx + 1) % 2 == 0 || (chunk_idx + 1) == ids_to_update.len().div_ceil(BATCH_SIZE) {
                let update_elapsed = update_start.elapsed();
                let overall_speed = format_transfer_speed(updated, update_elapsed.as_secs_f64());
                
                progress_bar.set_message(format!(
                    "🔄 {} → SQL Server (UPDATE) | Batch {}/{} | {} | {} | Last batch: {}",
                    "MySQL".green(),
                    chunk_idx + 1,
                    ids_to_update.len().div_ceil(BATCH_SIZE),
                    format!("{}/{} records", updated, ids_to_update.len()).yellow(),
                    overall_speed.bright_blue(),
                    batch_speed.cyan()
                ));
            }
        }
    }
    
    // Final statistics
    let total_elapsed = start_time.elapsed();
    let total_transfers = inserted + updated;
    let avg_speed = format_transfer_speed(total_transfers, total_elapsed.as_secs_f64());
    
    progress_bar.set_message(format!("✅ MySQL → SQL Server | {} inserted, {} updated | Avg speed: {}",
        inserted.to_string().green(),
        updated.to_string().green(),
        avg_speed.bright_blue()
    ));
    
    Ok((inserted, updated))
}

// Fixed batch synchronize scheduling records from MySQL to SQL Server with enhanced visuals
pub async fn batch_sync_scheduling_mysql_to_sql(
    sql_client: &mut SqlServerClient,
    mysql_records: &HashMap<i32, SchedulingRecord>,
    ids_to_insert: &[i32],
    ids_to_update: &[i32],
    sync_timestamp: &str,
    progress_bar: &ProgressBar,
) -> Result<(u32, u32)> {
    let mut inserted = 0;
    let mut updated = 0;
    
    // Metrics for real-time reporting
    let start_time = Instant::now();
    
    // OPTIMIZATION: Increased batch size for better performance
    const BATCH_SIZE: usize = 50; // Use a conservative batch size to ensure stability
    
    if !ids_to_insert.is_empty() {
        progress_bar.set_message(format!("🔄 {} → SQL Server (Scheduling INSERT)", "MySQL".green()));
        
        // Process each record individually for reliability
        for (chunk_idx, ids_chunk) in ids_to_insert.chunks(BATCH_SIZE).enumerate() {
            let batch_start = Instant::now();
            
            for &id in ids_chunk {
                let record = &mysql_records[&id];
                
                // Format optional values properly for SQL
                let remarks_sql = match &record.remarks {
                    Some(val) => format!("'{}'", val.replace("'", "''")),
                    None => "NULL".to_string()
                };
                
                let station_sql = match &record.station {
                    Some(val) => format!("'{}'", val.replace("'", "''")),
                    None => "NULL".to_string()
                };
                
                let case_sql = match &record.case {
                    Some(val) => format!("'{}'", val.replace("'", "''")),
                    None => "NULL".to_string()
                };
                
                let remark_2nd_sql = match &record.remark_2nd {
                    Some(val) => format!("'{}'", val.replace("'", "''")),
                    None => "NULL".to_string()
                };
                
                let display_order_sql = match &record.display_order {
                    Some(val) => format!("'{}'", val.replace("'", "''")),
                    None => "NULL".to_string()
                };
                
                let display_order_2nd_sql = match &record.display_order_2nd {
                    Some(val) => format!("'{}'", val.replace("'", "''")),
                    None => "NULL".to_string()
                };
                
                // Use direct SQL - simpler and more robust approach
                let sql = format!(
                    "SET IDENTITY_INSERT [dbo].[tbl_scheduling] ON;
                     
                     MERGE INTO [dbo].[tbl_scheduling] AS target
                     USING (SELECT {} AS scheduling_id) AS source
                     ON target.scheduling_id = source.scheduling_id
                     WHEN MATCHED THEN
                        UPDATE SET
                            date_start = '{}',
                            date_end = '{}',
                            remarks = {},
                            station = {},
                            employee_id = '{}',
                            department = '{}',
                            time_start = '{}',
                            time_end = '{}',
                            updated_at = '{}',
                            created_at = '{}',
                            [case] = {},
                            remark_2nd = {},
                            display_order = {},
                            display_order_2nd = {},
                            sync_status = 'SYNCED',
                            sync_datetime = '{}'
                     WHEN NOT MATCHED THEN
                        INSERT (
                            scheduling_id, date_start, date_end, remarks, station,
                            employee_id, department, time_start, time_end, updated_at,
                            created_at, [case], remark_2nd, display_order, display_order_2nd,
                            sync_status, sync_datetime
                        )
                        VALUES (
                            {}, '{}', '{}', {}, {},
                            '{}', '{}', '{}', '{}', '{}',
                            '{}', {}, {}, {}, {},
                            'SYNCED', '{}'
                        );
                     
                     SET IDENTITY_INSERT [dbo].[tbl_scheduling] OFF;",
                    record.scheduling_id,
                    record.date_start.replace("'", "''"),
                    record.date_end.replace("'", "''"),
                    remarks_sql,
                    station_sql,
                    record.employee_id.replace("'", "''"),
                    record.department.replace("'", "''"),
                    record.time_start.replace("'", "''"),
                    record.time_end.replace("'", "''"),
                    record.updated_at.replace("'", "''"),
                    record.created_at.replace("'", "''"),
                    case_sql,
                    remark_2nd_sql,
                    display_order_sql,
                    display_order_2nd_sql,
                    sync_timestamp,
                    record.scheduling_id,
                    record.date_start.replace("'", "''"),
                    record.date_end.replace("'", "''"),
                    remarks_sql,
                    station_sql,
                    record.employee_id.replace("'", "''"),
                    record.department.replace("'", "''"),
                    record.time_start.replace("'", "''"),
                    record.time_end.replace("'", "''"),
                    record.updated_at.replace("'", "''"),
                    record.created_at.replace("'", "''"),
                    case_sql,
                    remark_2nd_sql,
                    display_order_sql,
                    display_order_2nd_sql,
                    sync_timestamp
                );
                
                // Execute the SQL as a single batch
                sql_client.execute(&sql, &[]).await?;
                
                inserted += 1;
                progress_bar.inc(1);
            }
            
            let batch_elapsed = batch_start.elapsed();
            let batch_speed = format_transfer_speed(ids_chunk.len() as u32, batch_elapsed.as_secs_f64());
            
            // Update progress message with detailed information
            if (chunk_idx + 1) % 2 == 0 || (chunk_idx + 1) == ids_to_insert.len().div_ceil(BATCH_SIZE) {
                let elapsed = start_time.elapsed();
                let overall_speed = format_transfer_speed(inserted, elapsed.as_secs_f64());
                
                progress_bar.set_message(format!(
                    "🔄 {} → SQL Server (Scheduling INSERT) | Batch {}/{} | {} | {} | Last batch: {}",
                    "MySQL".green(),
                    chunk_idx + 1,
                    ids_to_insert.len().div_ceil(BATCH_SIZE),
                    format!("{}/{} records", inserted, ids_to_insert.len()).yellow(),
                    overall_speed.bright_blue(),
                    batch_speed.cyan()
                ));
            }
        }
    }

    // Handle updates
    if !ids_to_update.is_empty() {
        let update_start = Instant::now();
        progress_bar.set_message(format!("🔄 {} → SQL Server (Scheduling UPDATE)", "MySQL".green()));
        
        for (chunk_idx, ids_chunk) in ids_to_update.chunks(BATCH_SIZE).enumerate() {
            let batch_start = Instant::now();
            
            for &id in ids_chunk {
                let record = &mysql_records[&id];
                
                // Format optional values properly for SQL
                let remarks_sql = match &record.remarks {
                    Some(val) => format!("'{}'", val.replace("'", "''")),
                    None => "NULL".to_string()
                };
                
                let station_sql = match &record.station {
                    Some(val) => format!("'{}'", val.replace("'", "''")),
                    None => "NULL".to_string()
                };
                
                let case_sql = match &record.case {
                    Some(val) => format!("'{}'", val.replace("'", "''")),
                    None => "NULL".to_string()
                };
                
                let remark_2nd_sql = match &record.remark_2nd {
                    Some(val) => format!("'{}'", val.replace("'", "''")),
                    None => "NULL".to_string()
                };
                
                let display_order_sql = match &record.display_order {
                    Some(val) => format!("'{}'", val.replace("'", "''")),
                    None => "NULL".to_string()
                };
                
                let display_order_2nd_sql = match &record.display_order_2nd {
                    Some(val) => format!("'{}'", val.replace("'", "''")),
                    None => "NULL".to_string()
                };
                
                // Use straight SQL without parameters for simplicity
                let sql = format!(
                    "UPDATE [dbo].[tbl_scheduling] 
                    SET 
                        date_start = '{}', 
                        date_end = '{}', 
                        remarks = {}, 
                        station = {}, 
                        employee_id = '{}', 
                        department = '{}', 
                        time_start = '{}', 
                        time_end = '{}', 
                        updated_at = '{}', 
                        created_at = '{}', 
                        [case] = {}, 
                        remark_2nd = {}, 
                        display_order = {}, 
                        display_order_2nd = {},
                        sync_status = 'SYNCED', 
                        sync_datetime = '{}'
                    WHERE scheduling_id = {}",
                    record.date_start.replace("'", "''"),
                    record.date_end.replace("'", "''"),
                    remarks_sql,
                    station_sql,
                    record.employee_id.replace("'", "''"),
                    record.department.replace("'", "''"),
                    record.time_start.replace("'", "''"),
                    record.time_end.replace("'", "''"),
                    record.updated_at.replace("'", "''"),
                    record.created_at.replace("'", "''"),
                    case_sql,
                    remark_2nd_sql,
                    display_order_sql,
                    display_order_2nd_sql,
                    sync_timestamp,
                    record.scheduling_id
                );
                
                sql_client.execute(&sql, &[]).await?;
                
                updated += 1;
                progress_bar.inc(1);
            }
            
            let batch_elapsed = batch_start.elapsed();
            let batch_speed = format_transfer_speed(ids_chunk.len() as u32, batch_elapsed.as_secs_f64());
            
            // Update progress message with detailed information
            if (chunk_idx + 1) % 2 == 0 || (chunk_idx + 1) == ids_to_update.len().div_ceil(BATCH_SIZE) {
                let update_elapsed = update_start.elapsed();
                let overall_speed = format_transfer_speed(updated, update_elapsed.as_secs_f64());
                
                progress_bar.set_message(format!(
                    "🔄 {} → SQL Server (Scheduling UPDATE) | Batch {}/{} | {} | {} | Last batch: {}",
                    "MySQL".green(),
                    chunk_idx + 1,
                    ids_to_update.len().div_ceil(BATCH_SIZE),
                    format!("{}/{} records", updated, ids_to_update.len()).yellow(),
                    overall_speed.bright_blue(),
                    batch_speed.cyan()
                ));
            }
        }
    }
    
    // Final statistics
    let total_elapsed = start_time.elapsed();
    let total_transfers = inserted + updated;
    let avg_speed = format_transfer_speed(total_transfers, total_elapsed.as_secs_f64());
    
    progress_bar.set_message(format!("✅ MySQL → SQL Server (Scheduling) | {} inserted, {} updated | Avg speed: {}",
        inserted.to_string().green(),
        updated.to_string().green(),
        avg_speed.bright_blue()
    ));
    
    Ok((inserted, updated))
}