// src/database/sql_server.rs - Fixed version

use anyhow::{Context, Result};
use futures_util::TryStreamExt;
use std::collections::HashMap;
use std::io::Write;
use crate::database::connection::SqlServerClient;
use crate::models::AttendanceLog;
use crate::models::SchedulingRecord;

// Fetch logs from SQL Server
pub async fn fetch_sql_server_logs(
    client: &mut SqlServerClient,
    query: &str,
) -> Result<HashMap<i32, AttendanceLog>> {
    // Use efficient query with batch size
    let result = client.query(query, &[]).await?;

    let mut logs = HashMap::new();
    let mut stream = result.into_row_stream();
    let mut count = 0;

    // Show progress during fetch
    print!("\rFetching SQL records: 0");
    std::io::stdout().flush().unwrap();

    while let Some(row) = stream.try_next().await? {
        let log_id: i32 = row.get(0).context("Failed to get log_id")?;
        let employee_id: &str = row.get(1).context("Failed to get employee_id")?;
        let log_dtime_str: &str = row.get(2).context("Failed to get log_dtime")?;
        let add_by: i32 = row.get(3).with_context(|| format!("Failed to get add_by for log_id {}", log_id))?;
        let add_dtime_str: &str = row.get(4).context("Failed to get add_dtime")?;
        let insert_dtr_log_pic_opt: Option<&str> = row.get(5);
        let hr_approval_opt: Option<&str> = row.get(6);
        let dtr_type_opt: Option<&str> = row.get(7);
        let remarks_opt: Option<&str> = row.get(8);

        let attendance_log = AttendanceLog {
            log_id,
            employee_id: employee_id.to_string(),
            log_dtime: log_dtime_str.to_string(),
            add_by,
            add_dtime: add_dtime_str.to_string(),
            insert_dtr_log_pic: insert_dtr_log_pic_opt.map(|s| s.to_string()),
            hr_approval: hr_approval_opt.map(|s| s.to_string()),
            dtr_type: dtr_type_opt.map(|s| s.to_string()),
            remarks: remarks_opt.map(|s| s.to_string()),
        };

        logs.insert(log_id, attendance_log);
        
        // Show progress periodically
        count += 1;
        if count % 1000 == 0 {
            print!("\rFetching SQL records: {}", count);
            std::io::stdout().flush().unwrap();
        }
    }

    print!("\rFetched {} SQL records           \n", logs.len());
    
    Ok(logs)
}

// Fetch scheduling records from SQL Server with proper NULL handling
pub async fn fetch_sql_server_scheduling(
    client: &mut SqlServerClient,
    query: &str,
) -> Result<HashMap<i32, SchedulingRecord>> {
    // Use efficient query with batch size
    let result = client.query(query, &[]).await?;

    let mut records = HashMap::new();
    let mut stream = result.into_row_stream();
    let mut count = 0;

    // Show progress during fetch
    print!("\rFetching SQL Server scheduling records: 0");
    std::io::stdout().flush().unwrap();

    while let Some(row) = stream.try_next().await? {
        // First, safely extract the scheduling_id - if missing or NULL, skip record
        let scheduling_id = match row.try_get::<i32, _>(0) {
            Ok(Some(id)) => id,
            Ok(None) => {
                println!("Warning: Skipping record with NULL scheduling_id");
                continue;
            },
            Err(e) => {
                println!("Warning: Error extracting scheduling_id: {:?}", e);
                continue;
            }
        };

        // Process date_start with proper error handling
        let date_start = match row.try_get::<&str, _>(1) {
            Ok(Some(val)) => val.to_string(),
            Ok(None) => String::from("1900-01-01"),
            Err(e) => {
                println!("Warning: Invalid date_start for scheduling_id {}: {:?}", scheduling_id, e);
                String::from("1900-01-01")
            }
        };
        
        // Process date_end with proper error handling
        let date_end = match row.try_get::<&str, _>(2) {
            Ok(Some(val)) => val.to_string(),
            Ok(None) => String::from("1900-01-01"),
            Err(e) => {
                println!("Warning: Invalid date_end for scheduling_id {}: {:?}", scheduling_id, e);
                String::from("1900-01-01")
            }
        };
        
        // Handle optional string fields properly
        let remarks = match row.try_get::<&str, _>(3) {
            Ok(Some(val)) => Some(val.to_string()),
            Ok(None) => None,
            Err(_) => None,
        };

        let station = match row.try_get::<&str, _>(4) {
            Ok(Some(val)) => Some(val.to_string()),
            Ok(None) => None,
            Err(_) => None,
        };
        
        // Process employee_id with proper error handling
        let employee_id = match row.try_get::<&str, _>(5) {
            Ok(Some(val)) => val.to_string(),
            Ok(None) => String::from("UNKNOWN"),
            Err(e) => {
                println!("Warning: Invalid employee_id for scheduling_id {}: {:?}", scheduling_id, e);
                String::from("UNKNOWN")
            }
        };
        
        // Process department with proper error handling
        let department = match row.try_get::<&str, _>(6) {
            Ok(Some(val)) => val.to_string(),
            Ok(None) => String::from("UNKNOWN"),
            Err(e) => {
                println!("Warning: Invalid department for scheduling_id {}: {:?}", scheduling_id, e);
                String::from("UNKNOWN")
            }
        };
        
        // Process time_start with proper error handling
        let time_start = match row.try_get::<&str, _>(7) {
            Ok(Some(val)) => val.to_string(),
            Ok(None) => String::from("00:00"),
            Err(e) => {
                println!("Warning: Invalid time_start for scheduling_id {}: {:?}", scheduling_id, e);
                String::from("00:00")
            }
        };
        
        // Process time_end with proper error handling
        let time_end = match row.try_get::<&str, _>(8) {
            Ok(Some(val)) => val.to_string(),
            Ok(None) => String::from("00:00"),
            Err(e) => {
                println!("Warning: Invalid time_end for scheduling_id {}: {:?}", scheduling_id, e);
                String::from("00:00")
            }
        };
        
        // Process updated_at with proper error handling
        let updated_at = match row.try_get::<&str, _>(9) {
            Ok(Some(val)) => val.to_string(),
            Ok(None) => String::from("1900-01-01 00:00:00"),
            Err(e) => {
                println!("Warning: Invalid updated_at for scheduling_id {}: {:?}", scheduling_id, e);
                String::from("1900-01-01 00:00:00")
            }
        };
        
        // Process created_at with proper error handling
        let created_at = match row.try_get::<&str, _>(10) {
            Ok(Some(val)) => val.to_string(),
            Ok(None) => String::from("1900-01-01 00:00:00"),
            Err(e) => {
                println!("Warning: Invalid created_at for scheduling_id {}: {:?}", scheduling_id, e);
                String::from("1900-01-01 00:00:00")
            }
        };
        
        // Handle remaining optional fields correctly
        let case = match row.try_get::<&str, _>(11) {
            Ok(Some(val)) => Some(val.to_string()),
            Ok(None) => None,
            Err(_) => None,
        };

        let remark_2nd = match row.try_get::<&str, _>(12) {
            Ok(Some(val)) => Some(val.to_string()),
            Ok(None) => None,
            Err(_) => None,
        };

        let display_order = match row.try_get::<&str, _>(13) {
            Ok(Some(val)) => Some(val.to_string()),
            Ok(None) => None,
            Err(_) => None,
        };

        let display_order_2nd = match row.try_get::<&str, _>(14) {
            Ok(Some(val)) => Some(val.to_string()),
            Ok(None) => None,
            Err(_) => None,
        };

        // Create the SchedulingRecord
        let record = SchedulingRecord {
            scheduling_id,
            date_start,
            date_end,
            remarks,
            station,
            employee_id,
            department,
            time_start,
            time_end,
            updated_at,
            created_at,
            case,
            remark_2nd,
            display_order,
            display_order_2nd,
        };

        // Insert into HashMap with scheduling_id as key
        records.insert(scheduling_id, record);
        
        // Show progress periodically
        count += 1;
        if count % 1000 == 0 {
            print!("\rFetching SQL Server scheduling records: {}", count);
            std::io::stdout().flush().unwrap();
        }
    }

    print!("\rFetched {} SQL Server scheduling records           \n", records.len());
    
    Ok(records)
}