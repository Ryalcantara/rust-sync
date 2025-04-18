use anyhow::Result;
use mysql_async::prelude::*; 
use std::collections::HashMap;
use std::io::Write;
use crate::models::AttendanceLog;
use crate::models::SchedulingRecord;

// Fetch logs from MySQL
pub async fn fetch_mysql_logs(
    conn: &mut mysql_async::Conn,
    query: &str,
) -> Result<HashMap<i32, AttendanceLog>> {
    let logs: Vec<(i32, String, String, i32, String, Option<String>, Option<String>, Option<String>, Option<String>)> =
        conn.query(query).await?;

    let mut log_map = HashMap::with_capacity(logs.len());
    let mut count = 0;

    // Show progress during processing
    print!("\rFetching MySQL records: 0");
    std::io::stdout().flush().unwrap();

    for (log_id, employee_id, log_dtime, add_by, add_dtime, insert_dtr_log_pic, hr_approval, dtr_type, remarks) in logs {
        let attendance_log = AttendanceLog {
            log_id,
            employee_id,
            log_dtime,
            add_by,
            add_dtime,
            insert_dtr_log_pic,
            hr_approval,
            dtr_type,
            remarks,
        };

        log_map.insert(log_id, attendance_log);
        
        count += 1;
        if count % 1000 == 0 {
            print!("\rFetching MySQL records: {}", count);
            std::io::stdout().flush().unwrap();
        }
    }

    print!("\rFetched {} MySQL records           \n", log_map.len());
    
    Ok(log_map)
}

// Check MySQL record count
pub async fn get_mysql_record_count(conn: &mut mysql_async::Conn) -> Result<i64> {
    let count: i64 = conn.query_first("SELECT COUNT(*) FROM tbl_att_logs").await?.unwrap_or(0);
    Ok(count)
}

// Fetch scheduling records from MySQL with proper NULL handling
pub async fn fetch_mysql_scheduling(
    conn: &mut mysql_async::Conn,
    query: &str,
) -> Result<HashMap<i32, SchedulingRecord>> {
    // Use raw query to handle the data directly
    let mut record_map = HashMap::new();
    let mut count = 0;

    // Show progress during processing
    print!("\rFetching MySQL scheduling records: 0");
    std::io::stdout().flush().unwrap();

    // Get rows using simple query instead of prepared statement to avoid type issues
    let result = conn.query_iter(query).await?;
    
    let mapped_rows = result.map_and_drop(|row| {
        // Process each field carefully to avoid Option<Option<T>> issues
        let scheduling_id = row.get::<i32, _>(0).unwrap_or_default();
        
        // For date fields, first try to get the Option<String>, then unwrap properly
        let date_start = row.get::<Option<String>, _>(1)
            .flatten() // Handle Option<Option<String>> -> Option<String>
            .unwrap_or_else(|| "1900-01-01".to_string());
        
        let date_end = row.get::<Option<String>, _>(2)
            .flatten()
            .unwrap_or_else(|| "1900-01-01".to_string());
        
        // For optional fields, use flatten to handle Option<Option<String>> -> Option<String>
        let remarks = row.get::<Option<String>, _>(3).flatten();
        let station = row.get::<Option<String>, _>(4).flatten();
        
        // For required fields, unwrap with default values
        let employee_id = row.get::<Option<String>, _>(5)
            .flatten()
            .unwrap_or_else(|| "UNKNOWN".to_string());
        
        let department = row.get::<Option<String>, _>(6)
            .flatten()
            .unwrap_or_else(|| "UNKNOWN".to_string());
        
        let time_start = row.get::<Option<String>, _>(7)
            .flatten()
            .unwrap_or_else(|| "00:00".to_string());
        
        let time_end = row.get::<Option<String>, _>(8)
            .flatten()
            .unwrap_or_else(|| "00:00".to_string());
        
        let updated_at = row.get::<Option<String>, _>(9)
            .flatten()
            .unwrap_or_else(|| "1900-01-01 00:00:00".to_string());
        
        let created_at = row.get::<Option<String>, _>(10)
            .flatten()
            .unwrap_or_else(|| "1900-01-01 00:00:00".to_string());
        
        // More optional fields, use flatten for each
        let case = row.get::<Option<String>, _>(11).flatten();
        let remark_2nd = row.get::<Option<String>, _>(12).flatten();
        let display_order = row.get::<Option<String>, _>(13).flatten();
        let display_order_2nd = row.get::<Option<String>, _>(14).flatten();
        
        // Create the scheduling record
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
        
        (scheduling_id, record)
    }).await?;
    
    // Process the mapped rows
    for (id, record) in mapped_rows {
        record_map.insert(id, record);
        
        count += 1;
        if count % 1000 == 0 {
            print!("\rFetching MySQL scheduling records: {}", count);
            std::io::stdout().flush().unwrap();
        }
    }

    print!("\rFetched {} MySQL scheduling records           \n", record_map.len());
    
    Ok(record_map)
}

// Check MySQL scheduling record count
pub async fn get_mysql_scheduling_count(conn: &mut mysql_async::Conn) -> Result<i64> {
    let count: i64 = conn.query_first("SELECT COUNT(*) FROM tbl_scheduling").await?.unwrap_or(0);
    Ok(count)
}