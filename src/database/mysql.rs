use anyhow::Result;
use mysql_async::prelude::*; 
use std::collections::HashMap;
use std::io::Write;
use crate::models::AttendanceLog;

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