use anyhow::{Context, Result};
use futures_util::TryStreamExt;
use std::collections::HashMap;
use std::io::Write;
use crate::database::connection::SqlServerClient;
use crate::models::AttendanceLog;

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