use anyhow::Result;
use mysql_async::prelude::*; 
use crate::database::connection::SqlServerClient;

pub async fn add_columns_if_not_exist_sql_server(
    client: &mut SqlServerClient,
) -> Result<()> {
    let query = "SELECT 
                  SUM(CASE WHEN COLUMN_NAME = 'sync_status' THEN 1 ELSE 0 END) AS has_sync_status,
                  SUM(CASE WHEN COLUMN_NAME = 'sync_datetime' THEN 1 ELSE 0 END) AS has_sync_datetime
                 FROM INFORMATION_SCHEMA.COLUMNS 
                 WHERE TABLE_NAME = 'tbl_att_logs'
                 AND COLUMN_NAME IN ('sync_status', 'sync_datetime')";
    
    let result = client.query(query, &[]).await?;
    let row = result.into_row().await?.unwrap();
    
    let has_sync_status: i32 = row.get(0).unwrap();
    let has_sync_datetime: i32 = row.get(1).unwrap();

    if has_sync_status == 0 {
        let add_sync_status_query =
            "ALTER TABLE tbl_att_logs ADD sync_status VARCHAR(50) NULL";
        client.execute(add_sync_status_query, &[]).await?;
    }

    if has_sync_datetime == 0 {
        let add_sync_datetime_query =
            "ALTER TABLE tbl_att_logs ADD sync_datetime DATETIME NULL";
        client.execute(add_sync_datetime_query, &[]).await?;
    }

    // Create index for log_id if it doesn't exist
    let idx_check = "IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_log_id' AND object_id = OBJECT_ID('tbl_att_logs'))
                      CREATE INDEX idx_log_id ON tbl_att_logs(log_id)";
    client.execute(idx_check, &[]).await?;
    
    Ok(())
}

// Helper function to check and add columns in MySQL
pub async fn add_columns_if_not_exist_mysql(
    conn: &mut mysql_async::Conn,
    db_name: &str,
) -> Result<()> {
    // Check if sync_status and sync_datetime exist in a single query
    let query = format!(
        "SELECT 
          SUM(CASE WHEN column_name = 'sync_status' THEN 1 ELSE 0 END) as has_sync_status,
          SUM(CASE WHEN column_name = 'sync_datetime' THEN 1 ELSE 0 END) as has_sync_datetime
         FROM information_schema.columns 
         WHERE table_schema = '{}' AND table_name = 'tbl_att_logs'
         AND column_name IN ('sync_status', 'sync_datetime')",
        db_name
    );
    
    let row: (i64, i64) = conn.query_first(query).await?.unwrap_or((0, 0));
    let (has_sync_status, has_sync_datetime) = row;

    // Add columns if needed
    if has_sync_status == 0 {
        let add_sync_status_query =
            "ALTER TABLE tbl_att_logs ADD COLUMN sync_status VARCHAR(50) NULL";
        conn.exec_drop(add_sync_status_query, ()).await?;
    }

    if has_sync_datetime == 0 {
        let add_sync_datetime_query =
            "ALTER TABLE tbl_att_logs ADD COLUMN sync_datetime DATETIME NULL";
        conn.exec_drop(add_sync_datetime_query, ()).await?;
    }

    // Create index for log_id if it doesn't exist
    conn.exec_drop(
        "CREATE INDEX IF NOT EXISTS idx_log_id ON tbl_att_logs(log_id)",
        (),
    ).await?;
    
    // Ensure MySQL table has the correct structure
    conn.exec_drop("CREATE TABLE IF NOT EXISTS tbl_att_logs (
        log_id INT PRIMARY KEY,
        employee_id VARCHAR(50) NOT NULL,
        log_dtime DATETIME NOT NULL,
        add_by INT,
        add_dtime DATETIME,
        insert_dtr_log_pic VARCHAR(255),
        hr_approval VARCHAR(50),
        dtr_type VARCHAR(50),
        remarks TEXT,
        sync_status VARCHAR(50),
        sync_datetime DATETIME,
        INDEX idx_employee_id (employee_id),
        INDEX idx_log_dtime (log_dtime),
        INDEX idx_sync_status (sync_status),
        INDEX idx_sync_datetime (sync_datetime)
    )", ()).await?;
    
    Ok(())
}