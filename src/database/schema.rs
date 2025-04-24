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
    let row = result.into_row().await?;
    
    // Handle case where row might be None (no columns found)
    if let Some(row) = row {
        let has_sync_status: i32 = row.get(0).unwrap_or(0);
        let has_sync_datetime: i32 = row.get(1).unwrap_or(0);

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
    } else {
        // Table might not exist, create it
        let create_table_query = "IF OBJECT_ID('tbl_att_logs', 'U') IS NULL
                                  CREATE TABLE tbl_att_logs (
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
                                      sync_datetime DATETIME
                                  )";
        client.execute(create_table_query, &[]).await?;
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

// SQL Server schema verification for tbl_scheduling
pub async fn add_scheduling_columns_if_not_exist_sql_server(
    client: &mut SqlServerClient,
) -> Result<()> {
    // First check if the table exists
    let table_check = "SELECT CASE WHEN OBJECT_ID('tbl_scheduling', 'U') IS NOT NULL THEN 1 ELSE 0 END AS table_exists";
    let table_result = client.query(table_check, &[]).await?;
    let table_row = table_result.into_row().await?;
    let table_exists: i32 = if let Some(row) = table_row {
        row.get(0).unwrap_or(0)
    } else {
        0
    };

    // If table doesn't exist, create it
    if table_exists == 0 {
        let create_table_query = "CREATE TABLE tbl_scheduling (
            scheduling_id INT PRIMARY KEY,
            date_start DATE NOT NULL,
            date_end DATE NOT NULL,
            remarks NVARCHAR(MAX),
            station NVARCHAR(255),
            employee_id NVARCHAR(50) NOT NULL,
            department NVARCHAR(100) NOT NULL,
            time_start NVARCHAR(10) NOT NULL,
            time_end NVARCHAR(10) NOT NULL,
            updated_at DATETIME,
            created_at DATETIME,
            [case] NVARCHAR(255),
            remark_2nd NVARCHAR(MAX),
            display_order NVARCHAR(255),
            display_order_2nd NVARCHAR(255),
            sync_status VARCHAR(50) NULL,
            sync_datetime DATETIME NULL
        )";
        client.execute(create_table_query, &[]).await?;
        
        // Create the index
        let create_index = "CREATE INDEX idx_scheduling_id ON tbl_scheduling(scheduling_id)";
        client.execute(create_index, &[]).await?;
        
        return Ok(());
    }

    // If table exists, check for columns
    let query = "SELECT 
                  SUM(CASE WHEN COLUMN_NAME = 'sync_status' THEN 1 ELSE 0 END) AS has_sync_status,
                  SUM(CASE WHEN COLUMN_NAME = 'sync_datetime' THEN 1 ELSE 0 END) AS has_sync_datetime
                 FROM INFORMATION_SCHEMA.COLUMNS 
                 WHERE TABLE_NAME = 'tbl_scheduling'
                 AND COLUMN_NAME IN ('sync_status', 'sync_datetime')";
    
    let result = client.query(query, &[]).await?;
    let row = result.into_row().await?;
    
    // Handle case where row might be None
    if let Some(row) = row {
        let has_sync_status: i32 = row.get(0).unwrap_or(0);
        let has_sync_datetime: i32 = row.get(1).unwrap_or(0);

        if has_sync_status == 0 {
            let add_sync_status_query =
                "ALTER TABLE tbl_scheduling ADD sync_status VARCHAR(50) NULL";
            client.execute(add_sync_status_query, &[]).await?;
        }

        if has_sync_datetime == 0 {
            let add_sync_datetime_query =
                "ALTER TABLE tbl_scheduling ADD sync_datetime DATETIME NULL";
            client.execute(add_sync_datetime_query, &[]).await?;
        }
    }

    // Create index for scheduling_id if it doesn't exist
    let idx_check = "IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_scheduling_id' AND object_id = OBJECT_ID('tbl_scheduling'))
                      CREATE INDEX idx_scheduling_id ON tbl_scheduling(scheduling_id)";
    client.execute(idx_check, &[]).await?;
    
    Ok(())
}

// MySQL schema verification for tbl_scheduling
pub async fn add_scheduling_columns_if_not_exist_mysql(
    conn: &mut mysql_async::Conn,
    db_name: &str,
) -> Result<()> {
    // First check if the table exists
    let table_check = format!(
        "SELECT COUNT(*) FROM information_schema.tables 
        WHERE table_schema = '{}' AND table_name = 'tbl_scheduling'",
        db_name
    );
    
    let table_exists: i64 = conn.query_first(table_check).await?.unwrap_or(0);
    
    // If table doesn't exist, create it
    if table_exists == 0 {
        // Ensure MySQL table has the correct structure first
        conn.exec_drop("CREATE TABLE IF NOT EXISTS tbl_scheduling (
            scheduling_id INT PRIMARY KEY,
            date_start DATE NOT NULL,
            date_end DATE NOT NULL,
            remarks TEXT,
            station VARCHAR(255),
            employee_id VARCHAR(50) NOT NULL,
            department VARCHAR(100) NOT NULL,
            time_start VARCHAR(10) NOT NULL,
            time_end VARCHAR(10) NOT NULL,
            updated_at DATETIME,
            created_at DATETIME,
            `case` VARCHAR(255),
            remark_2nd TEXT,
            display_order VARCHAR(255),
            display_order_2nd VARCHAR(255),
            sync_status VARCHAR(50),
            sync_datetime DATETIME,
            INDEX idx_employee_id (employee_id),
            INDEX idx_date_start (date_start),
            INDEX idx_date_end (date_end),
            INDEX idx_sync_status (sync_status),
            INDEX idx_sync_datetime (sync_datetime)
        )", ()).await?;
        
        return Ok(());
    }

    // If table exists, check for columns
    // Check if sync_status and sync_datetime exist in a single query
    let query = format!(
        "SELECT 
          SUM(CASE WHEN column_name = 'sync_status' THEN 1 ELSE 0 END) as has_sync_status,
          SUM(CASE WHEN column_name = 'sync_datetime' THEN 1 ELSE 0 END) as has_sync_datetime
         FROM information_schema.columns 
         WHERE table_schema = '{}' AND table_name = 'tbl_scheduling'
         AND column_name IN ('sync_status', 'sync_datetime')",
        db_name
    );
    
    let row: (i64, i64) = conn.query_first(query).await?.unwrap_or((0, 0));
    let (has_sync_status, has_sync_datetime) = row;

    // Add columns if needed
    if has_sync_status == 0 {
        let add_sync_status_query =
            "ALTER TABLE tbl_scheduling ADD COLUMN sync_status VARCHAR(50) NULL";
        conn.exec_drop(add_sync_status_query, ()).await?;
    }

    if has_sync_datetime == 0 {
        let add_sync_datetime_query =
            "ALTER TABLE tbl_scheduling ADD COLUMN sync_datetime DATETIME NULL";
        conn.exec_drop(add_sync_datetime_query, ()).await?;
    }

    // Create index for scheduling_id if it doesn't exist
    conn.exec_drop(
        "CREATE INDEX IF NOT EXISTS idx_scheduling_id ON tbl_scheduling(scheduling_id)",
        (),
    ).await?;
    
    Ok(())
}