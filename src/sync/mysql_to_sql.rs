// Corrected src/sync/mysql_to_sql.rs file

use anyhow::Result;
use indicatif::ProgressBar;
use std::collections::HashMap;
use crate::database::connection::SqlServerClient;
use crate::models::AttendanceLog;
use crate::models::SchedulingRecord;

// Batch synchronize from MySQL to SQL Server
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
    
    if !ids_to_insert.is_empty() {
        progress_bar.set_message(format!("Inserting {} MySQL records to SQL Server...", ids_to_insert.len()));
        
        // Process each record by directly building a SQL statement with all parameters
        for &id in ids_to_insert {
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
            
            // Build a complete SQL statement that:
            // 1. Creates a temp table with the right structure
            // 2. Inserts our data into the temp table
            // 3. Enables IDENTITY_INSERT in a properly scoped context
            // 4. Performs the insert with a SELECT from the temp table
            // 5. Cleans up the temp table
            let sql = format!(
                "
                -- Create temp table
                IF OBJECT_ID('tempdb..#TempLog') IS NOT NULL DROP TABLE #TempLog;
                
                CREATE TABLE #TempLog (
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
                );
                
                -- Insert data into temp table
                INSERT INTO #TempLog (log_id, employee_id, log_dtime, add_by, add_dtime, insert_dtr_log_pic, hr_approval, dtr_type, remarks, sync_status, sync_datetime)
                VALUES (
                    {}, '{}', '{}', {}, '{}', 
                    {}, {}, {}, {}, 
                    'SYNCED', '{}'
                );
                
                -- Set IDENTITY_INSERT ON in a properly scoped context and insert from temp table
                EXEC('SET IDENTITY_INSERT [dbo].[tbl_att_logs] ON;
                     INSERT INTO [dbo].[tbl_att_logs] (
                        log_id, employee_id, log_dtime, add_by, add_dtime, 
                        insert_dtr_log_pic, hr_approval, dtr_type, remarks, 
                        sync_status, sync_datetime
                     )
                     SELECT 
                        log_id, employee_id, log_dtime, add_by, add_dtime, 
                        insert_dtr_log_pic, hr_approval, dtr_type, remarks, 
                        sync_status, sync_datetime
                     FROM #TempLog;
                     SET IDENTITY_INSERT [dbo].[tbl_att_logs] OFF;');
                
                -- Clean up
                DROP TABLE #TempLog;
                ",
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
            
            // Execute the SQL as a single batch
            sql_client.execute(&sql, &[]).await?;
            
            inserted += 1;
            progress_bar.inc(1);
        }
    }

    // Handle updates 
    if !ids_to_update.is_empty() {
        progress_bar.set_message(format!("Updating {} MySQL records in SQL Server...", ids_to_update.len()));
        
        for &id in ids_to_update {
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
            
            // Use straight SQL without parameters for simplicity
            let sql = format!(
                "UPDATE [dbo].[tbl_att_logs] 
                SET 
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
                WHERE log_id = {}",
                log.employee_id.replace("'", "''"),
                log.log_dtime.replace("'", "''"),
                log.add_by,
                log.add_dtime.replace("'", "''"),
                insert_dtr_log_pic_sql,
                hr_approval_sql,
                dtr_type_sql,
                remarks_sql,
                sync_timestamp,
                log.log_id
            );
            
            sql_client.execute(&sql, &[]).await?;
            
            updated += 1;
            progress_bar.inc(1);
        }
    }
    
    Ok((inserted, updated))
}

// Batch synchronize scheduling records from MySQL to SQL Server
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
    
    if !ids_to_insert.is_empty() {
        progress_bar.set_message(format!("Inserting {} MySQL scheduling records to SQL Server...", ids_to_insert.len()));
        
        // Process each record by directly building a SQL statement with all parameters
        for &id in ids_to_insert {
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
            
            // Build a complete SQL statement that:
            // 1. Creates a temp table with the right structure
            // 2. Inserts our data into the temp table
            // 3. Enables IDENTITY_INSERT in a properly scoped context
            // 4. Performs the insert with a SELECT from the temp table
            // 5. Cleans up the temp table
            let sql = format!(
                "
                -- Create temp table
                IF OBJECT_ID('tempdb..#TempScheduling') IS NOT NULL DROP TABLE #TempScheduling;
                
                CREATE TABLE #TempScheduling (
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
                    sync_status NVARCHAR(50),
                    sync_datetime DATETIME
                );
                
                -- Insert data into temp table
                INSERT INTO #TempScheduling (
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
                
                -- Set IDENTITY_INSERT ON in a properly scoped context and insert from temp table
                EXEC('SET IDENTITY_INSERT [dbo].[tbl_scheduling] ON;
                     INSERT INTO [dbo].[tbl_scheduling] (
                        scheduling_id, date_start, date_end, remarks, station, 
                        employee_id, department, time_start, time_end, updated_at, 
                        created_at, [case], remark_2nd, display_order, display_order_2nd, 
                        sync_status, sync_datetime
                     )
                     SELECT 
                        scheduling_id, date_start, date_end, remarks, station, 
                        employee_id, department, time_start, time_end, updated_at, 
                        created_at, [case], remark_2nd, display_order, display_order_2nd, 
                        sync_status, sync_datetime
                     FROM #TempScheduling;
                     SET IDENTITY_INSERT [dbo].[tbl_scheduling] OFF;');
                
                -- Clean up
                DROP TABLE #TempScheduling;
                ",
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
    }

    // Handle updates 
    if !ids_to_update.is_empty() {
        progress_bar.set_message(format!("Updating {} MySQL scheduling records in SQL Server...", ids_to_update.len()));
        
        for &id in ids_to_update {
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
    }
    
    Ok((inserted, updated))
}