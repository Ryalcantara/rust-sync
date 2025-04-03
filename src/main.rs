use anyhow::{Context, Result};
use colored::*;
use dotenv::dotenv;
use futures_util::{future::try_join, TryStreamExt};
use indicatif::{ProgressBar, ProgressStyle};
use mysql_async::{prelude::*, Opts};
use std::collections::{HashMap, HashSet};
use std::env;
use std::io::Write;
use std::time::Instant;
use tokio::time::Duration;
use tiberius::{AuthMethod, Client, Config};
use tokio::net::TcpStream;
use tokio_util::compat::{TokioAsyncWriteCompatExt, Compat};

// Styled progress bar creation
fn create_spinner(message: &str) -> ProgressBar {
    let spinner = ProgressBar::new_spinner();
    spinner.set_style(
        ProgressStyle::default_spinner()
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
            .template("{spinner:.blue} {msg} [{elapsed_precise}]")
            .unwrap(),
    );
    spinner.set_message(message.to_string());
    spinner.enable_steady_tick(Duration::from_millis(100)); // Update every 100ms for real-time display
    spinner
}

// Styled progress bar for overall progress
fn create_progress_bar(total: u64, message: &str) -> ProgressBar {
    let progress_bar = ProgressBar::new(total);
    progress_bar.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} {msg}\n{wide_bar:.cyan/blue} {pos}/{len} [{elapsed_precise}] [{per_sec}] {eta}")
            .unwrap()
            .progress_chars("#>-"),
    );
    progress_bar.set_message(message.to_string());
    progress_bar.enable_steady_tick(Duration::from_millis(100)); // Update every 100ms for real-time display
    progress_bar
}

// Structure to store attendance log data
#[derive(Debug, Clone, PartialEq)]
struct AttendanceLog {
    log_id: i32,
    employee_id: String,
    log_dtime: String, // Store as string for compatibility
    add_by: i32,
    add_dtime: String, // Store as string for compatibility
    insert_dtr_log_pic: Option<String>,
    hr_approval: Option<String>,
    dtr_type: Option<String>,
    remarks: Option<String>,
}

// Helper function to check and add columns in SQL Server
async fn add_columns_if_not_exist_sql_server(
    client: &mut Client<Compat<TcpStream>>,
) -> Result<()> {
    // Check if sync_status and sync_datetime exist in a single query for efficiency
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

    // Add columns if needed
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
async fn add_columns_if_not_exist_mysql(
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
    
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // Start timing the execution
    let start_time = Instant::now();

    // Clear screen and set up initial display
    print!("\x1B[2J\x1B[1;1H");

    // Banner
    println!(
        "{} {} {}",
        "🚀".green(),
        "Database Synchronization Tool".bold().blue(),
        "🚀".green()
    );
    println!("{}", "Connecting and preparing for sync...".yellow());

    // Load environment variables
    dotenv().ok();

    // Connection spinner
    let connection_spinner = create_spinner("Establishing database connections...");

    // Connect to both databases
    let (mut sql_client, mut mysql_conn, mysql_db_name) =
        connect_databases(&connection_spinner).await?;

    // Ensure the additional columns exist on both databases - run in parallel
    let prepare_futures = try_join(
        add_columns_if_not_exist_sql_server(&mut sql_client),
        add_columns_if_not_exist_mysql(&mut mysql_conn, &mysql_db_name),
    );
    prepare_futures.await?;

    connection_spinner.finish_with_message("✅ Databases connected and columns verified!");

    // Fetch logs progress
    let fetch_spinner = create_spinner("Preparing to fetch logs...");

    // Perform optimized sync
    let sync_result = optimized_sync_databases(&mut sql_client, &mut mysql_conn, fetch_spinner).await;

    // Display execution time
    let duration = start_time.elapsed();
    println!(
        "\n{}",
        format!("⏱️ Total execution time: {:.2?}", duration).cyan()
    );

    // Final result display
    match sync_result {
        Ok(_) => {
            println!(
                "{}",
                "🎉 Database synchronization completed successfully! 🎉"
                    .green()
                    .bold()
            );
        }
        Err(e) => {
            eprintln!("{} {}", "❌ Sync failed:".red().bold(), e);
        }
    }

    Ok(())
}

async fn connect_databases(
    spinner: &ProgressBar,
) -> Result<(Client<Compat<TcpStream>>, mysql_async::Conn, String)> {
    // SQL Server connection setup
    let mut config = Config::new();

    let sql_server_host = env::var("SQL_SERVER").context("SQL_SERVER env var not set")?;
    let sql_server_db = env::var("SQL_DB").context("SQL_DB env var not set")?;

    config.host(&sql_server_host);
    config.database(&sql_server_db);
    config.authentication(AuthMethod::Integrated);
    config.trust_cert();
    
    // Connect to SQL Server
    let tcp = TcpStream::connect(config.get_addr()).await?;
    tcp.set_nodelay(true)?;
    let mut client = Client::connect(config, tcp.compat_write()).await?;

    // Test SQL Server connection
    let result = client.query("SELECT @@VERSION", &[]).await?;
    let row = result.into_row().await?.unwrap();
    let version: &str = row.get(0).unwrap();
    spinner.set_message(format!("Connected to SQL Server: {}", version));

    // MySQL connection setup
    let username = env::var("HOSTINGER_USER").context("HOSTINGER_USER not set")?;
    let password = env::var("HOSTINGER_PASSWORD").context("HOSTINGER_PASSWORD not set")?;
    let host = env::var("HOSTINGER_HOST").context("HOSTINGER_HOST not set")?;
    let database = env::var("HOSTINGER_DATABASE").context("HOSTINGER_DATABASE not set")?;

    let database_url = format!("mysql://{}:{}@{}:3306/{}", username, password, host, database);

    // Connect to MySQL
    let opts = Opts::from_url(&database_url).context("Invalid MySQL connection URL")?;
    let pool = mysql_async::Pool::new(opts);
    let conn = pool.get_conn().await.context("Failed to connect to MySQL database")?;

    spinner.set_message("Connected to both databases successfully!".to_string());

    Ok((client, conn, database))
}

async fn optimized_sync_databases(
    sql_client: &mut Client<Compat<TcpStream>>,
    mysql_conn: &mut mysql_async::Conn,
    fetch_spinner: ProgressBar,
) -> Result<()> {
    // First, make sure the target table in MySQL has the correct structure
    fetch_spinner.set_message("Verifying MySQL table structure...".to_string());
    
    // Add indexes for better performance
    mysql_conn.exec_drop("CREATE TABLE IF NOT EXISTS tbl_att_logs (
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

    // 1. Get the latest sync timestamp from both databases
    fetch_spinner.set_message("Determining last sync time...".to_string());
    
    // Get the most recent sync time from SQL Server - handle NULL explicitly in SQL
    let sql_last_sync: Option<String> = {
        // Convert NULL to a string representation in SQL Server to avoid Rust conversion issues
        // Include both SYNCED and pending records in the sync logic
        let query = "SELECT CONVERT(VARCHAR(23), MAX(sync_datetime), 120) FROM tbl_att_logs WHERE sync_status IN ('SYNCED', 'pending')";
        let result = sql_client.query(query, &[]).await?;
        
        // Process the result safely
        match result.into_row().await? {
            Some(row) => {
                // Get as Option<&str> to properly handle NULL values
                let str_val: Option<&str> = row.get(0);
                match str_val {
                    Some(s) if !s.is_empty() => Some(s.to_string()),
                    _ => None
                }
            },
            None => None
        }
    };

    // Get the most recent sync time from MySQL - handle NULL with COALESCE
    let mysql_last_sync: Option<String> = {
        // Use COALESCE to convert NULL to empty string in MySQL
        // Include both SYNCED and pending records in the sync logic
        let query = "SELECT COALESCE(MAX(sync_datetime), '') FROM tbl_att_logs WHERE sync_status IN ('SYNCED', 'pending')";
        let result: String = mysql_conn.query_first(query).await?.unwrap_or_default();
        
        // Only use non-empty results
        if !result.is_empty() {
            Some(result)
        } else {
            None
        }
    };

    // Determine the overall last sync time (use the earlier one if they differ)
    let last_sync_str = match (sql_last_sync.as_deref(), mysql_last_sync.as_deref()) {
        (Some(sql), Some(mysql)) => {
            if sql < mysql { Some(sql.to_string()) } else { Some(mysql.to_string()) }
        },
        (Some(sql), None) => Some(sql.to_string()),
        (None, Some(mysql)) => Some(mysql.to_string()),
        (None, None) => None,
    };

    // Format for SQL queries - now including records with 'pending' status
    let last_sync_clause = match &last_sync_str {
        Some(timestamp) => format!("AND (sync_status IS NULL OR sync_status = 'pending' OR sync_datetime > '{}')", timestamp),
        None => String::from(""),
    };

    fetch_spinner.set_message(format!(
        "Fetching unsynchronized logs since {}...",
        last_sync_str.as_deref().unwrap_or("beginning")
    ));

    // 2. Fetch only the logs that need synchronization
    // For SQL Server
    let sql_query = format!(
        "SELECT log_id, employee_id, \
         CONVERT(VARCHAR(23), log_dtime, 120) as log_dtime_str, \
         ISNULL(add_by, 0) as add_by_safe, \
         CONVERT(VARCHAR(23), add_dtime, 120) as add_dtime_str, \
         insert_dtr_log_pic, hr_approval, dtr_type, remarks \
         FROM tbl_att_logs \
         WHERE 1=1 {} \
         ORDER BY log_id",
        last_sync_clause
    );

    // For MySQL
    let mysql_query = format!(
        "SELECT log_id, employee_id, log_dtime, add_by, add_dtime, \
         insert_dtr_log_pic, hr_approval, dtr_type, remarks \
         FROM tbl_att_logs \
         WHERE 1=1 {} \
         ORDER BY log_id",
        last_sync_clause
    );

    // Fetch logs from SQL Server and MySQL - parallel fetch
    let fetch_sql_spinner = create_spinner("Fetching logs from SQL Server...");
    let fetch_mysql_spinner = create_spinner("Fetching logs from MySQL...");
    
    // Run both fetch operations in parallel
    let sql_logs_future = fetch_sql_server_logs(sql_client, &sql_query);
    let mysql_logs_future = fetch_mysql_logs(mysql_conn, &mysql_query);
    
    // Execute both fetch operations concurrently
    let (sql_logs_result, mysql_logs_result) = try_join(
        async {
            let logs = sql_logs_future.await?;
            fetch_sql_spinner.finish_with_message(format!(
                "✅ Fetched {} SQL Server logs", logs.len()
            ));
            Ok::<_, anyhow::Error>(logs)
        },
        async {
            let logs = mysql_logs_future.await?;
            fetch_mysql_spinner.finish_with_message(format!(
                "✅ Fetched {} MySQL logs", logs.len()
            ));
            Ok::<_, anyhow::Error>(logs)
        }
    ).await?;
    
    let sql_logs = sql_logs_result;
    let mysql_logs = mysql_logs_result;
    
    // Double-check for MySQL records by querying a count of all records
    let all_mysql_count: i64 = mysql_conn.query_first("SELECT COUNT(*) FROM tbl_att_logs").await?.unwrap_or(0);
    if all_mysql_count > 0 && mysql_logs.is_empty() {
        println!("{}", format!("⚠️  Warning: Found {} records in MySQL but none matched sync criteria", all_mysql_count).yellow());
    }

    // Calculate what logs need to be synced in each direction
    let sql_log_ids: HashSet<i32> = sql_logs.keys().copied().collect();
    let mysql_log_ids: HashSet<i32> = mysql_logs.keys().copied().collect();

    // IDs to insert in each database (records that exist in one database but not the other)
    let sql_only_ids: Vec<i32> = sql_log_ids.difference(&mysql_log_ids).copied().collect();
    let mysql_only_ids: Vec<i32> = mysql_log_ids.difference(&sql_log_ids).copied().collect();

    // IDs that exist in both databases
    let common_ids: HashSet<i32> = sql_log_ids.intersection(&mysql_log_ids).copied().collect();

    // Find logs that need updates (are different between databases)
    let need_update_in_mysql: Vec<i32> = common_ids
        .iter()
        .filter(|id| {
            let sql_log = &sql_logs[id];
            let mysql_log = &mysql_logs[id];
            sql_log != mysql_log
        })
        .copied()
        .collect();

    let need_update_in_sql: Vec<i32> = common_ids
        .iter()
        .filter(|id| {
            let sql_log = &sql_logs[id];
            let mysql_log = &mysql_logs[id];
            sql_log != mysql_log
        })
        .copied()
        .collect();
    
    // Count of identical records in both databases (these don't need syncing)
    let identical_records = common_ids.len() - need_update_in_mysql.len();
    
    if identical_records > 0 {
        println!("{} {} {}", 
            "↪".cyan(),
            format!("{} records already in sync - skipping these", identical_records).yellow(),
            "↩".cyan()
        );
    }

    // Display sync summary
    fetch_spinner.finish_with_message(format!(
        "✅ Analysis complete: {} SQL Server logs, {} MySQL logs",
        sql_logs.len().to_string().blue(),
        mysql_logs.len().to_string().blue()
    ));
    
    // Show what needs to be synced
    println!("Sync needed:");
    println!("  → SQL Server to MySQL: {} new, {} updates", 
        sql_only_ids.len().to_string().yellow(),
        need_update_in_mysql.len().to_string().yellow()
    );
    println!("  → MySQL to SQL Server: {} new, {} updates", 
        mysql_only_ids.len().to_string().yellow(),
        need_update_in_sql.len().to_string().yellow()
    );

    // Early return if no updates needed
    if sql_logs.is_empty() && mysql_logs.is_empty() {
        println!("{}", "⚡ No changes detected, both databases are in sync!".green());
        return Ok(());
    }
    
    // Early return if all records exist in both databases and are identical
    if sql_only_ids.is_empty() && mysql_only_ids.is_empty() && 
       need_update_in_mysql.is_empty() && need_update_in_sql.is_empty() {
        println!("{}", "✓ All existing records are identical - no synchronization needed!".green());
        return Ok(());
    }

    // Current timestamp for sync_datetime
    let sync_timestamp = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
    
    // Mark existing identical records as synced
    if !common_ids.is_empty() {
        // Get IDs of records that are identical
        let identical_ids: Vec<i32> = common_ids.iter()
            .filter(|id| {
                let sql_log = &sql_logs[id];
                let mysql_log = &mysql_logs[id];
                // Consider them identical if their content matches, 
                // even if one has 'pending' status
                sql_log == mysql_log
            })
            .copied()
            .collect();
            
        // If there are identical records, we still want to mark them as synced
        if !identical_ids.is_empty() {
            let chunk_size = 100;
            let update_spinner = create_spinner(&format!(
                "Marking {} identical records as synced...", 
                identical_ids.len()
            ));
            
            // Update SQL Server
            for chunk in identical_ids.chunks(chunk_size) {
                if !chunk.is_empty() {
                    let placeholders = chunk.iter()
                        .map(|id| id.to_string())
                        .collect::<Vec<String>>()
                        .join(",");
                        
                    let sql_query = format!(
                        "UPDATE tbl_att_logs SET sync_status = 'SYNCED', sync_datetime = '{}' \
                         WHERE log_id IN ({})",
                        sync_timestamp, placeholders
                    );
                    
                    // Execute SQL Server update
                    sql_client.execute(&sql_query, &[]).await?;
                }
            }
            
            // Update MySQL
            let mut transaction = mysql_conn.start_transaction(Default::default()).await?;
            
            for chunk in identical_ids.chunks(chunk_size) {
                if !chunk.is_empty() {
                    // For MySQL, we need to create a separate parameter for each ID
                    let placeholders = chunk.iter()
                        .map(|_| "?".to_string())
                        .collect::<Vec<String>>()
                        .join(",");
                    
                    let mysql_query = format!(
                        "UPDATE tbl_att_logs SET sync_status = 'SYNCED', sync_datetime = '{}' \
                         WHERE log_id IN ({})",
                        sync_timestamp, placeholders
                    );
                    
                    // Convert i32 chunk to Vec<i32> for MySQL parameters
                    let params = chunk.iter()
                        .map(|&id| id)
                        .collect::<Vec<i32>>();
                    
                    // Execute MySQL update
                    transaction.exec_drop(mysql_query, params).await?;
                }
            }
            
            transaction.commit().await?;
            update_spinner.finish_with_message(format!(
                "✓ Marked {} identical records as synced", identical_ids.len()
            ));
        }
    }
    
    // Progress bar for sync
    let total_operations = sql_only_ids.len() + mysql_only_ids.len() + need_update_in_mysql.len() + need_update_in_sql.len();
    
    // If no operations to perform, exit early
    if total_operations == 0 {
        return Ok(());
    }
    
    let sync_progress = create_progress_bar(total_operations as u64, "Synchronizing databases...");

    // 3. Perform batch operations for better performance
    // Sync SQL → MySQL (inserts and updates)
    let sql_to_mysql_result = batch_sync_sql_to_mysql(
        mysql_conn,
        &sql_logs,
        &sql_only_ids,
        &need_update_in_mysql,
        &sync_timestamp,
        &sync_progress,
    ).await?;

    // Sync MySQL → SQL (inserts and updates)
    let mysql_to_sql_result = batch_sync_mysql_to_sql(
        sql_client,
        &mysql_logs,
        &mysql_only_ids,
        &need_update_in_sql,
        &sync_timestamp,
        &sync_progress,
    ).await?;

    sync_progress.finish_with_message(format!(
        "✅ Sync Complete! \n  SQL Server → MySQL: {} \n  MySQL → SQL Server: {}",
        format!("Inserted {}, Updated {}", sql_to_mysql_result.0, sql_to_mysql_result.1).green(),
        format!("Inserted {}, Updated {}", mysql_to_sql_result.0, mysql_to_sql_result.1).green()
    ));

    Ok(())
}

async fn fetch_sql_server_logs(
    client: &mut Client<Compat<TcpStream>>,
    query: &str,
) -> Result<HashMap<i32, AttendanceLog>> {
    // Use efficient query with batch size - may need to be adjusted depending on tiberius version
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

async fn fetch_mysql_logs(
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
        
        // Show progress periodically
        count += 1;
        if count % 1000 == 0 {
            print!("\rFetching MySQL records: {}", count);
            std::io::stdout().flush().unwrap();
        }
    }

    print!("\rFetched {} MySQL records           \n", log_map.len());
    
    Ok(log_map)
}

async fn batch_sync_sql_to_mysql(
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
    const BATCH_SIZE: usize = 200; // Increased batch size
    
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
                
                // Add all values for this record as the appropriate mysql_async::Value type
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

async fn batch_sync_mysql_to_sql(
    sql_client: &mut Client<Compat<TcpStream>>,
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