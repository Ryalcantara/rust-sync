use anyhow::{Context, Result};
use indicatif::ProgressBar;
use mysql_async::{Opts, Pool};
use std::env;
use tiberius::{AuthMethod, Client, Config};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;

pub type SqlServerClient = Client<tokio_util::compat::Compat<TcpStream>>;

pub async fn connect_databases(
    spinner: &ProgressBar,
) -> Result<(SqlServerClient, mysql_async::Conn, String)> {
    let mut config = Config::new();

    let sql_server_host = env::var("SQL_SERVER").context("SQL_SERVER env var not set")?;
    let sql_server_db = env::var("SQL_DB").context("SQL_DB env var not set")?;

    config.host(&sql_server_host);
    config.database(&sql_server_db);
    config.authentication(AuthMethod::Integrated);
    config.trust_cert();
    
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
    let pool = Pool::new(opts);
    let conn = pool.get_conn().await.context("Failed to connect to MySQL database")?;

    spinner.set_message("Connected to both databases successfully!".to_string());

    Ok((client, conn, database))
}