use anyhow::Context;
use dotenv::dotenv;
use mysql_async::{prelude::Queryable, Opts};
use std::env;
use tiberius::{AuthMethod, Client, Config};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok();
    let mut config = Config::new();

    let sql_server_host = env::var("SQL_SERVER");
    let sql_server_db = env::var("SQL_DB");
    // Set server address and database name
    config.host(sql_server_host.unwrap());
    config.database(sql_server_db.unwrap());
    config.authentication(AuthMethod::Integrated);
    config.trust_cert();

    // Connect to the server
    let tcp = TcpStream::connect(config.get_addr()).await?;
    tcp.set_nodelay(true)?;

    let mut client = Client::connect(config, tcp.compat_write()).await?;

    // Test the connection
    let result = client.query("SELECT @@VERSION", &[]).await?;
    let row = result.into_row().await?.unwrap();
    let version: &str = row.get(0).unwrap();
    println!("Connected successfully: {}", version);

    // Query data from SQL Server
    let username = env::var("HOSTINGER_USER").expect("HOSTINGER_USER not set");
    let password = env::var("HOSTINGER_PASSWORD").expect("HOSTINGER_PASSWORD not set");
    let host = env::var("HOSTINGER_HOST").expect("HOSTINGER_HOST not set");
    let database = env::var("HOSTINGER_DATABASE").expect("HOSTINGER_DATABASE not set");
    let database_url = format!(
        "mysql://{}:{}@{}:3306/{}",
        username, password, host, database
    );
    // Convert URL string to Opts
    let opts = Opts::from_url(&database_url).expect("Invalid connection URL");
    let pool = mysql_async::Pool::new(opts);
    let mut conn = pool
        .get_conn()
        .await
        .context("Failed to connect to MySQL database")?;

    let result: Vec<(u32, String)> = conn.query("SELECT * FROM tbl_masterlist_emp").await?;
    println!("{:#?}", result);
    Ok(())
}
