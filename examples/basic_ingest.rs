use std::error::Error;

use scopedb_practice::Config;
use scopedb_practice::run;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = Config::from_env()?;
    run(&config).await?;
    Ok(())
}
