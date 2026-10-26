// Copyright 2026 tyranitar-mega
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::error::Error;

use scopedb_practice::Config;
use scopedb_practice::run_bulk_import;

#[tokio::main]
async fn main() {
    if let Err(err) = try_main().await {
        eprintln!("Error: {err}");
        std::process::exit(1);
    }
}

async fn try_main() -> Result<(), Box<dyn Error>> {
    let config = Config::from_env_and_args()?;
    run_bulk_import(&config).await?;
    Ok(())
}
