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
use std::fmt;

use clap::Parser;
use clap::ValueEnum;
use scopedb_client::Client;
use scopedb_client::ResultObject;
use scopedb_client::Table;
use scopedb_client::reqwest;
use scopedb_client::reqwest::Url;
use scopedb_client::reqwest::header::AUTHORIZATION;
use scopedb_client::reqwest::header::CONTENT_TYPE;
use scopedb_client::reqwest::header::HeaderValue;
use scopedb_practice::Config;
use scopedb_practice::TableArgs;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value as JsonValue;
use serde_json::json;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, ValueEnum)]
enum WritePath {
    Ingest,
    Append,
    #[default]
    Both,
}

#[derive(Debug, Clone, Parser)]
#[command(
    name = "basic_ingest",
    about = "Run the basic ScopeDB ingestion and query example",
    after_help = "Required environment variables:\n  SCOPEDB_ENDPOINT  HTTPS base URL of the ScopeDB workspace API\n  SCOPEDB_API_KEY   Bearer API key for the workspace"
)]
struct ExampleArgs {
    #[command(flatten)]
    target: TableArgs,

    #[arg(
        long,
        value_enum,
        default_value_t = WritePath::Both,
        help = "Write path to execute"
    )]
    write_path: WritePath,
}

fn parse_example_args() -> Result<(Config, WritePath), Box<dyn Error>> {
    let args = ExampleArgs::parse();
    let config = Config::from_env_and_table_args(args.target)?;
    Ok((config, args.write_path))
}

#[derive(Debug, Serialize)]
struct IngestRequest {
    data: IngestData,
    statement: String,
}

#[derive(Debug, Serialize)]
struct IngestData {
    format: &'static str,
    rows: String,
}

#[derive(Debug, Deserialize)]
struct IngestResponse {
    num_rows_inserted: i64,
}

#[derive(Debug)]
struct HttpIngestError {
    status: reqwest::StatusCode,
    body: String,
}

impl fmt::Display for HttpIngestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "POST /v1/ingest failed with HTTP {}: {}",
            self.status, self.body
        )
    }
}

impl Error for HttpIngestError {}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    if let Err(err) = try_main().await {
        eprintln!("Error: {err}");
        std::process::exit(1);
    }
}

async fn try_main() -> Result<(), Box<dyn Error>> {
    let (config, write_path) = parse_example_args()?;

    // Redirects and transport-level retries are disabled so write requests are never replayed behind the caller's back.
    let http_client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .build()?;

    let client = Client::builder(&config.endpoint)
        .api_key(&config.api_key)
        .http_client(http_client.clone())
        .build()?;

    let ready = client.query("SELECT 1 AS ready").await?;
    let ready_rows = ready.into_objects()?;
    println!("Connected to ScopeDB (ready: {})", ready_rows[0]["ready"]);

    let table = client
        .table(&config.table)
        .with_database(&config.database)
        .with_schema(&config.schema);

    prepare_destination_table(&client, &table).await?;
    println!("Prepared destination table {}", table.identifier());

    if matches!(write_path, WritePath::Ingest | WritePath::Both) {
        let count = ingest_events_http(&http_client, &config, &table, &sample_raw_events()).await?;
        println!("Ingested rows via POST /v1/ingest: {count}");
    }

    if matches!(write_path, WritePath::Append | WritePath::Both) {
        let count = append_events_sdk(&table, &sample_shaped_events()).await?;
        println!("Committed rows via SDK append_stream: {count}");
    }

    let queried_rows = query_recent_events(&client, &table).await?;
    println!(
        "Queried recent checkout events ({} rows):",
        queried_rows.len()
    );
    for row in &queried_rows {
        println!("  {row:?}");
    }

    Ok(())
}

async fn prepare_destination_table(
    client: &Client,
    table: &Table,
) -> Result<(), scopedb_client::Error> {
    let statement = format!(
        "CREATE TABLE IF NOT EXISTS {} (
            time timestamp,
            service string,
            name string,
            message string,
            var object
        )",
        table.identifier()
    );
    client.query(statement).await?;
    Ok(())
}

async fn ingest_events_http(
    http_client: &reqwest::Client,
    config: &Config,
    table: &Table,
    events: &[JsonValue],
) -> Result<i64, Box<dyn Error>> {
    let rows = events
        .iter()
        .map(serde_json::to_string)
        .collect::<Result<Vec<_>, _>>()?
        .join("\n");

    let statement = format!(
        "SELECT
            NOW() AS time,
            $0['service']::string AS service,
            $0['name']::string AS name,
            $0['message']::string AS message,
            $0::object AS var
        WHERE service IS NOT NULL
          AND name IS NOT NULL
        INSERT INTO {}",
        table.identifier()
    );

    let payload = IngestRequest {
        data: IngestData {
            format: "json",
            rows,
        },
        statement,
    };

    let mut base_url = Url::parse(&config.endpoint)?;
    if !base_url.path().ends_with('/') {
        let path = format!("{}/", base_url.path());
        base_url.set_path(&path);
    }
    let ingest_url = base_url.join("v1/ingest")?;

    let mut auth_header = HeaderValue::from_str(&format!("Bearer {}", config.api_key))?;
    auth_header.set_sensitive(true);

    let response = http_client
        .post(ingest_url)
        .header(AUTHORIZATION, auth_header)
        .header(CONTENT_TYPE, "application/json")
        .json(&payload)
        .send()
        .await?;

    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(Box::new(HttpIngestError { status, body }));
    }

    let result: IngestResponse = response.json().await?;
    Ok(result.num_rows_inserted)
}

async fn append_events_sdk(
    table: &Table,
    events: &[JsonValue],
) -> Result<u64, scopedb_client::Error> {
    let stream = table.append_stream().build()?;
    for event in events {
        stream.send(event).await?;
    }
    let report = stream.shutdown().await?;
    Ok(report.committed_rows)
}

async fn query_recent_events(
    client: &Client,
    table: &Table,
) -> Result<Vec<ResultObject>, scopedb_client::Error> {
    let query = format!(
        "FROM {}
        WHERE time >= NOW() - 'PT15m'::interval
          AND service = 'checkout'
        SELECT time, name, message, var['gateway']::string AS gateway
        ORDER BY time DESC
        LIMIT 10",
        table.identifier()
    );
    let result = client.query(query).await?;
    result.into_objects()
}

fn sample_raw_events() -> Vec<JsonValue> {
    vec![
        json!({
            "service": "checkout",
            "name": "payment_failed",
            "message": "Credit card gateway timed out",
            "gateway": "credit_card"
        }),
        json!({
            "service": "checkout",
            "name": "checkout_started",
            "message": "User opened checkout",
            "region": "apac",
            "plan": "pro"
        }),
    ]
}

fn sample_shaped_events() -> Vec<JsonValue> {
    let now = jiff::Timestamp::now().to_string();
    vec![json!({
        "time": now,
        "service": "checkout",
        "name": "payment_failed",
        "message": "Credit card gateway timed out",
        "var": {
            "region": "apac",
            "gateway": "credit_card"
        }
    })]
}
