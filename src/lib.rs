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

use std::env;
use std::error::Error;
use std::fs::File;
use std::io::BufWriter;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use scopedb_client::Client;
use scopedb_client::Table;
use serde_json::Value as JsonValue;
use serde_json::json;

pub const DEFAULT_DATABASE: &str = "scopedb";
pub const DEFAULT_SCHEMA: &str = "public";
pub const DEFAULT_TABLE: &str = "events";
pub const DEFAULT_BULK_ROWS: usize = 10_000;
pub const DEFAULT_BACKUP_FILE: &str = "events.ndjson";
pub const DEFAULT_BATCH_BYTES: usize = 4 * 1024 * 1024;
pub const DEFAULT_MAX_BATCH_ROWS: usize = 200_000;
pub const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub endpoint: String,
    pub api_key: String,
    pub database: String,
    pub schema: String,
    pub table: String,
    pub bulk_rows: usize,
    pub backup_file: PathBuf,
    pub batch_bytes: usize,
    pub max_batch_rows: usize,
    pub flush_interval: Duration,
}

impl Config {
    pub fn from_env_and_args() -> Result<Self, Box<dyn Error>> {
        Self::from_env_and_iter(env::args().skip(1))
    }

    pub fn from_env_and_iter<I, S>(args: I) -> Result<Self, Box<dyn Error>>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let endpoint = required_env("SCOPEDB_ENDPOINT")?;
        let api_key = required_env("SCOPEDB_API_KEY")?;
        Self::from_credentials_and_iter(endpoint, api_key, args)
    }

    pub fn from_credentials_and_iter<I, S>(
        endpoint: String,
        api_key: String,
        args: I,
    ) -> Result<Self, Box<dyn Error>>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut config = Self {
            endpoint,
            api_key,
            database: DEFAULT_DATABASE.to_string(),
            schema: DEFAULT_SCHEMA.to_string(),
            table: DEFAULT_TABLE.to_string(),
            bulk_rows: DEFAULT_BULK_ROWS,
            backup_file: PathBuf::from(DEFAULT_BACKUP_FILE),
            batch_bytes: DEFAULT_BATCH_BYTES,
            max_batch_rows: DEFAULT_MAX_BATCH_ROWS,
            flush_interval: DEFAULT_FLUSH_INTERVAL,
        };

        let mut iter = args.into_iter().map(Into::into);
        while let Some(arg) = iter.next() {
            if arg == "--help" || arg == "-h" {
                return Err(usage_text().into());
            }
            let (flag, inline_val) = match arg.split_once('=') {
                Some((f, v)) => (f, Some(v.to_string())),
                None => (arg.as_str(), None),
            };
            let mut next_val = || -> Result<String, Box<dyn Error>> {
                if let Some(v) = inline_val.clone() {
                    return Ok(v);
                }
                iter.next()
                    .ok_or_else(|| format!("missing value for argument '{flag}'").into())
            };

            match flag {
                "--database" => {
                    config.database = non_empty_arg(flag, next_val()?)?;
                }
                "--schema" => {
                    config.schema = non_empty_arg(flag, next_val()?)?;
                }
                "--table" => {
                    config.table = non_empty_arg(flag, next_val()?)?;
                }
                "--bulk-rows" | "-n" => {
                    config.bulk_rows = parse_positive_usize(flag, &next_val()?)?;
                }
                "--backup-file" => {
                    config.backup_file = PathBuf::from(non_empty_arg(flag, next_val()?)?);
                }
                "--batch-bytes" => {
                    config.batch_bytes = parse_positive_usize(flag, &next_val()?)?;
                }
                "--max-batch-rows" => {
                    config.max_batch_rows = parse_positive_usize(flag, &next_val()?)?;
                }
                "--flush-interval" => {
                    config.flush_interval = parse_duration(flag, &next_val()?)?;
                }
                other => {
                    return Err(format!("unknown argument '{other}'\n\n{}", usage_text()).into());
                }
            }
        }

        Ok(config)
    }
}

fn usage_text() -> &'static str {
    "Usage: scopedb-practice [OPTIONS]

Required environment variables:
  SCOPEDB_ENDPOINT              HTTPS base URL of the ScopeDB workspace API
  SCOPEDB_API_KEY               Bearer API key for the workspace

Optional arguments:
  --database <NAME>             Target database name [default: scopedb]
  --schema <NAME>               Target schema name [default: public]
  --table <NAME>                Destination table name [default: events]
  -n, --bulk-rows <N>           Number of random event rows to generate [default: 10000]
  --backup-file <PATH>          Local NDJSON backup file path [default: events.ndjson]
  --batch-bytes <BYTES>         Target NDJSON batch size in bytes [default: 4194304]
  --max-batch-rows <ROWS>       Maximum rows per HTTP batch [default: 200000]
  --flush-interval <DURATION>   Maximum flush delay, e.g. 500ms, 1s [default: 1s]"
}

fn non_empty_arg(flag: &str, value: String) -> Result<String, Box<dyn Error>> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(format!("argument '{flag}' must not be empty").into());
    }
    Ok(trimmed.to_string())
}

fn parse_positive_usize(flag: &str, raw: &str) -> Result<usize, Box<dyn Error>> {
    let parsed: usize = raw
        .trim()
        .parse()
        .map_err(|_| format!("invalid {flag} '{raw}'; expected a positive integer"))?;
    if parsed == 0 {
        return Err(format!("{flag} must be greater than 0").into());
    }
    Ok(parsed)
}

fn parse_duration(flag: &str, raw: &str) -> Result<Duration, Box<dyn Error>> {
    let trimmed = raw.trim();
    if let Ok(millis) = trimmed.parse::<u64>() {
        if millis == 0 {
            return Err(format!("{flag} must be greater than 0").into());
        }
        return Ok(Duration::from_millis(millis));
    }
    let signed: jiff::SignedDuration = trimmed.parse().map_err(|_| {
        format!(
            "invalid {flag} '{raw}'; expected a duration such as '500ms', '1s', or milliseconds"
        )
    })?;
    let duration =
        Duration::try_from(signed).map_err(|_| format!("{flag} must be a positive duration"))?;
    if duration.is_zero() {
        return Err(format!("{flag} must be greater than 0").into());
    }
    Ok(duration)
}

pub fn required_env(key: &str) -> Result<String, String> {
    env::var(key)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .ok_or_else(|| format!("environment variable {key} is required"))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BulkImportReport {
    pub rows_generated: usize,
    pub backup_file: PathBuf,
    pub committed_rows: u64,
    pub elapsed: Duration,
}

pub async fn run_bulk_import(config: &Config) -> Result<BulkImportReport, Box<dyn Error>> {
    let client = Client::builder(&config.endpoint)
        .api_key(&config.api_key)
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

    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0x9e37_79b9_7f4a_7c15);
    let events = generate_random_events(config.bulk_rows, seed);
    write_ndjson_backup(&config.backup_file, &events)?;
    println!(
        "Generated {} random events and saved NDJSON backup to {}",
        events.len(),
        config.backup_file.display()
    );

    let start = Instant::now();
    let stream = table
        .append_stream()
        .target_batch_bytes(config.batch_bytes)
        .max_batch_rows(config.max_batch_rows)
        .flush_interval(config.flush_interval)
        .build()?;
    let admission_error = match stream.send_all(&events).await {
        Ok(admitted) => {
            println!(
                "Admitted {} rows into SDK append_stream",
                admitted.accepted_rows
            );
            None
        }
        Err(err) => {
            // Settle the already admitted prefix before returning the admission error.
            eprintln!("Admission stopped early ({err}); settling admitted prefix...");
            Some(err)
        }
    };
    let report = stream.shutdown().await?;
    if let Some(err) = admission_error {
        return Err(err.into());
    }
    let elapsed = start.elapsed();

    println!(
        "Bulk import finished: committed {} rows in {:.3}s",
        report.committed_rows,
        elapsed.as_secs_f64()
    );

    Ok(BulkImportReport {
        rows_generated: events.len(),
        backup_file: config.backup_file.clone(),
        committed_rows: report.committed_rows,
        elapsed,
    })
}

pub fn generate_random_events(count: usize, seed: u64) -> Vec<JsonValue> {
    const SERVICES: &[&str] = &[
        "checkout",
        "auth",
        "inventory",
        "billing",
        "shipping",
        "notification",
    ];
    const NAMES: &[&str] = &[
        "payment_failed",
        "checkout_started",
        "order_placed",
        "token_refreshed",
        "stock_reserved",
        "webhook_dispatched",
    ];
    const MESSAGES: &[&str] = &[
        "Credit card gateway timed out",
        "User opened checkout",
        "Order committed to fulfillment queue",
        "Session token rotated",
        "Inventory reservation succeeded",
        "Webhook delivered to downstream endpoint",
    ];
    const REGIONS: &[&str] = &["apac", "emea", "us-east", "us-west"];
    const GATEWAYS: &[&str] = &["credit_card", "paypal", "apple_pay", "stripe"];

    let mut rng = SplitMix64::new(seed);
    let base_time = jiff::Timestamp::now();
    let mut events = Vec::with_capacity(count);

    for i in 0..count {
        let offset_millis = (rng.next_u64() % 600_000) as i64;
        let ts = base_time
            .checked_sub(jiff::SignedDuration::from_millis(offset_millis))
            .unwrap_or(base_time);

        let service = if rng.next_u64().is_multiple_of(5) {
            JsonValue::Null
        } else {
            JsonValue::String(SERVICES[rng.choose_index(SERVICES.len())].to_string())
        };

        let name = if rng.next_u64().is_multiple_of(5) {
            JsonValue::Null
        } else {
            JsonValue::String(NAMES[rng.choose_index(NAMES.len())].to_string())
        };

        let message = if rng.next_u64().is_multiple_of(4) {
            JsonValue::Null
        } else {
            JsonValue::String(MESSAGES[rng.choose_index(MESSAGES.len())].to_string())
        };

        let region = REGIONS[rng.choose_index(REGIONS.len())];
        let gateway = GATEWAYS[rng.choose_index(GATEWAYS.len())];

        let var = if rng.next_u64().is_multiple_of(2) {
            json!({
                "region": region,
                "gateway": gateway,
                "payload": [
                    format!("step_{}", (i % 4) + 1),
                    gateway,
                    (rng.next_u64() % 500) as i64
                ],
                "tags": ["bulk", region]
            })
        } else {
            json!({
                "region": region,
                "gateway": gateway,
                "payload": {
                    "attempt": ((rng.next_u64() % 3) + 1) as i64,
                    "latency_ms": (rng.next_u64() % 1500) as i64,
                    "retriable": rng.next_u64().is_multiple_of(2)
                }
            })
        };

        events.push(json!({
            "time": ts.to_string(),
            "service": service,
            "name": name,
            "message": message,
            "var": var
        }));
    }

    events
}

pub fn write_ndjson_backup(path: &Path, events: &[JsonValue]) -> Result<(), Box<dyn Error>> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    let file = File::create(path)?;
    let mut writer = BufWriter::new(file);
    for event in events {
        serde_json::to_writer(&mut writer, event)?;
        writer.write_all(b"\n")?;
    }
    writer.flush()?;
    Ok(())
}

struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn choose_index(&mut self, len: usize) -> usize {
        (self.next_u64() as usize) % len
    }
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
