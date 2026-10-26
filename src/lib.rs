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

pub const DEFAULT_BULK_ROWS: usize = 10_000;
pub const DEFAULT_BACKUP_FILE: &str = "events.ndjson";

#[derive(Debug, Clone)]
pub struct Config {
    pub endpoint: String,
    pub api_key: String,
    pub database: String,
    pub schema: String,
    pub table: String,
    pub bulk_rows: usize,
    pub backup_file: PathBuf,
}

impl Config {
    pub fn from_env() -> Result<Self, Box<dyn Error>> {
        let endpoint = required_env("SCOPEDB_ENDPOINT")?;
        let api_key = required_env("SCOPEDB_API_KEY")?;
        let database = optional_env("SCOPEDB_DATABASE").unwrap_or_else(|| "scopedb".to_string());
        let schema = optional_env("SCOPEDB_SCHEMA").unwrap_or_else(|| "public".to_string());
        let table = optional_env("SCOPEDB_TABLE").unwrap_or_else(|| "events".to_string());
        let bulk_rows = match optional_env("SCOPEDB_BULK_ROWS") {
            Some(val) => {
                let parsed: usize = val.parse().map_err(|_| {
                    format!("invalid SCOPEDB_BULK_ROWS '{val}'; expected a positive integer")
                })?;
                if parsed == 0 {
                    return Err("SCOPEDB_BULK_ROWS must be greater than 0".into());
                }
                parsed
            }
            None => DEFAULT_BULK_ROWS,
        };
        let backup_file = optional_env("SCOPEDB_BACKUP_FILE")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(DEFAULT_BACKUP_FILE));

        Ok(Self {
            endpoint,
            api_key,
            database,
            schema,
            table,
            bulk_rows,
            backup_file,
        })
    }
}

fn required_env(key: &str) -> Result<String, String> {
    optional_env(key).ok_or_else(|| format!("environment variable {key} is required"))
}

fn optional_env(key: &str) -> Option<String> {
    env::var(key)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
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
    let stream = table.append_stream().build()?;
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
