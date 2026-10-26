# ScopeDB Event Ingestion Practice (Rust)

A Rust application that ingests sample event data into a ScopeDB workspace and queries it back, following the official [Ingest Data Guide](https://www.scopedb.io/docs/guides/ingest-events) and [Rust SDK Guide](https://www.scopedb.io/docs/developer/rust).

## Prerequisites

- **Rust**: `1.91.0` or later (`cargo` and `rustc`)
- **ScopeDB Workspace**: Access to a workspace in the ScopeDB Console

## Obtaining Workspace Credentials

Before running the program, retrieve two values from your ScopeDB Console (see [Connect an Application](https://www.scopedb.io/docs/guides/connect-to-scopedb)):

1. **ScopeDB API Endpoint (`SCOPEDB_ENDPOINT`)**:
   - Open your target workspace in ScopeDB Console and go to **Connect**.
   - Under **ScopeDB API**, copy the HTTPS base URL (e.g., `https://<endpoint>`).
2. **API Key (`SCOPEDB_API_KEY`)**:
   - From **Connect**, choose **Manage keys** (or open **API Keys** for the workspace).
   - Create a new API key and copy the secret immediately.

## Environment Variables

All workspace-specific settings are configured via environment variables:

| Variable | Required | Default | Description |
| :--- | :--- | :--- | :--- |
| `SCOPEDB_ENDPOINT` | **Yes** | — | HTTPS base URL of the ScopeDB API for your workspace (e.g., `https://<endpoint>`). |
| `SCOPEDB_API_KEY` | **Yes** | — | Bearer API key secret created for your workspace. |
| `SCOPEDB_DATABASE` | No | `scopedb` | Target database in your workspace. |
| `SCOPEDB_SCHEMA` | No | `public` | Target schema in the database. |
| `SCOPEDB_TABLE` | No | `events` | Destination table to create, ingest into, and query. |
| `SCOPEDB_BULK_ROWS` | No | `10000` | Number of random event rows (`N`) to generate and import in bulk mode (`cargo run`). |
| `SCOPEDB_BACKUP_FILE` | No | `events.ndjson` | Local file path where the generated `N`-row NDJSON backup is saved before streaming. |
| `SCOPEDB_WRITE_PATH` | No | `both` | Write path for the `basic_ingest` example: `ingest` (`POST /v1/ingest` with ScopeQL transform), `append` (SDK `Table::append_stream`), or `both`. |

## Usage

### 1. Bulk Import Mode (Default Binary)

Generates `N` random events (where `string` columns `service`, `name`, and `message` can randomly be `null`, and `var` is a JSON object whose nested fields randomly contain arrays or JSON objects), saves them to a local NDJSON backup file, streams all rows via the SDK `Table::append_stream`, and prints the total elapsed time:

```bash
export SCOPEDB_ENDPOINT="https://<your-workspace-endpoint>"
export SCOPEDB_API_KEY="<your-api-key>"

# Optional bulk import settings:
export SCOPEDB_BULK_ROWS="10000"
export SCOPEDB_BACKUP_FILE="events.ndjson"

cargo run
```

### 2. Basic Ingestion Example (`examples/basic_ingest.rs`)

Runs the step-by-step official guide workflow (connectivity check, table creation, `POST /v1/ingest` transform ingestion, SDK `append_stream`, and querying recent `checkout` events):

```bash
export SCOPEDB_ENDPOINT="https://<your-workspace-endpoint>"
export SCOPEDB_API_KEY="<your-api-key>"
export SCOPEDB_WRITE_PATH="both"

cargo run --example basic_ingest
```

## What the Program Does

1. **Verifies Connectivity**: Connects to `SCOPEDB_ENDPOINT` using `SCOPEDB_API_KEY` and executes `SELECT 1 AS ready`.
2. **Prepares the Destination Table**: Creates the destination table if it does not already exist:
   ```sql
   CREATE TABLE IF NOT EXISTS `scopedb`.`public`.`events` (
       time timestamp,
       service string,
       name string,
       message string,
       var object
   )
   ```
3. **Ingests Events**:
   - **Transform Ingest (`POST /v1/ingest`)**: Sends raw JSON events and reshapes each row (`$0`) on the server with ScopeQL before inserting into the destination table:
     ```sql
     SELECT
         NOW() AS time,
         $0['service']::string AS service,
         $0['name']::string AS name,
         $0['message']::string AS message,
         $0::object AS var
     WHERE service IS NOT NULL
       AND name IS NOT NULL
     INSERT INTO `scopedb`.`public`.`events`
     ```
   - **SDK Append Stream (`Table::append_stream`)**: Streams pre-shaped event rows matching the destination table schema via `scopedb-client`.
4. **Queries the New Rows**: Fetches the newly ingested `checkout` events from the last 15 minutes:
   ```sql
   FROM `scopedb`.`public`.`events`
   WHERE time >= NOW() - 'PT15m'::interval
     AND service = 'checkout'
   SELECT time, name, message, var['gateway']::string AS gateway
   ORDER BY time DESC
   LIMIT 10
   ```

## Querying Ingested Data with ScopeDB CLI (`scope`)

You can also inspect and query the ingested rows directly from your terminal using the official [ScopeDB CLI (`scope`)](https://www.scopedb.io/docs/developer/scopedb-cli).

### 1. Install `scope`

**Download a release archive** (for example, macOS Apple Silicon `v0.2.0`):

```bash
curl -LO https://github.com/scopedb/scopedb-cli/releases/download/v0.2.0/scope_0.2.0_darwin_arm64.tar.gz
tar -xzf scope_0.2.0_darwin_arm64.tar.gz scope
mkdir -p "$HOME/.local/bin"
install -m 0755 scope "$HOME/.local/bin/scope"
export PATH="$HOME/.local/bin:$PATH"
scope version
```

*(For other platforms, see the [official release downloads](https://www.scopedb.io/docs/developer/scopedb-cli#download-a-release), or build from source with Go 1.27+: `go install github.com/scopedb/scopedb-cli/cmd/scope@v0.2.0`.)*

### 2. Connect to Your Workspace

`scope` supports two authentication methods (see [Connect the CLI](https://www.scopedb.io/docs/developer/scopedb-cli/connect) and [Automate CLI Commands](https://www.scopedb.io/docs/developer/scopedb-cli/automation)):

- **Option A — Reuse Environment Variables**:
  If `SCOPEDB_ENDPOINT` and `SCOPEDB_API_KEY` are already exported in your shell (for running this Rust project), `scope` uses them automatically without requiring a separate login:
  ```bash
  scope auth status
  scope doctor
  scope query 'SELECT 1 AS ready'
  ```

- **Option B — Interactive Browser / Device Login**:
  If you prefer interactive login (`unset SCOPEDB_ENDPOINT SCOPEDB_API_KEY` first so environment variables do not override the login):
  ```bash
  scope auth login
  # Or in SSH / headless environments:
  # scope auth login --use-device-code --no-browser

  scope workspace list
  scope workspace use '<workspace-id-or-name>'
  scope auth status
  scope doctor
  ```

### 3. Run Queries with `scope query`

Sample ScopeQL queries (see [Query Data](https://www.scopedb.io/docs/guides/query-events) and [Query Data with the CLI](https://www.scopedb.io/docs/developer/scopedb-cli/query)) are saved in local `.scopeql` files:

- **[`query.scopeql`](./query.scopeql)** — Inspect recent `checkout` events:
  ```sql
  FROM scopedb.public.events
  WHERE time >= NOW() - 'PT15m'::interval
    AND service = 'checkout'
  SELECT time, name, message, var['gateway']::string AS gateway
  ORDER BY time DESC
  LIMIT 10
  ```
- **[`aggregate.scopeql`](./aggregate.scopeql)** — Aggregate event counts by `service` and `name`:
  ```sql
  FROM events
  WHERE time >= NOW() - 'PT15m'::interval
  GROUP BY service, name
  AGGREGATE count() AS events
  ORDER BY events DESC
  ```
- **[`join.scopeql`](./join.scopeql)** — Join events with service ownership metadata and aggregate by `team` and `name`:
  ```sql
  FROM events AS e
  JOIN (
      VALUES ('checkout', 'payments')
      SELECT $0 AS name, $1 AS team
  ) AS s ON e.service = s.name
  WHERE e.time >= NOW() - 'PT24h'::interval
  GROUP BY s.team, e.name
  AGGREGATE count() AS events
  ORDER BY events DESC
  ```

*(If you customized `SCOPEDB_DATABASE`, `SCOPEDB_SCHEMA`, or `SCOPEDB_TABLE`, update the table identifier in the `.scopeql` files accordingly.)*

Once connected, execute any query file with `scope query -f`:

- **Default `table` output format**:
  ```bash
  scope query -f query.scopeql
  scope query -f aggregate.scopeql
  scope query -f join.scopeql
  ```

- **Format output as `json`, `jsonl`, or `csv`**:
  ```bash
  scope query -f query.scopeql --format json
  scope query -f aggregate.scopeql --format jsonl
  scope query -f join.scopeql --format csv > events.csv
  ```

## Running Tests

Run the end-to-end integration test suite (uses a local mock server, no external workspace credentials required):

```bash
cargo test
```
