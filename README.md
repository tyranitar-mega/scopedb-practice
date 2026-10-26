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
| `SCOPEDB_WRITE_PATH` | No | `both` | Write path to execute: `ingest` (`POST /v1/ingest` with ScopeQL transform), `append` (SDK `Table::append_stream`), or `both`. |

## Usage

1. Export the required environment variables in your shell:

```bash
export SCOPEDB_ENDPOINT="https://<your-workspace-endpoint>"
export SCOPEDB_API_KEY="<your-api-key>"
```

*(Optional)* Override the target location or write path if needed:

```bash
export SCOPEDB_DATABASE="scopedb"
export SCOPEDB_SCHEMA="public"
export SCOPEDB_TABLE="events"
export SCOPEDB_WRITE_PATH="both"
```

2. Run the ingestion workflow:

```bash
cargo run
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

## Running Tests

Run the end-to-end integration test suite (uses a local mock server, no external workspace credentials required):

```bash
cargo test
```
