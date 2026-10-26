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

use scopedb_client::StatementEstimatedProgress;
use scopedb_practice::Config;
use scopedb_practice::run_bulk_import;
use serde_json::Value as JsonValue;
use serde_json::json;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;

#[derive(Debug, Clone)]
struct RecordedRequest {
    method: String,
    path: String,
    authorization: Option<String>,
}

async fn read_http_request(stream: &mut TcpStream) -> RecordedRequest {
    let mut buf = Vec::new();
    let mut temp = [0u8; 4096];
    let header_end = loop {
        let n = stream.read(&mut temp).await.unwrap();
        assert!(n > 0, "unexpected EOF while reading HTTP request headers");
        buf.extend_from_slice(&temp[..n]);
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos + 4;
        }
    };

    let header_text = String::from_utf8_lossy(&buf[..header_end]).into_owned();
    let mut lines = header_text.split("\r\n");
    let request_line = lines.next().unwrap();
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap().to_string();
    let path = parts.next().unwrap().to_string();

    let mut content_length = 0usize;
    let mut authorization = None;
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            let name_lower = name.trim().to_ascii_lowercase();
            let value_trimmed = value.trim().to_string();
            if name_lower == "content-length" {
                content_length = value_trimmed.parse().unwrap();
            } else if name_lower == "authorization" {
                authorization = Some(value_trimmed);
            }
        }
    }

    while buf.len() < header_end + content_length {
        let n = stream.read(&mut temp).await.unwrap();
        assert!(n > 0, "unexpected EOF while reading HTTP request body");
        buf.extend_from_slice(&temp[..n]);
    }

    RecordedRequest {
        method,
        path,
        authorization,
    }
}

async fn write_json_response(stream: &mut TcpStream, status: u16, body: &JsonValue) {
    let payload = serde_json::to_vec(body).unwrap();
    let reason = match status {
        200 => "OK",
        _ => "Error",
    };
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        payload.len()
    );
    stream.write_all(response.as_bytes()).await.unwrap();
    stream.write_all(&payload).await.unwrap();
    stream.flush().await.unwrap();
}

#[tokio::test]
async fn end_to_end_bulk_import_workflow() {
    let bulk_rows = 50usize;
    let max_batch_rows = 25usize;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let mut statement_seq = 0usize;
        let mut append_batches = 0usize;
        for _ in 0..4 {
            let (mut socket, _) = listener.accept().await.unwrap();
            let req = read_http_request(&mut socket).await;
            assert_eq!(req.authorization.as_deref(), Some("Bearer bulk-secret-key"));

            match (req.method.as_str(), req.path.as_str()) {
                ("POST", "/v1/statements") => {
                    let current = statement_seq;
                    statement_seq += 1;
                    let response = if current == 0 {
                        json!({
                            "statement_id": "00000000-0000-0000-0000-000000000011",
                            "status": "finished",
                            "created_at": "2026-10-26T06:00:00Z",
                            "progress": StatementEstimatedProgress::default(),
                            "result_set": {
                                "metadata": {
                                    "fields": [{ "name": "ready", "data_type": "int" }],
                                    "num_rows": 1
                                },
                                "format": "json",
                                "rows": [["1"]]
                            }
                        })
                    } else {
                        json!({
                            "statement_id": "00000000-0000-0000-0000-000000000012",
                            "status": "finished",
                            "created_at": "2026-10-26T06:00:01Z",
                            "progress": StatementEstimatedProgress::default(),
                            "result_set": {
                                "metadata": {
                                    "fields": [],
                                    "num_rows": 0
                                },
                                "format": "json",
                                "rows": []
                            }
                        })
                    };
                    write_json_response(&mut socket, 200, &response).await;
                }
                ("POST", "/v1/catalog/databases/scopedb/schemas/public/tables/events/ingest") => {
                    append_batches += 1;
                    write_json_response(
                        &mut socket,
                        200,
                        &json!({ "status": "committed", "num_rows_inserted": max_batch_rows }),
                    )
                    .await;
                }
                (method, path) => panic!("unexpected request: {method} {path}"),
            }
        }
        assert_eq!(append_batches, 2);
    });

    let backup_path =
        std::env::temp_dir().join(format!("scopedb_bulk_backup_{}.ndjson", std::process::id()));
    let _ = std::fs::remove_file(&backup_path);

    let config = Config::from_credentials_and_iter(
        format!("http://{addr}"),
        "bulk-secret-key".to_string(),
        [
            "--database",
            "scopedb",
            "--schema",
            "public",
            "--table",
            "events",
            "--bulk-rows",
            &bulk_rows.to_string(),
            "--backup-file",
            backup_path.to_str().unwrap(),
            "--max-batch-rows",
            &max_batch_rows.to_string(),
            "--flush-interval",
            "250ms",
            "--batch-bytes",
            "65536",
        ],
    )
    .unwrap();

    let report = run_bulk_import(&config).await.unwrap();
    server.await.unwrap();

    assert_eq!(report.rows_generated, bulk_rows);
    assert_eq!(report.committed_rows, bulk_rows as u64);
    assert_eq!(report.backup_file, backup_path);

    let content = std::fs::read_to_string(&backup_path).unwrap();
    std::fs::remove_file(&backup_path).unwrap();

    let lines: Vec<&str> = content.lines().collect();
    assert_eq!(lines.len(), bulk_rows);

    let mut saw_null_string_field = false;
    let mut saw_non_null_string_field = false;
    let mut saw_array_in_var = false;
    let mut saw_object_in_var = false;

    for line in lines {
        let row: JsonValue = serde_json::from_str(line).unwrap();
        assert!(row["time"].is_string());

        for field in ["service", "name", "message"] {
            if row[field].is_null() {
                saw_null_string_field = true;
            } else if row[field].is_string() {
                saw_non_null_string_field = true;
            } else {
                panic!("unexpected type for {field}: {}", row[field]);
            }
        }

        assert!(row["var"].is_object());
        if row["var"]["payload"].is_array() {
            saw_array_in_var = true;
        } else if row["var"]["payload"].is_object() {
            saw_object_in_var = true;
        } else {
            panic!("unexpected var.payload: {}", row["var"]["payload"]);
        }
    }

    assert!(saw_null_string_field);
    assert!(saw_non_null_string_field);
    assert!(saw_array_in_var);
    assert!(saw_object_in_var);
}
