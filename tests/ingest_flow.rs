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

use std::sync::Arc;

use scopedb_client::StatementEstimatedProgress;
use scopedb_client::Value;
use scopedb_practice::Config;
use scopedb_practice::WritePath;
use scopedb_practice::run;
use serde_json::Value as JsonValue;
use serde_json::json;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
struct RecordedRequest {
    method: String,
    path: String,
    authorization: Option<String>,
    body: Vec<u8>,
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
        body: buf[header_end..header_end + content_length].to_vec(),
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
async fn end_to_end_ingest_and_append_workflow() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let recorded = Arc::new(Mutex::new(Vec::<RecordedRequest>::new()));
    let recorded_server = Arc::clone(&recorded);

    let server = tokio::spawn(async move {
        let mut statement_seq = 0usize;
        for _ in 0..5 {
            let (mut socket, _) = listener.accept().await.unwrap();
            let req = read_http_request(&mut socket).await;
            recorded_server.lock().await.push(req.clone());

            match (req.method.as_str(), req.path.as_str()) {
                ("POST", "/v1/statements") => {
                    let current = statement_seq;
                    statement_seq += 1;
                    let response = if current == 0 {
                        json!({
                            "statement_id": "00000000-0000-0000-0000-000000000001",
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
                    } else if current == 1 {
                        json!({
                            "statement_id": "00000000-0000-0000-0000-000000000002",
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
                    } else {
                        json!({
                            "statement_id": "00000000-0000-0000-0000-000000000003",
                            "status": "finished",
                            "created_at": "2026-10-26T06:00:02Z",
                            "progress": StatementEstimatedProgress::default(),
                            "result_set": {
                                "metadata": {
                                    "fields": [
                                        { "name": "time", "data_type": "timestamp" },
                                        { "name": "name", "data_type": "string" },
                                        { "name": "message", "data_type": "string" },
                                        { "name": "gateway", "data_type": "string" }
                                    ],
                                    "num_rows": 1
                                },
                                "format": "json",
                                "rows": [[
                                    "2026-10-26T06:00:00Z",
                                    "payment_failed",
                                    "Credit card gateway timed out",
                                    "credit_card"
                                ]]
                            }
                        })
                    };
                    write_json_response(&mut socket, 200, &response).await;
                }
                ("POST", "/v1/ingest") => {
                    write_json_response(&mut socket, 200, &json!({ "num_rows_inserted": 2 })).await;
                }
                (
                    "POST",
                    "/v1/catalog/databases/analytics/schemas/raw/tables/checkout_events/ingest",
                ) => {
                    write_json_response(
                        &mut socket,
                        200,
                        &json!({ "status": "committed", "num_rows_inserted": 1 }),
                    )
                    .await;
                }
                (method, path) => panic!("unexpected request: {method} {path}"),
            }
        }
    });

    let config = Config {
        endpoint: format!("http://{addr}"),
        api_key: "test-secret-api-key".to_string(),
        database: "analytics".to_string(),
        schema: "raw".to_string(),
        table: "checkout_events".to_string(),
        write_path: WritePath::Both,
    };

    let report = run(&config).await.unwrap();
    server.await.unwrap();

    assert_eq!(report.ingested_rows, Some(2));
    assert_eq!(report.appended_rows, Some(1));
    assert_eq!(report.queried_rows.len(), 1);
    assert_eq!(
        report.queried_rows[0]["name"],
        Value::String("payment_failed".to_string())
    );
    assert_eq!(
        report.queried_rows[0]["gateway"],
        Value::String("credit_card".to_string())
    );

    let requests = recorded.lock().await;
    assert_eq!(requests.len(), 5);
    for req in requests.iter() {
        assert_eq!(
            req.authorization.as_deref(),
            Some("Bearer test-secret-api-key")
        );
    }

    let ingest_payload: JsonValue = serde_json::from_slice(&requests[2].body).unwrap();
    assert_eq!(ingest_payload["data"]["format"], "json");
    assert!(
        ingest_payload["data"]["rows"]
            .as_str()
            .unwrap()
            .contains("\"payment_failed\"")
    );
    assert!(
        ingest_payload["statement"]
            .as_str()
            .unwrap()
            .contains("INSERT INTO `analytics`.`raw`.`checkout_events`")
    );
}
