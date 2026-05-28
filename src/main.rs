use std::collections::HashMap;
use std::io::{Read, Write};
#[allow(unused_imports)]
use std::net::TcpListener;

#[derive(Debug)]
struct Request {
    method: String,
    path: String,
    headers: HashMap<String, String>,
}

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:4221").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");

                let mut buffer = [0; 1024];
                let bytes_read = stream.read(&mut buffer).unwrap();
                let request_bytes = &buffer[..bytes_read];

                match parse_request(request_bytes) {
                    Ok(req) => {
                        println!("Parsed request: {:#?}", req);
                        if req.path == "/" {
                            _ = stream.write_all(b"HTTP/1.1 200 OK\r\n\r\n");
                        } else if req.path.starts_with("/echo/") {
                            let echo_str = &req.path["/echo/".len()..];
                            let len = echo_str.len();
                            let response = format!(
                                "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n{}",
                                len, echo_str
                            );
                            _ = stream.write_all(response.as_bytes());
                        } else if req.path == "/user-agent" {
                            match req.headers.get("User-Agent") {
                                Some(agent) => {
                                    let response = format!(
                                        "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n{}",
                                        agent.len(),
                                        agent
                                    );
                                    _ = stream.write_all(response.as_bytes());
                                }
                                None => _ = stream.write_all(b"HTTP/1.1 404 Not Found\r\n\r\n"),
                            }
                        } else {
                            _ = stream.write_all(b"HTTP/1.1 404 Not Found\r\n\r\n");
                        }
                    }
                    Err(e) => {
                        println!("Failed to parse request: {}", e);
                        _ = stream.write_all(b"HTTP/1.1 400 Bad Request\r\n\r\n");
                    }
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn parse_request(request: &[u8]) -> Result<Request, String> {
    let request_str = std::str::from_utf8(request).map_err(|e| format!("Invalid UTF-8: {}", e))?;

    let mut lines = request_str.split("\r\n");

    // Parse request line: METHOD PATH VERSION
    let request_line = lines.next().ok_or("Empty request")?;
    let mut request_parts = request_line.split_whitespace();

    let method = request_parts.next().ok_or("Missing method")?.to_string();
    let path = request_parts.next().ok_or("Missing path")?.to_string();

    // Parse headers
    let mut headers = HashMap::new();
    for line in lines {
        if line.is_empty() {
            break;
        }
        if let Some((key, value)) = line.split_once(": ") {
            headers.insert(key.to_string(), value.to_string());
        }
    }

    Ok(Request {
        method,
        path,
        headers,
    })
}
