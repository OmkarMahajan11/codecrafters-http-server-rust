use futures::future::{Either, select};
use smol::channel::bounded;
use smol::fs::File;
use smol::io::{AsyncReadExt, AsyncWriteExt, Result};
use smol::net::{TcpListener, TcpStream};
use std::collections::HashMap;
use std::env;
use std::path::Path;
use std::pin::pin;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

#[derive(Debug)]
struct Request {
    method: String,
    path: String,
    headers: HashMap<String, String>,
    body: Option<String>,
}

pub static DIR: OnceLock<String> = OnceLock::new();

fn main() -> Result<()> {
    let args = env::args().skip(1).collect::<Vec<String>>();

    let dir = match args.windows(2).find(|w| w[0] == "--directory") {
        Some(w) => w[1].clone(),
        None => {
            if args.last().map_or(false, |arg| arg == "--directory") {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "--directory flag given without a value",
                ));
            }
            String::new()
        }
    };

    if dir != "" {
        std::fs::create_dir_all(&dir).expect("Could not create directory");
        DIR.set(dir).expect("Failed to set global var");
    }

    let (shutdown_tx, shutdown_rx) = bounded(1);

    ctrlc::set_handler(move || {
        println!("Shutting down...");
        let _ = shutdown_tx.try_send(());
    })
    .expect("Error setting Ctrl-C handler");

    smol::block_on(async {
        let listener = TcpListener::bind("127.0.0.1:4221").await?;
        let active_connections = Arc::new(AtomicUsize::new(0));

        loop {
            let accept_fut = pin!(listener.accept());
            let shutdown_fut = pin!(shutdown_rx.recv());

            match select(accept_fut, shutdown_fut).await {
                Either::Left((Ok((stream, _)), _)) => {
                    let counter = active_connections.clone();
                    counter.fetch_add(1, Ordering::Relaxed);

                    smol::spawn(async move {
                        if let Err(e) = handle_connection(stream).await {
                            eprintln!("Handle connection error: {}", e);
                        }
                        counter.fetch_sub(1, Ordering::Relaxed);
                    })
                    .detach();
                }
                Either::Left((Err(e), _)) => {
                    eprintln!("Listener accept error: {}", e);
                }
                Either::Right(_) => {
                    break;
                }
            }
        }

        println!("Waiting for all connection to close...");
        while active_connections.load(Ordering::Relaxed) > 0 {
            smol::Timer::after(Duration::from_millis(50)).await;
        }

        println!("All connections closed. Shutting down.");

        Ok(())
    })
}

async fn handle_connection(mut stream: TcpStream) -> smol::io::Result<()> {
    println!("accepted new connection");

    let mut buffer = [0; 1024];
    let bytes_read = stream.read(&mut buffer).await?;
    if bytes_read == 0 {
        return Ok(());
    }

    let request_bytes = &buffer[..bytes_read];

    match parse_request(request_bytes) {
        Ok(req) => {
            //println!("Parsed request: {:#?}", req);
            match req.method.as_str() {
                "GET" => handle_get(&req, &mut stream).await?,
                "POST" => handle_post(&req, &mut stream).await?,
                _ => stream.write_all(b"HTTP/1.1 404 Not Found\r\n\r\n").await?,
            }
        }
        Err(e) => {
            println!("Failed to parse request: {}", e);
            stream
                .write_all(b"HTTP/1.1 400 Bad Request\r\n\r\n")
                .await?;
        }
    }
    Ok(())
}

fn parse_request(request: &[u8]) -> std::result::Result<Request, String> {
    let request_str = std::str::from_utf8(request).map_err(|e| format!("Invalid UTF-8: {}", e))?;

    let mut lines = request_str.split("\r\n");

    let request_line = lines.next().take().ok_or("Empty request")?;
    let mut request_parts = request_line.split_whitespace();

    let method = request_parts.next().ok_or("Missing method")?.to_string();
    let path = request_parts.next().ok_or("Missing path")?.to_string();

    let headers = lines
        .by_ref()
        .take_while(|l| l.contains(": "))
        .filter_map(|l| l.split_once(": "))
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();

    let body = lines.last().map(String::from);

    Ok(Request {
        method,
        path,
        headers,
        body,
    })
}

async fn handle_get(req: &Request, stream: &mut TcpStream) -> Result<()> {
    let path = req.path.as_str();

    match path {
        "/" => {
            stream.write_all(b"HTTP/1.1 200 OK\r\n\r\n").await?;
        }
        "/user-agent" => match req.headers.get("User-Agent") {
            Some(agent) => {
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n{}",
                    agent.len(),
                    agent
                );
                stream.write_all(resp.as_bytes()).await?;
            }
            None => stream.write_all(b"HTTP/1.1 404 Not Found\r\n\r\n").await?,
        },
        p if p.starts_with("/echo/") => {
            let echo_str = &p["/echo/".len()..];
            let len = echo_str.len();
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n{}",
                len, echo_str
            );
            stream.write_all(resp.as_bytes()).await?;
        }
        p if p.starts_with("/files/") => {
            let dir = DIR.get().expect("Expected a directory value");
            if dir == "" {
                stream.write_all(b"HTTP/1.1 404 Not Found\r\n\r\n").await?;
                return Ok(());
            }

            let file_str = &p["/files/".len()..];
            let path_str = format!("{}/{}", dir, file_str);
            let path = Path::new(&path_str);

            let mut buf = String::new();
            match File::open(path).await {
                Ok(mut f) => {
                    f.read_to_string(&mut buf).await?;
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: {}\r\n\r\n{}",
                        buf.len(),
                        &buf
                    );
                    stream.write_all(resp.as_bytes()).await?;
                }
                Err(_) => stream.write_all(b"HTTP/1.1 404 Not Found\r\n\r\n").await?,
            }
        }
        _ => {
            stream.write_all(b"HTTP/1.1 404 Not Found\r\n\r\n").await?;
        }
    }

    Ok(())
}

async fn handle_post(req: &Request, stream: &mut TcpStream) -> Result<()> {
    match &req.path {
        p if p.starts_with("/files/") => {
            let dir = DIR.get().expect("Expected a directory value");
            let file_str = &p["/files/".len()..];

            let path_str = format!("{}/{}", dir, file_str);
            let path = Path::new(&path_str);
            match File::create(path).await {
                Ok(mut f) => {
                    if let Some(s) = &req.body
                        && s != ""
                    {
                        f.write_all(s.as_bytes()).await?;
                        f.flush().await?
                    }
                    stream.write_all(b"HTTP/1.1 201 Created\r\n\r\n").await?;
                }
                Err(e) => {
                    println!("Could not create file: {}, error: {}", file_str, e);
                    stream
                        .write_all(b"HTTP/1.1 500 Internal Server Error\r\n\r\n")
                        .await?
                }
            }
        }
        _ => stream.write_all(b"HTTP/1.1 404 Not Found\r\n\r\n").await?,
    }
    Ok(())
}
