// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

use std::cell::Cell;
use std::rc::Rc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use tokio::net::UnixListener;

use super::*;

/// A socket path short enough for `sockaddr_un`, which a test's own temporary
/// directory may not be.
fn socket_path() -> PathBuf {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    let path = PathBuf::from(format!("/tmp/wd-agent-{}-{n}.sock", std::process::id()));
    let _ = std::fs::remove_file(&path);
    path
}

fn run<F: std::future::Future>(future: F) -> F::Output {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    tokio::task::LocalSet::new().block_on(&runtime, future)
}

/// How the fake provider frames an answer.
#[derive(Clone, Copy)]
enum Framing {
    Length,
    Chunked,
    /// Content-Length, then the connection closes.
    Close,
}

/// A provider that answers each request with what `answer` makes of its
/// method, path and body, counting connections.
fn serve(
    path: &PathBuf,
    framing: Framing,
    answer: impl Fn(&str, &str, &[u8]) -> (u16, String) + 'static,
) -> Rc<Cell<usize>> {
    let listener = UnixListener::bind(path).unwrap();
    let connections = Rc::new(Cell::new(0));
    let counted = Rc::clone(&connections);
    let answer = Rc::new(answer);
    tokio::task::spawn_local(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            counted.set(counted.get() + 1);
            let answer = Rc::clone(&answer);
            tokio::task::spawn_local(async move {
                let mut buffer = Vec::new();
                loop {
                    let head_end = loop {
                        if let Some(at) = find(&buffer, b"\r\n\r\n") {
                            break at;
                        }
                        let mut chunk = [0u8; 1024];
                        let n = stream.read(&mut chunk).await.unwrap_or(0);
                        if n == 0 {
                            return;
                        }
                        buffer.extend_from_slice(&chunk[..n]);
                    };
                    let head = String::from_utf8_lossy(&buffer[..head_end]).into_owned();
                    let length = head
                        .lines()
                        .find_map(|l| l.strip_prefix("Content-Length: "))
                        .map_or(0, |n| n.parse::<usize>().unwrap());
                    while buffer.len() < head_end + 4 + length {
                        let mut chunk = [0u8; 1024];
                        let n = stream.read(&mut chunk).await.unwrap();
                        buffer.extend_from_slice(&chunk[..n]);
                    }
                    let body = buffer[head_end + 4..head_end + 4 + length].to_vec();
                    buffer.drain(..head_end + 4 + length);
                    let mut words = head.split(' ');
                    let (method, target) = (words.next().unwrap(), words.next().unwrap());
                    let (status, text) = answer(method, target, &body);
                    let reply = match framing {
                        Framing::Length | Framing::Close => format!(
                            "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\n\
                             Content-Length: {}\r\n{}\r\n{text}",
                            text.len(),
                            if matches!(framing, Framing::Close) {
                                "Connection: close\r\n"
                            } else {
                                ""
                            }
                        ),
                        Framing::Chunked => {
                            let (a, b) = text.split_at(text.len() / 2);
                            format!(
                                "HTTP/1.1 {status} X\r\nTransfer-Encoding: chunked\r\n\r\n\
                                 {:x}\r\n{a}\r\n{:x}\r\n{b}\r\n0\r\n\r\n",
                                a.len(),
                                b.len()
                            )
                        }
                    };
                    stream.write_all(reply.as_bytes()).await.unwrap();
                    if matches!(framing, Framing::Close) {
                        return;
                    }
                }
            });
        }
    });
    connections
}

fn provider(method: &str, path: &str, body: &[u8]) -> (u16, String) {
    let body: serde_json::Value = if body.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(body).unwrap()
    };
    let instance = |id: &str| {
        format!(
            r#"{{"instance":"{id}","address":"10.0.0.1:7000","machine":"m1","expiresMs":5000,"draining":false}}"#
        )
    };
    match (method, path) {
        ("POST", "/v1/register") => {
            assert_eq!(body["address"], "10.0.0.2:7000");
            (200, r#"{"validForMs":15000,"machine":"m2"}"#.into())
        }
        ("POST", "/v1/heartbeat") if body["instance"] == "gone" => {
            (404, r#"{"error":"no such instance"}"#.into())
        }
        ("POST", "/v1/heartbeat") => {
            assert_eq!(body["progress"], 7);
            assert_eq!(body["draining"], true);
            (200, r#"{"validForMs":12000}"#.into())
        }
        ("POST", "/v1/deregister") => (200, "{}".into()),
        ("GET", "/v1/watch") => (
            200,
            format!(r#"{{"ageMs":1200,"instances":[{}]}}"#, instance("a")),
        ),
        ("POST", "/v1/probe") if body["instance"] == "a" => (
            200,
            format!(r#"{{"ageMs":0,"instance":{}}}"#, instance("a")),
        ),
        ("POST", "/v1/probe") => (200, r#"{"ageMs":0,"instance":null}"#.into()),
        _ => (400, r#"{"error":"unknown call"}"#.into()),
    }
}

fn calls(framing: Framing) -> usize {
    run(async move {
        let path = socket_path();
        let connections = serve(&path, framing, provider);
        let agent = Agent::new(path.clone());
        let grant = agent.register("b", "10.0.0.2:7000", false).await.unwrap();
        assert_eq!(
            grant,
            Grant {
                valid_for_ms: 15_000,
                machine: Some("m2".into())
            }
        );
        let grant = agent.heartbeat("b", 7, true).await.unwrap();
        assert_eq!(grant.valid_for_ms, 12_000);
        let view = agent.watch().await.unwrap();
        assert_eq!(view.age_ms, 1200);
        assert_eq!(view.instances.len(), 1);
        assert_eq!(view.instances[0].address, "10.0.0.1:7000");
        assert_eq!(view.instances[0].expires_ms, 5000);
        assert_eq!(agent.probe("a").await.unwrap().unwrap().machine, "m1");
        assert_eq!(agent.probe("z").await.unwrap(), None);
        agent.deregister("b").await.unwrap();
        let refused = agent.heartbeat("gone", 7, true).await.unwrap_err();
        assert!(
            refused.to_string().contains("404 no such instance"),
            "{refused}"
        );
        let _ = std::fs::remove_file(&path);
        connections.get()
    })
}

#[test]
fn calls_share_a_kept_connection() {
    assert_eq!(calls(Framing::Length), 1);
}

#[test]
fn a_chunked_answer_is_read() {
    assert_eq!(calls(Framing::Chunked), 1);
}

#[test]
fn a_closed_connection_is_not_kept() {
    assert_eq!(calls(Framing::Close), 7);
}

#[test]
fn a_kept_connection_the_provider_closed_is_replaced() {
    run(async {
        let path = socket_path();
        let connections = serve(&path, Framing::Length, provider);
        let agent = Agent::new(path.clone());
        agent.watch().await.unwrap();
        // The provider restarts: its connections close.
        let kept = agent.idle.borrow_mut().pop().unwrap();
        let std = kept.into_std().unwrap();
        std.shutdown(std::net::Shutdown::Both).unwrap();
        agent
            .idle
            .borrow_mut()
            .push(UnixStream::from_std(std).unwrap());
        agent.watch().await.unwrap();
        assert_eq!(connections.get(), 2);
        let _ = std::fs::remove_file(&path);
    });
}

#[test]
fn no_provider_is_an_error_naming_the_socket() {
    run(async {
        let agent = Agent::new(PathBuf::from("/tmp/wd-agent-nobody.sock"));
        let e = agent.watch().await.unwrap_err().to_string();
        assert!(e.contains("/tmp/wd-agent-nobody.sock"), "{e}");
    });
}

#[test]
fn the_monotonic_clock_starts_above_zero() {
    let a = mono_ms();
    assert!(a >= 1);
    assert!(mono_ms() >= a);
}
