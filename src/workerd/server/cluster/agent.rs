// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

//! An external liveness provider (`ClusterConfig.liveness`): a process on the
//! same machine, such as a host agent, that holds one lease in the bucket for
//! every workerd instance on the machine, so that the instances need not each
//! renew a record of their own.
//!
//! It answers HTTP/1.1 with JSON bodies on a unix socket:
//!
//! - `POST /v1/register` `{instance, address, draining}`: `{validForMs, machine}`.
//! - `POST /v1/heartbeat` `{instance, progress, draining}`: `{validForMs}`.
//! - `POST /v1/deregister` `{instance}`: `{}`.
//! - `GET /v1/watch`: `{ageMs, instances: [Instance]}`, every instance of the
//!   caller's cluster on every machine, as the provider last read them.
//! - `POST /v1/probe` `{instance}`: `{ageMs, instance: Instance | null}`, from
//!   a read of the instance's machine lease that started after the probe
//!   arrived.
//!
//! A grant is relative: `validForMs` from when the instance sent the request,
//! by its own monotonic clock. The provider computes it from its own lease,
//! leaving the time that peers' clocks may be ahead of the bucket's, so the
//! instance serves only while it holds and needs no margin of its own. The
//! provider tells the caller's cluster from the connection's credentials, not
//! from anything the request says.

use std::cell::RefCell;
use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::Instant;

use serde::Deserialize;
use serde::Serialize;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::UnixStream;

use crate::Error;
use crate::Result;

/// Where an instance's liveness comes from (`ClusterConfig.liveness`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Liveness {
    /// Its own record in the bucket, renewed by its own lease thread.
    Bucket,
    /// A grant from the provider at this socket.
    External(PathBuf),
    /// Both, for a rolling switch between the two: the instance serves only
    /// while both hold, and finds a peer live if either says so.
    Both(PathBuf),
}

impl Liveness {
    /// The provider's socket, unless the bucket alone keeps the lease.
    pub fn socket(&self) -> Option<&PathBuf> {
        match self {
            Self::Bucket => None,
            Self::External(path) | Self::Both(path) => Some(path),
        }
    }

    /// Whether the instance renews its own record in the bucket.
    pub fn renews_record(&self) -> bool {
        !matches!(self, Self::External(_))
    }
}

/// A grant: the instance's lease holds for `valid_for_ms` from when it sent
/// the request.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Grant {
    pub valid_for_ms: u64,
    /// The machine whose lease holds the instance; answered by `register`.
    #[serde(default)]
    pub machine: Option<String>,
}

/// An instance as a machine lease lists it.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Instance {
    pub instance: String,
    pub address: String,
    #[serde(default)]
    pub machine: String,
    /// When the machine's lease expires: bucket-referenced wall time, judged
    /// by each reader's clock as a node record's `expires_ms` is.
    pub expires_ms: u64,
    #[serde(default)]
    pub draining: bool,
}

/// The provider's view of the cluster.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct View {
    /// How long ago the reads it comes from started, by the provider's
    /// monotonic clock.
    pub age_ms: u64,
    pub instances: Vec<Instance>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Probed {
    #[serde(default)]
    instance: Option<Instance>,
}

#[derive(Serialize)]
struct Register<'a> {
    instance: &'a str,
    address: &'a str,
    draining: bool,
}

#[derive(Serialize)]
struct Heartbeat<'a> {
    instance: &'a str,
    progress: u64,
    draining: bool,
}

#[derive(Serialize)]
struct Named<'a> {
    instance: &'a str,
}

/// The largest response head and body the client reads.
const MAX_HEAD: usize = 16 * 1024;
const MAX_BODY: usize = 8 * 1024 * 1024;

/// A client of the provider at one socket, for the thread that made it: its
/// connections belong to that thread's runtime. Calls may overlap, each on a
/// connection of its own; an idle one is kept for the next.
pub struct Agent {
    socket: PathBuf,
    idle: RefCell<Vec<UnixStream>>,
}

/// A provider's answer that is not a success.
fn refused(path: &str, status: u16, body: &[u8]) -> Error {
    #[derive(Deserialize)]
    struct Said {
        error: String,
    }
    let said = serde_json::from_slice::<Said>(body)
        .map(|s| s.error)
        .unwrap_or_else(|_| String::from_utf8_lossy(&body[..body.len().min(200)]).into_owned());
    Error::Http(format!("liveness provider: {path}: {status} {said}"))
}

impl Agent {
    pub fn new(socket: PathBuf) -> Self {
        Self {
            socket,
            idle: RefCell::new(Vec::new()),
        }
    }

    pub async fn register(&self, instance: &str, address: &str, draining: bool) -> Result<Grant> {
        let body = Register {
            instance,
            address,
            draining,
        };
        self.json("POST", "/v1/register", Some(serde_json::to_vec(&body)?))
            .await
    }

    pub async fn heartbeat(&self, instance: &str, progress: u64, draining: bool) -> Result<Grant> {
        let body = Heartbeat {
            instance,
            progress,
            draining,
        };
        self.json("POST", "/v1/heartbeat", Some(serde_json::to_vec(&body)?))
            .await
    }

    pub async fn deregister(&self, instance: &str) -> Result<()> {
        let body = serde_json::to_vec(&Named { instance })?;
        self.call("POST", "/v1/deregister", Some(body))
            .await
            .map(drop)
    }

    pub async fn watch(&self) -> Result<View> {
        self.json("GET", "/v1/watch", None).await
    }

    /// `instance` as its machine lease lists it now, if one does.
    pub async fn probe(&self, instance: &str) -> Result<Option<Instance>> {
        let body = serde_json::to_vec(&Named { instance })?;
        let probed: Probed = self.json("POST", "/v1/probe", Some(body)).await?;
        Ok(probed.instance)
    }

    async fn json<T: serde::de::DeserializeOwned>(
        &self,
        method: &str,
        path: &str,
        body: Option<Vec<u8>>,
    ) -> Result<T> {
        let answer = self.call(method, path, body).await?;
        Ok(serde_json::from_slice(&answer)?)
    }

    /// One request; its answer's body if the status is 200. A kept
    /// connection the provider closed meanwhile (an idle timeout, a restart)
    /// fails before any answer: the request is sent once more on a new one.
    async fn call(&self, method: &str, path: &str, body: Option<Vec<u8>>) -> Result<Vec<u8>> {
        let body = body.unwrap_or_default();
        let mut request =
            format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nAccept: application/json\r\n");
        if method != "GET" {
            request.push_str("Content-Type: application/json\r\n");
        }
        request.push_str(&format!("Content-Length: {}\r\n\r\n", body.len()));
        let mut request = request.into_bytes();
        request.extend_from_slice(&body);
        let kept = self.idle.borrow_mut().pop();
        let answer = match kept {
            Some(stream) => match exchange(stream, &request).await {
                Err(Exchange::Unanswered(_)) => None,
                other => Some(other),
            },
            None => None,
        };
        let answer = match answer {
            Some(answer) => answer,
            None => {
                let stream = UnixStream::connect(&self.socket).await.map_err(|e| {
                    Error::Http(format!(
                        "liveness provider at {}: {e}",
                        self.socket.display()
                    ))
                })?;
                exchange(stream, &request).await
            }
        };
        let (status, answer, stream) = answer.map_err(|e| match e {
            Exchange::Unanswered(e) | Exchange::Broken(e) => {
                Error::Http(format!("liveness provider: {path}: {e}"))
            }
        })?;
        if let Some(stream) = stream {
            self.idle.borrow_mut().push(stream);
        }
        if status != 200 {
            return Err(refused(path, status, &answer));
        }
        Ok(answer)
    }
}

enum Exchange {
    /// Failed before any byte of an answer: safe to send again.
    Unanswered(String),
    Broken(String),
}

/// Sends `request` on `stream` and reads the answer: its status, body, and
/// the stream again if it may carry another request.
async fn exchange(
    mut stream: UnixStream,
    request: &[u8],
) -> std::result::Result<(u16, Vec<u8>, Option<UnixStream>), Exchange> {
    stream
        .write_all(request)
        .await
        .map_err(|e| Exchange::Unanswered(e.to_string()))?;
    let mut buffer = Vec::new();
    let head_end = loop {
        if let Some(at) = find(&buffer, b"\r\n\r\n") {
            break at;
        }
        if buffer.len() > MAX_HEAD {
            return Err(Exchange::Broken("the answer's head is too large".into()));
        }
        let mut chunk = [0u8; 4096];
        let n = stream.read(&mut chunk).await.map_err(|e| {
            if buffer.is_empty() {
                Exchange::Unanswered(e.to_string())
            } else {
                Exchange::Broken(e.to_string())
            }
        })?;
        if n == 0 {
            return Err(if buffer.is_empty() {
                Exchange::Unanswered("the connection closed".into())
            } else {
                Exchange::Broken("the connection closed mid-answer".into())
            });
        }
        buffer.extend_from_slice(&chunk[..n]);
    };
    let head = String::from_utf8_lossy(&buffer[..head_end]).into_owned();
    let mut rest = buffer.split_off(head_end + 4);
    let mut lines = head.split("\r\n");
    let status = lines
        .next()
        .and_then(|line| line.split(' ').nth(1))
        .and_then(|code| code.parse::<u16>().ok())
        .ok_or_else(|| Exchange::Broken(format!("not an HTTP answer: {head:.80}")))?;
    let (mut length, mut chunked, mut close) = (None, false, false);
    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let value = value.trim();
        match name.trim().to_ascii_lowercase().as_str() {
            "content-length" => length = value.parse::<usize>().ok(),
            "transfer-encoding" => chunked = value.eq_ignore_ascii_case("chunked"),
            "connection" => close = value.eq_ignore_ascii_case("close"),
            _ => {}
        }
    }
    let broken = |e: std::io::Error| Exchange::Broken(e.to_string());
    let body = if chunked {
        read_chunked(&mut stream, &mut rest).await?
    } else if let Some(length) = length {
        if length > MAX_BODY {
            return Err(Exchange::Broken("the answer is too large".into()));
        }
        while rest.len() < length {
            let mut chunk = vec![0u8; (length - rest.len()).min(64 * 1024)];
            let n = stream.read(&mut chunk).await.map_err(broken)?;
            if n == 0 {
                return Err(Exchange::Broken("the connection closed mid-body".into()));
            }
            rest.extend_from_slice(&chunk[..n]);
        }
        let more = rest.split_off(length);
        close |= !more.is_empty();
        rest
    } else {
        // Until the connection closes.
        stream.read_to_end(&mut rest).await.map_err(broken)?;
        close = true;
        rest
    };
    Ok((status, body, (!close).then_some(stream)))
}

/// A chunked body, `rest` holding what was read past the head.
async fn read_chunked(
    stream: &mut UnixStream,
    rest: &mut Vec<u8>,
) -> std::result::Result<Vec<u8>, Exchange> {
    let mut body = Vec::new();
    let mut pos = 0;
    loop {
        // A size line, then that many bytes and a CRLF; the last chunk is 0,
        // followed by trailers and a blank line.
        let line_end = loop {
            if let Some(at) = find(&rest[pos..], b"\r\n") {
                break pos + at;
            }
            fill(stream, rest).await?;
        };
        let size_text = String::from_utf8_lossy(&rest[pos..line_end]).into_owned();
        let size_text = size_text.split(';').next().unwrap_or_default().trim();
        let size = usize::from_str_radix(size_text, 16)
            .map_err(|_| Exchange::Broken("a malformed chunk".into()))?;
        pos = line_end + 2;
        if size == 0 {
            while find(&rest[pos..], b"\r\n").is_none_or(|at| at != 0)
                && find(&rest[pos..], b"\r\n\r\n").is_none()
            {
                fill(stream, rest).await?;
            }
            return Ok(body);
        }
        if body.len() + size > MAX_BODY {
            return Err(Exchange::Broken("the answer is too large".into()));
        }
        while rest.len() < pos + size + 2 {
            fill(stream, rest).await?;
        }
        body.extend_from_slice(&rest[pos..pos + size]);
        pos += size + 2;
    }
}

async fn fill(stream: &mut UnixStream, rest: &mut Vec<u8>) -> std::result::Result<(), Exchange> {
    let mut chunk = [0u8; 4096];
    let n = stream
        .read(&mut chunk)
        .await
        .map_err(|e| Exchange::Broken(e.to_string()))?;
    if n == 0 {
        return Err(Exchange::Broken("the connection closed mid-body".into()));
    }
    rest.extend_from_slice(&chunk[..n]);
    Ok(())
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// Milliseconds on this process's monotonic clock, from 1: a grant's deadline
/// is on it, never on the wall clock, which may step.
pub fn mono_ms() -> u64 {
    static START: OnceLock<Instant> = OnceLock::new();
    let start = START.get_or_init(Instant::now);
    u64::try_from(start.elapsed().as_millis())
        .unwrap_or(u64::MAX)
        .saturating_add(1)
}

#[cfg(test)]
#[path = "agent-test.rs"]
mod tests;

#[cfg(test)]
#[path = "liveness-test.rs"]
mod liveness_tests;
