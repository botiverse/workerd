// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

//! The lease and the peers' liveness with an external provider (`Liveness`),
//! against a fake provider on a thread of its own.

use std::rc::Rc;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::mpsc;
use std::time::Duration;

use cluster_logic::lease::Timing;
use futures::executor::block_on;
use serde_json::Value;
use serde_json::json;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::UnixListener;

use super::*;
use crate::Bucket;
use crate::Registry;
use crate::cluster::now_ms;
use crate::keeper::Thread;
use crate::registry::HELD_BY_MACHINE;

/// What the fake provider answers.
struct State {
    /// The grant for registrations and heartbeats; `None` answers 503.
    valid_for_ms: Option<u64>,
    /// How long it takes to answer a grant.
    delay_ms: u64,
    instances: Vec<Value>,
    age_ms: u64,
    /// Calls as `(path, body)`.
    calls: Vec<(String, Value)>,
}

#[derive(Clone)]
struct Fake(Arc<Mutex<State>>);

impl Fake {
    fn set(&self, f: impl FnOnce(&mut State)) {
        f(&mut self.0.lock().unwrap());
    }

    fn calls(&self, path: &str) -> Vec<Value> {
        let state = self.0.lock().unwrap();
        let calls = state.calls.iter().filter(|(p, _)| p == path);
        calls.map(|(_, body)| body.clone()).collect()
    }
}

fn socket_path() -> PathBuf {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    let path = PathBuf::from(format!("/tmp/wd-live-{}-{n}.sock", std::process::id()));
    let _ = std::fs::remove_file(&path);
    path
}

fn instance(id: &str, machine: &str, expires_ms: u64) -> Value {
    json!({"instance": id, "address": format!("{id}:1"), "machine": machine,
           "expiresMs": expires_ms, "draining": false})
}

/// A provider on a thread of its own, granting `valid_for_ms`.
fn provider(valid_for_ms: u64) -> (PathBuf, Fake) {
    let path = socket_path();
    let fake = Fake(Arc::new(Mutex::new(State {
        valid_for_ms: Some(valid_for_ms),
        delay_ms: 0,
        instances: Vec::new(),
        age_ms: 0,
        calls: Vec::new(),
    })));
    let (bound, ready) = mpsc::channel();
    let (at, state) = (path.clone(), fake.clone());
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        tokio::task::LocalSet::new().block_on(&runtime, async move {
            let listener = UnixListener::bind(&at).unwrap();
            bound.send(()).unwrap();
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                tokio::task::spawn_local(answer(stream, state.clone()));
            }
        });
    });
    ready.recv().unwrap();
    (path, fake)
}

async fn answer(mut stream: tokio::net::UnixStream, fake: Fake) {
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
        let body: Value = serde_json::from_slice(&buffer[head_end + 4..head_end + 4 + length])
            .unwrap_or_default();
        buffer.drain(..head_end + 4 + length);
        let path = head.split(' ').nth(1).unwrap().to_owned();
        let (status, text, delay) = {
            let mut s = fake.0.lock().unwrap();
            s.calls.push((path.clone(), body.clone()));
            let grant = |extra: Value| match s.valid_for_ms {
                Some(ms) => {
                    let mut grant = json!({"validForMs": ms});
                    if let (Some(g), Some(e)) = (grant.as_object_mut(), extra.as_object()) {
                        g.extend(e.clone());
                    }
                    (200, grant.to_string())
                }
                None => (503, json!({"error": "no lease"}).to_string()),
            };
            let (status, text) = match path.as_str() {
                "/v1/register" => grant(json!({"machine": "m1"})),
                "/v1/heartbeat" => grant(json!({})),
                "/v1/deregister" => (200, "{}".into()),
                "/v1/watch" => (
                    200,
                    json!({"ageMs": s.age_ms, "instances": s.instances}).to_string(),
                ),
                "/v1/probe" => {
                    let found = s
                        .instances
                        .iter()
                        .find(|i| i["instance"] == body["instance"])
                        .cloned();
                    (200, json!({"ageMs": 0, "instance": found}).to_string())
                }
                _ => (404, json!({"error": "no such call"}).to_string()),
            };
            (status, text, s.delay_ms)
        };
        if delay > 0 && path != "/v1/watch" && path != "/v1/probe" {
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }
        let reply = format!(
            "HTTP/1.1 {status} X\r\nContent-Length: {}\r\n\r\n{text}",
            text.len()
        );
        if stream.write_all(reply.as_bytes()).await.is_err() {
            return;
        }
    }
}

/// A short lease, as `keeper-test.rs` has.
fn timing(lease_ms: u64) -> Timing {
    Timing {
        lease_ms,
        interval_ms: 10,
        renew_timeout_ms: lease_ms / 2,
        list_timeout_ms: lease_ms / 2,
        retry_ms: 5,
        renew_attempt_ms: lease_ms / 4,
        list_attempt_ms: lease_ms / 4,
        contended_pause_ms: 5,
        max_skew_ms: lease_ms / 20,
        stall_ms: 2 * lease_ms,
    }
}

/// A lease thread for instance `a`, its lease from the provider at `socket`,
/// and what it fenced with.
fn lease_thread(liveness: Liveness, timing: Timing) -> (Thread, mpsc::Receiver<(String, u64)>) {
    let (fenced, fences) = mpsc::channel();
    let fence = Box::new(move |reason: String, _: Duration| {
        let _ = fenced.send((reason, mono_ms()));
    });
    let open = || Ok(Bucket::memory(""));
    let within = Box::new(|body: Box<dyn FnOnce() + Send>| body());
    let thread = Thread::spawn(
        open,
        "a".into(),
        "a:1".into(),
        timing,
        liveness,
        fence,
        within,
    );
    (thread.unwrap(), fences)
}

#[test]
fn a_grant_runs_from_when_its_request_was_sent() {
    let (socket, fake) = provider(1_000);
    fake.set(|s| s.delay_ms = 300);
    let liveness = Liveness::External(socket);
    let (thread, _fences) = lease_thread(liveness, timing(400));
    let sent = mono_ms();
    assert_eq!(block_on(thread.take_lease()).unwrap(), Some("m1".into()));
    let answered = mono_ms();
    assert!(answered - sent >= 300);
    let held = thread.held();
    assert!(held.holds(now_ms()));
    // 1000 ms from the send, not from the answer that came 300 ms later.
    let remaining = held.remaining(now_ms()).as_millis() as u64;
    assert!(remaining <= 1_000 - (answered - sent) + 5, "{remaining}");
    assert!(remaining + 100 >= 1_000 - (answered - sent), "{remaining}");
    let registered = &fake.calls("/v1/register")[0];
    assert_eq!(registered["instance"], "a");
    assert_eq!(registered["address"], "a:1");
    // The provider's grant alone: nothing was written to the bucket.
    drop(thread);
}

#[test]
fn heartbeats_keep_the_lease_and_its_end_fences_within_the_grant() {
    let (socket, fake) = provider(300);
    let (thread, fences) = lease_thread(Liveness::External(socket), timing(400));
    block_on(thread.take_lease()).unwrap();
    thread.maintain();
    // Many grants' time: the heartbeats renew it.
    std::thread::sleep(Duration::from_millis(1_000));
    assert!(fences.try_recv().is_err());
    assert!(thread.held().holds(now_ms()));
    assert!(fake.calls("/v1/heartbeat").len() > 20);
    // The provider stops granting: the instance fences before its last
    // grant ends.
    fake.set(|s| s.valid_for_ms = None);
    let stopped = mono_ms();
    let (reason, at) = fences.recv_timeout(Duration::from_secs(2)).unwrap();
    assert!(at - stopped <= 300, "fenced {} ms later", at - stopped);
    assert!(reason.contains("lease cannot be kept"), "{reason}");
}

#[test]
fn with_both_the_lease_holds_while_both_do() {
    let (socket, fake) = provider(300);
    let (thread, fences) = lease_thread(Liveness::Both(socket), timing(400));
    block_on(thread.take_lease()).unwrap();
    thread.maintain();
    std::thread::sleep(Duration::from_millis(500));
    assert!(fences.try_recv().is_err());
    // What holds is the shorter of the two: the grant.
    let remaining = thread.held().remaining(now_ms()).as_millis();
    assert!(remaining <= 300, "{remaining}");
    // The bucket lease holds on, but the grant does not.
    fake.set(|s| s.valid_for_ms = None);
    fences.recv_timeout(Duration::from_secs(2)).unwrap();
}

#[test]
fn leaving_deregisters_and_fences_nothing() {
    let (socket, fake) = provider(300);
    let (thread, fences) = lease_thread(Liveness::External(socket), timing(400));
    block_on(thread.take_lease()).unwrap();
    thread.maintain();
    thread.start_draining();
    block_on(thread.announce_draining());
    assert_eq!(
        fake.calls("/v1/heartbeat").last().unwrap()["draining"],
        true
    );
    block_on(thread.deregister()).unwrap();
    assert_eq!(fake.calls("/v1/deregister")[0]["instance"], "a");
    std::thread::sleep(Duration::from_millis(500));
    assert!(fences.try_recv().is_err());
    assert!(!thread.held().holds(now_ms()));
}

fn run<F: std::future::Future>(future: F) -> F::Output {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    tokio::task::LocalSet::new().block_on(&runtime, future)
}

/// A record in `nodes/` as an instance of `liveness` writes it.
async fn record(bucket: &Bucket, node: &str, expires_ms: u64, machine: Option<&str>) {
    let record = crate::NodeRecord {
        node: node.into(),
        address: format!("{node}:1"),
        expires_ms,
        instance: 1,
        draining: false,
        write: 1,
        machine: machine.map(str::to_owned),
    };
    let body = serde_json::to_vec(&record).unwrap();
    bucket
        .put(&format!("nodes/{node}.json"), body, &[])
        .await
        .unwrap();
}

#[test]
fn peers_come_from_the_providers_view() {
    let (socket, fake) = provider(300);
    run(async {
        let bucket = Bucket::memory("");
        let now = now_ms();
        fake.set(|s| {
            s.age_ms = 700;
            s.instances = vec![
                instance("a", "m1", now + 10_000),
                instance("b", "m2", now + 10_000),
            ];
        });
        let agent = Rc::new(Agent::new(socket));
        let registry = Registry::new(bucket.clone(), "a", "a:1", 20_000).with_agent(agent, false);
        let peers = registry.peers(now).await.unwrap();
        assert_eq!(peers.len(), 2);
        assert!(registry.listed_live("b", now));
        // The listing is as old as the reads the view comes from.
        assert_eq!(
            registry.listing(),
            Some((now - 700, vec!["a".into(), "b".into()]))
        );
        // An expired machine lease lists none live.
        fake.set(|s| {
            s.instances = vec![instance("a", "m1", now + 10_000), instance("b", "m2", now)]
        });
        registry.peers(now).await.unwrap();
        assert!(!registry.listed_live("b", now));
        assert_eq!(registry.serving_nodes(now), vec!["a".to_owned()]);
    });
}

#[test]
fn a_peer_is_live_by_a_probe_or_by_a_record_of_its_own() {
    let (socket, fake) = provider(300);
    run(async {
        let bucket = Bucket::memory("");
        let now = now_ms();
        let agent = Rc::new(Agent::new(socket));
        let registry = Registry::new(bucket.clone(), "a", "a:1", 20_000).with_agent(agent, false);
        // No machine lease lists it, and it has no record: dead.
        assert_eq!(registry.live_now("b", now).await.unwrap(), None);
        // A machine lease lists it: live, as the probe says, not as the
        // last view (none) did.
        fake.set(|s| s.instances = vec![instance("b", "m2", now + 10_000)]);
        let b = registry.live_now("b", now).await.unwrap().unwrap();
        assert_eq!(
            (b.address.as_str(), b.machine.as_deref()),
            ("b:1", Some("m2"))
        );
        // Gone from it: dead, its static record notwithstanding.
        fake.set(|s| s.instances.clear());
        record(&bucket, "b", 0, Some("m2")).await;
        assert_eq!(registry.live_now("b", now).await.unwrap(), None);
        assert!(!registry.listed_live("b", now));
        // A peer that renews a record of its own (`bucket`, mid-switch) is
        // live by it.
        record(&bucket, "c", now + 10_000, None).await;
        assert!(registry.live_now("c", now).await.unwrap().is_some());
        record(&bucket, "c", now, None).await;
        assert_eq!(registry.live_now("c", now).await.unwrap(), None);
        assert_eq!(fake.calls("/v1/probe").len(), 5);
    });
}

#[test]
fn with_both_a_peer_is_live_if_either_says_so() {
    let (socket, fake) = provider(300);
    run(async {
        let bucket = Bucket::memory("");
        let now = now_ms();
        let agent = Rc::new(Agent::new(socket));
        let registry = Registry::new(bucket.clone(), "a", "a:1", 20_000).with_agent(agent, true);
        // An `external` peer: its record held by its machine's lease, live.
        record(&bucket, "b", HELD_BY_MACHINE, Some("m2")).await;
        // One whose machine's lease no longer lists it: dead, whatever its
        // record says.
        record(&bucket, "e", HELD_BY_MACHINE, Some("m2")).await;
        // A `bucket` peer, which no lease lists.
        record(&bucket, "c", now + 10_000, None).await;
        // A dead one.
        record(&bucket, "d", now - 1, None).await;
        fake.set(|s| s.instances = vec![instance("b", "m2", now + 10_000)]);
        let mut live: Vec<_> = registry
            .peers(now)
            .await
            .unwrap()
            .into_iter()
            .map(|p| p.node)
            .collect();
        live.sort();
        assert_eq!(live, ["b", "c"]);
        let mut dead = registry.dead();
        dead.sort();
        assert_eq!(dead, ["d", "e"]);
        assert_eq!(registry.live_now("e", now).await.unwrap(), None);
        assert!(registry.live_now("b", now).await.unwrap().is_some());
        assert!(registry.live_now("c", now).await.unwrap().is_some());
        assert_eq!(registry.live_now("d", now).await.unwrap(), None);
    });
}

#[test]
fn the_scan_finds_the_dead_and_deletes_old_records() {
    let (socket, fake) = provider(300);
    run(async {
        let bucket = Bucket::memory("");
        let ttl = 1_000;
        let agent = Rc::new(Agent::new(socket));
        let registry = Registry::new(bucket.clone(), "a", "a:1", ttl).with_agent(agent, false);
        let now = now_ms();
        fake.set(|s| {
            s.instances = vec![
                instance("a", "m1", now + 10_000),
                instance("b", "m1", now + 10_000),
            ]
        });
        record(&bucket, "a", 0, Some("m1")).await;
        record(&bucket, "b", 0, Some("m1")).await;
        // Just written, and no view had it live: it may be registering.
        record(&bucket, "new", 0, Some("m2")).await;
        registry.peers(now).await.unwrap();
        assert!(registry.dead().is_empty());
        assert_eq!(fake.calls("/v1/watch").len(), 1);
        // b leaves the view: a scan at once finds it dead, though its record
        // is new; `new` stays unjudged until it is a lease old.
        fake.set(|s| s.instances.truncate(1));
        registry.peers(now + 10).await.unwrap();
        assert_eq!(registry.dead(), ["b"]);
        // Later, without a scan due, the dead stay as the last scan found them.
        registry.peers(now + 20).await.unwrap();
        assert_eq!(registry.dead(), ["b"]);
        // The next scan, three leases on: `new` never made it to a view.
        let later = now + 3 * ttl + 50;
        fake.set(|s| s.instances = vec![instance("a", "m1", later + 10_000)]);
        registry.peers(later).await.unwrap();
        let mut dead = registry.dead();
        dead.sort();
        assert_eq!(dead, ["b", "new"]);
        // Ten leases on, records without alarms go; one with alarms stays.
        bucket
            .put("alarms/new/0/ns/actor/0", Vec::new(), &[])
            .await
            .unwrap();
        let much_later = now + 12 * ttl;
        fake.set(|s| s.instances = vec![instance("a", "m1", much_later + 10_000)]);
        registry.peers(much_later).await.unwrap();
        assert!(bucket.get("nodes/b.json").await.unwrap().is_none());
        assert!(bucket.get("nodes/new.json").await.unwrap().is_some());
        assert!(bucket.get("nodes/a.json").await.unwrap().is_some());
    });
}

#[test]
fn an_external_instance_publishes_a_static_record() {
    let (socket, _fake) = provider(300);
    run(async {
        let bucket = Bucket::memory("");
        let agent = Rc::new(Agent::new(socket));
        let registry = Registry::new(bucket.clone(), "a", "a:1", 20_000).with_agent(agent, false);
        registry.set_machine(Some("m1".into()));
        registry.publish().await.unwrap();
        let read = |bucket: Bucket| async move {
            let (body, _) = bucket.get("nodes/a.json").await.unwrap().unwrap();
            serde_json::from_slice::<crate::NodeRecord>(&body).unwrap()
        };
        let joined = read(bucket.clone()).await;
        assert_eq!(
            (joined.expires_ms, joined.machine.as_deref()),
            (HELD_BY_MACHINE, Some("m1"))
        );
        assert!(!joined.draining);
        registry.deregister().await.unwrap();
        let left = read(bucket.clone()).await;
        assert!(left.draining);
        assert_eq!(left.expires_ms, 0);
        assert_eq!(left.machine.as_deref(), Some("m1"));
    });
}

#[test]
fn a_reader_without_a_provider_never_takes_a_machine_held_instance_for_dead() {
    run(async {
        let bucket = Bucket::memory("");
        let now = now_ms();
        let registry = Registry::new(bucket.clone(), "a", "a:1", 1_000);
        record(&bucket, "b", HELD_BY_MACHINE, Some("m2")).await;
        // Live: nothing here can tell its machine's lease ran out, so its
        // objects wait rather than move from a live owner.
        assert!(registry.live_now("b", now).await.unwrap().is_some());
        // Nor is its record deleted, however old.
        let much_later = now + 100 * 1_000;
        registry.peers(much_later).await.unwrap();
        assert!(bucket.get("nodes/b.json").await.unwrap().is_some());
        // Left: its rewritten record is dead to every reader.
        record(&bucket, "b", 0, Some("m2")).await;
        assert_eq!(registry.live_now("b", now).await.unwrap(), None);
    });
}

#[test]
fn a_stale_view_judges_no_one_dead() {
    let (socket, fake) = provider(300);
    run(async {
        let bucket = Bucket::memory("");
        let ttl = 1_000;
        let agent = Rc::new(Agent::new(socket));
        let registry = Registry::new(bucket.clone(), "a", "a:1", ttl).with_agent(agent, false);
        let now = now_ms();
        fake.set(|s| {
            s.instances = vec![
                instance("a", "m1", now + 10_000),
                instance("b", "m2", now + 600),
            ]
        });
        record(&bucket, "b", HELD_BY_MACHINE, Some("m2")).await;
        registry.peers(now).await.unwrap();
        assert!(registry.dead().is_empty());
        // The provider has read nothing for a while: m2's lease, as it last
        // read it, ran out by now. b is not listed, but neither dead nor its
        // record deleted, however long.
        fake.set(|s| s.age_ms = ttl);
        let later = now + 20 * ttl;
        registry.peers(later).await.unwrap();
        assert!(!registry.listed_live("b", later));
        assert!(registry.dead().is_empty());
        assert!(bucket.get("nodes/b.json").await.unwrap().is_some());
        // A fresh view without it: dead.
        fake.set(|s| {
            s.age_ms = 0;
            s.instances.truncate(1);
        });
        registry.peers(later).await.unwrap();
        assert_eq!(registry.dead(), ["b"]);
    });
}

#[test]
fn a_provider_that_grants_nothing_fences_at_once() {
    // Well over a renewal's bound (half the lease) left at every round.
    let (socket, fake) = provider(3_500);
    let (thread, fences) = lease_thread(Liveness::External(socket), timing(4_000));
    block_on(thread.take_lease()).unwrap();
    thread.maintain();
    std::thread::sleep(Duration::from_millis(100));
    assert!(fences.try_recv().is_err());
    // A clock off the bucket's, say: the grant is 0, though 3 s were left.
    fake.set(|s| s.valid_for_ms = Some(0));
    let zeroed = mono_ms();
    let (reason, at) = fences.recv_timeout(Duration::from_secs(2)).unwrap();
    assert!(at - zeroed < 200, "fenced {} ms later", at - zeroed);
    assert!(reason.contains("grants no lease"), "{reason}");
    assert!(!thread.held().holds(now_ms()));
}

#[test]
fn the_grant_clock_counts_time_suspended() {
    // `CLOCK_BOOTTIME`, not `Instant`: equal to it while nothing suspends.
    let (a, i) = (mono_ms(), std::time::Instant::now());
    std::thread::sleep(Duration::from_millis(50));
    let elapsed = mono_ms() - a;
    assert!(
        elapsed >= 49 && elapsed <= i.elapsed().as_millis() as u64 + 2,
        "{elapsed}"
    );
}

#[test]
fn with_both_a_stale_view_judges_no_held_record_dead_and_a_fresh_one_deletes_it() {
    let (socket, fake) = provider(300);
    run(async {
        let bucket = Bucket::memory("");
        let ttl = 1_000;
        let agent = Rc::new(Agent::new(socket));
        let registry = Registry::new(bucket.clone(), "a", "a:1", ttl).with_agent(agent, true);
        let now = now_ms();
        // An `external` peer, which its machine's lease no longer lists in
        // a view too old to tell.
        record(&bucket, "b", HELD_BY_MACHINE, Some("m2")).await;
        fake.set(|s| s.age_ms = ttl);
        registry.peers(now).await.unwrap();
        assert!(registry.dead().is_empty());
        // Fresh: dead. Its record stays while it is new, and while it has
        // alarms; then it goes, as no reader without a provider could ever
        // take it for dead.
        fake.set(|s| s.age_ms = 0);
        registry.peers(now).await.unwrap();
        assert_eq!(registry.dead(), ["b"]);
        assert!(bucket.get("nodes/b.json").await.unwrap().is_some());
        bucket
            .put("alarms/b/0/ns/actor/0", Vec::new(), &[])
            .await
            .unwrap();
        let later = now + 11 * ttl;
        registry.peers(later).await.unwrap();
        assert!(bucket.get("nodes/b.json").await.unwrap().is_some());
        bucket.delete("alarms/b/0/ns/actor/0").await.unwrap();
        registry.peers(later).await.unwrap();
        assert!(bucket.get("nodes/b.json").await.unwrap().is_none());
    });
}
