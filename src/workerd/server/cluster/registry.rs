// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

use std::cell::Cell;
use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::HashSet;
use std::rc::Rc;
use std::time::Duration;

use cluster_logic::lease::Timing;
use serde::Deserialize;
use serde::Serialize;

use crate::Bucket;
use crate::Error;
use crate::Guard;
use crate::Result;
use crate::Version;
use crate::agent::Agent;
use crate::agent::Instance;

/// One instance's record in `nodes/<node>.json`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeRecord {
    /// The instance's identity: 16 random bytes, hex, new each start.
    pub node: String,
    /// Where peers reach it, `host:port`.
    pub address: String,
    /// The instance is live until this time and must renew before it.
    pub expires_ms: u64,
    /// The process that wrote it.
    #[serde(default)]
    pub instance: u64,
    /// The instance is stopping: it hands its objects off, and peers
    /// choose it for none.
    #[serde(default)]
    pub draining: bool,
    /// Counts this process's writes of the record, so that no two have
    /// one body: S3's version of an object (its `ETag`) is a hash of the
    /// body, and a version that came back would let a late write of ours
    /// replace a record it was not sent for. Older processes write none.
    #[serde(default)]
    pub write: u64,
    /// The machine whose lease keeps the instance live, when a liveness
    /// provider does (`agent::Liveness`): the record of an instance that
    /// renews none of its own (`External`) says `expires_ms`
    /// [`HELD_BY_MACHINE`], and readers with a provider ask it instead. Readers that keep their own leases never
    /// delete such a record. Older processes write none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub machine: Option<String>,
}

impl NodeRecord {
    /// An instance as its machine's lease lists it.
    fn of(instance: Instance) -> Self {
        Self {
            node: instance.instance,
            address: instance.address,
            expires_ms: instance.expires_ms,
            instance: 0,
            draining: instance.draining,
            write: 0,
            machine: Some(instance.machine),
        }
    }
}

/// This instance's membership: it publishes its own record and reads its
/// peers'. A peer whose lease expired is dead; nothing else is.
pub struct Registry {
    bucket: Bucket,
    node: String,
    address: String,
    ttl_ms: u64,
    /// The version of our own record we last wrote, so a renewal replaces
    /// exactly that and notices anyone else's write.
    own: RefCell<Option<Version>>,
    /// Random per process: a renewal whose answer was lost may still have
    /// landed, and this tells that write from another writer's.
    instance: u64,
    /// Live peers' records, as of the last listing.
    listed: RefCell<HashMap<String, NodeRecord>>,
    /// When the latest listing that succeeded started, and the nodes it
    /// found live, sorted: unlike `listed`, nothing else changes it.
    listing: RefCell<Option<(u64, Vec<String>)>>,
    /// Dead peers whose records remain, as of the last listing.
    dead: RefCell<Vec<String>>,
    /// This instance left the cluster: it renews its record no more.
    left: Cell<bool>,
    /// This instance drains, which its record says from its next renewal.
    draining: Cell<bool>,
    /// The last `NodeRecord::write`.
    writes: Cell<u64>,
    /// How long a renewal waits to write again when its guard failed on a
    /// version that is still current.
    contended_pause: Duration,
    /// The liveness provider that answers which peers live, if one does,
    /// and whether peers' own records may say so too (`Liveness::Both`).
    agent: Option<Rc<Agent>>,
    both: bool,
    /// The machine the provider placed this instance on, for its record.
    machine: RefCell<Option<String>>,
    /// With a provider alone: when `nodes/` was last scanned, the instances
    /// the last view had live, every one a view had live (an instance's
    /// record is written after it is), and peers that renew records of their
    /// own as the last scan found them.
    scanned_ms: Cell<Option<u64>>,
    viewed: RefCell<HashSet<String>>,
    seen: RefCell<HashSet<String>>,
    renewing: RefCell<HashMap<String, NodeRecord>>,
}

fn node_key(node: &str) -> String {
    format!("nodes/{node}.json")
}

impl Registry {
    pub fn new(bucket: Bucket, node: &str, address: &str, ttl_ms: u64) -> Self {
        Self {
            bucket,
            node: node.to_owned(),
            address: address.to_owned(),
            ttl_ms,
            own: RefCell::new(None),
            instance: std::hash::BuildHasher::hash_one(
                &std::collections::hash_map::RandomState::new(),
                std::time::SystemTime::now(),
            ),
            listed: RefCell::new(HashMap::new()),
            listing: RefCell::new(None),
            dead: RefCell::new(Vec::new()),
            left: Cell::new(false),
            draining: Cell::new(false),
            writes: Cell::new(0),
            contended_pause: Duration::from_millis(Timing::DEFAULT.contended_pause_ms),
            agent: None,
            both: false,
            machine: RefCell::new(None),
            scanned_ms: Cell::new(None),
            viewed: RefCell::default(),
            seen: RefCell::default(),
            renewing: RefCell::default(),
        }
    }

    /// Peers' liveness from `agent`, and from their own records as well if
    /// `both`.
    #[must_use]
    pub fn with_agent(mut self, agent: Rc<Agent>, both: bool) -> Self {
        self.agent = Some(agent);
        self.both = both;
        self
    }

    /// The machine this instance's record names.
    pub fn set_machine(&self, machine: Option<String>) {
        *self.machine.borrow_mut() = machine;
    }

    /// Whether `start_draining` ran.
    pub fn draining(&self) -> bool {
        self.draining.get()
    }

    /// Whether a liveness provider alone says which peers live.
    fn external(&self) -> bool {
        self.agent.is_some() && !self.both
    }

    /// With `Timing::contended_pause_ms` for a lease whose times are not
    /// the default.
    #[must_use]
    pub const fn contended_pause(mut self, pause: Duration) -> Self {
        self.contended_pause = pause;
        self
    }

    /// The next `NodeRecord::write`.
    fn next_write(&self) -> u64 {
        self.writes.set(self.writes.get() + 1);
        self.writes.get()
    }

    pub fn node(&self) -> &str {
        &self.node
    }

    /// Where this instance listens.
    pub fn advertise(&self) -> &str {
        &self.address
    }

    /// Where peer `node` listens, as of the last listing.
    pub fn address(&self, node: &str) -> Option<String> {
        self.listed.borrow().get(node).map(|r| r.address.clone())
    }

    /// Publish or renew this instance's record; answers the new expiry. The
    /// first write requires the key to be absent (the node id is fresh per
    /// process), and every later one requires our last version, so a record
    /// someone else wrote fences this process instead of being overwritten.
    pub async fn renew(&self, now_ms: u64) -> Result<u64> {
        let expires_ms = now_ms + self.ttl_ms;
        self.write_own(expires_ms).await?;
        Ok(expires_ms)
    }

    /// The record of an instance whose liveness provider alone keeps it live
    /// (`Liveness::External`), written once, as it joins: `expires_ms`
    /// [`HELD_BY_MACHINE`] and its machine. Peers find it by scanning `nodes/`, for its alarms once it
    /// is dead; `deregister` rewrites it as it leaves.
    pub async fn publish(&self) -> Result<()> {
        self.write_own(HELD_BY_MACHINE).await
    }

    /// Writes this instance's record, as `renew` describes.
    async fn write_own(&self, expires_ms: u64) -> Result<()> {
        let key = node_key(&self.node);
        let mut guard = self
            .own
            .borrow()
            .clone()
            .map_or(Guard::Absent, Guard::Match);
        loop {
            // A renewal that lands after `deregister` would bring this
            // instance back to life.
            if self.left.get() {
                return Err(Error::NotHere(format!("{} left the cluster", self.node)));
            }
            // Built for each attempt, so that a renewal that races
            // `start_draining` retries with the flag set.
            let body = serde_json::to_vec(&NodeRecord {
                node: self.node.clone(),
                address: self.address.clone(),
                expires_ms,
                instance: self.instance,
                draining: self.draining.get(),
                write: self.next_write(),
                machine: self.machine.borrow().clone(),
            })?;
            let sent = match &guard {
                Guard::Match(version) => Some(version.clone()),
                Guard::Absent => None,
            };
            match self.bucket.put_if(&key, body, guard).await {
                Ok(Some(version)) => {
                    self.own.replace(Some(version));
                    return Ok(());
                }
                Ok(None) | Err(Error::OutcomeUnknown(_)) => {}
                Err(error) => return Err(error),
            }
            // Not the version we know, or a refusal of this renewal's own
            // write, whose first answer was lost. If this process wrote it,
            // earlier renewals landed and only their answers were lost
            // (timed-out rounds on the fleet, or a request sent again after
            // it stalled that landed first), and one may land between our
            // read and write: retry. Anything else is another writer. The
            // record tells this process's writes from others', not one of
            // them from another, and need not: only a write that answers its
            // version renews the lease. Each of our writes names the version
            // it replaces, and no two of our writes have one body
            // (`NodeRecord::write`), so no version of ours comes back once
            // replaced: a late write lands only while the version it names
            // is current, and is refused after.
            let Some((current, version)) = self.bucket.get(&key).await? else {
                return Err(Error::Fenced(self.node.clone()));
            };
            let current: NodeRecord = serde_json::from_slice(&current)?;
            if current.instance != self.instance {
                return Err(Error::Fenced(self.node.clone()));
            }
            // Refused, but the version it named is still there: a write of
            // ours to it is in progress, which the bucket answered with a
            // conflict (R2's 409), and the read does not show yet. Writing
            // again at once would race it again.
            if sent.as_ref() == Some(&version) {
                tokio::time::sleep(self.contended_pause).await;
            }
            guard = Guard::Match(version);
        }
    }

    /// Expire this instance's record on a clean shutdown, so peers stop
    /// routing to it before its lease would have run out and adopt its
    /// alarms. The record goes once they have. A renewal of ours that lands
    /// first is expired too; no renewal starts after this.
    pub async fn deregister(&self) -> Result<()> {
        self.leave();
        let Some(mut version) = self.own.take() else {
            return Ok(());
        };
        let key = node_key(&self.node);
        for _ in 0..DEREGISTER_ATTEMPTS {
            let record = NodeRecord {
                node: self.node.clone(),
                address: self.address.clone(),
                expires_ms: 0,
                instance: self.instance,
                draining: true,
                write: self.next_write(),
                machine: self.machine.borrow().clone(),
            };
            let body = serde_json::to_vec(&record)?;
            let guard = Guard::Match(version);
            match self.bucket.put_if(&key, body, guard).await {
                Ok(Some(_)) => return Ok(()),
                Ok(None) | Err(Error::OutcomeUnknown(_)) => {}
                Err(error) => return Err(error),
            }
            let Some((current, current_version)) = self.bucket.get(&key).await? else {
                return Ok(());
            };
            let current: NodeRecord = serde_json::from_slice(&current)?;
            if current.instance != self.instance {
                return Err(Error::Fenced(self.node.clone()));
            }
            version = current_version;
        }
        Err(Error::Contended(key))
    }

    /// Marks this instance's record as draining from its next renewal.
    pub fn start_draining(&self) {
        self.draining.set(true);
    }

    /// Marks this instance as having left the cluster, without writing its
    /// record: for the registry that reads the peers while another, on the
    /// lease thread, writes the record (`keeper::Thread`).
    pub fn leave(&self) {
        self.left.set(true);
    }

    /// Whether this instance left: `deregister` or `leave` ran.
    pub fn left(&self) -> bool {
        self.left.get()
    }

    /// Whether `node` held a lease past `now_ms` at the last listing. No
    /// I/O: a route checks it on every use.
    pub fn listed_live(&self, node: &str, now_ms: u64) -> bool {
        self.listed
            .borrow()
            .get(node)
            .is_some_and(|record| record.expires_ms > now_ms)
    }

    /// A peer's record while its lease holds, from the last listing when it
    /// was there: routing asks on every request, and a GET per request was
    /// most of a routed request's time.
    pub async fn live(&self, node: &str, now_ms: u64) -> Result<Option<NodeRecord>> {
        if let Some(record) = self.listed.borrow().get(node)
            && record.expires_ms > now_ms
        {
            return Ok(Some(record.clone()));
        }
        self.fetch(node, now_ms).await
    }

    /// A peer's record while its lease holds, read now, whatever the last
    /// listing said.
    pub async fn live_now(&self, node: &str, now_ms: u64) -> Result<Option<NodeRecord>> {
        self.fetch(node, now_ms).await
    }

    /// A peer's record while its lease holds. One found dead leaves the
    /// listing, as its lease running out would, so that routes to it go
    /// before the next listing.
    ///
    /// With a liveness provider, the provider is asked first, and answers
    /// from a read of the peer's machine lease that started after it was
    /// asked. A peer that no machine lease has live may still renew a record
    /// of its own: a `bucket` instance during a switch, live by its record.
    async fn fetch(&self, node: &str, now_ms: u64) -> Result<Option<NodeRecord>> {
        if let Some(agent) = &self.agent {
            let probed = agent.probe(node).await?.map(NodeRecord::of);
            match probed {
                Some(record) if record.expires_ms > now_ms => {
                    self.listed
                        .borrow_mut()
                        .insert(node.to_owned(), record.clone());
                    return Ok(Some(record));
                }
                // Listed, by a lease that expired: no record of its own says
                // more, as one that a lease lists is the provider's.
                Some(_) if !self.both => {
                    self.listed.borrow_mut().remove(node);
                    return Ok(None);
                }
                _ => {}
            }
        }
        self.read_record(node, now_ms).await
    }

    /// A peer's record while the record's own lease holds: today's rule.
    async fn read_record(&self, node: &str, now_ms: u64) -> Result<Option<NodeRecord>> {
        Ok(self.read_held(node, now_ms).await?.0)
    }

    /// As `read_record`, and whether the record is one a machine's lease
    /// holds (`HELD_BY_MACHINE`), whose expiry says nothing.
    async fn read_held(&self, node: &str, now_ms: u64) -> Result<(Option<NodeRecord>, bool)> {
        let Some((body, _)) = self.bucket.get(&node_key(node)).await? else {
            self.listed.borrow_mut().remove(node);
            return Ok((None, false));
        };
        let record: NodeRecord = serde_json::from_slice(&body)?;
        let held = record.machine.is_some() && record.expires_ms == HELD_BY_MACHINE;
        if record.machine.is_none()
            && record.expires_ms + DEAD_RECORD_TTLS * self.ttl_ms < now_ms
            && self
                .bucket
                .list(&format!("alarms/{node}/"))
                .await?
                .is_empty()
        {
            // A fenced or crashed instance leaves its record, every restart
            // adds one, and every listing read them all: 40 after an
            // afternoon of fleet tests. One this long expired belongs to a
            // process that fenced long ago; were it still running, its next
            // renewal's conditional write would fail and fence it. It stays
            // while it has alarms, so that survivors find them. No process
            // takes its node id again, so a late copy of this deletion
            // (`http::Repeat::Delete`) deletes nothing written since. A record
            // a machine's lease backs is not judged by its own expiry: the
            // scan of instances whose provider alone keeps them deletes it.
            self.bucket.delete(&node_key(node)).await?;
        }
        // A record a machine's lease backs, read by an instance whose
        // provider alone says who lives: the provider already said. With
        // both, a record that no lease of its own renews is the provider's
        // to judge too. Without a provider, `HELD_BY_MACHINE` keeps it live:
        // nothing here can tell whether its machine's lease holds.
        let backed = record.machine.is_some()
            && (self.external() || (self.both && record.expires_ms == HELD_BY_MACHINE));
        if record.expires_ms <= now_ms || backed {
            self.listed.borrow_mut().remove(node);
            return Ok((None, held));
        }
        // Until the next listing: one read serves every request that finds
        // the listing stale, and a stale listing is when the bucket is busy.
        self.listed
            .borrow_mut()
            .insert(node.to_owned(), record.clone());
        Ok((Some(record), held))
    }

    /// Every live instance, this one included when its lease holds.
    pub async fn peers(&self, now_ms: u64) -> Result<Vec<NodeRecord>> {
        let Some(agent) = &self.agent else {
            return self.bucket_peers(now_ms).await;
        };
        if self.both {
            // Live by either rule: by its own record, or by its machine's
            // lease. The listing is as old as the older of the two.
            let view = agent.watch().await?;
            let (mut peers, mut dead, held) = self.bucket_records(now_ms).await?;
            for record in view.instances.into_iter().map(NodeRecord::of) {
                if record.expires_ms > now_ms && !peers.iter().any(|p| p.node == record.node) {
                    peers.push(record);
                }
            }
            dead.retain(|node| !peers.iter().any(|p| &p.node == node));
            // An `external` peer's record says nothing by its expiry: a view
            // too old to list it proves no death (as `external_peers`). A
            // fresh one that does not list it does, and its record goes once
            // old enough and without alarms, as the scan of an `external`
            // instance deletes it: a crashed one never rewrites it, and a
            // reader without a provider would take it for live for good.
            if view.age_ms < self.ttl_ms / 2 {
                for (node, modified_ms) in &held {
                    if dead.contains(node)
                        && modified_ms + DEAD_RECORD_TTLS * self.ttl_ms < now_ms
                        && self
                            .bucket
                            .list(&format!("alarms/{node}/"))
                            .await?
                            .is_empty()
                    {
                        self.bucket.delete(&node_key(node)).await?;
                    }
                }
            } else {
                dead.retain(|node| !held.iter().any(|(h, _)| h == node));
            }
            *self.dead.borrow_mut() = dead;
            self.list(&peers, now_ms.saturating_sub(view.age_ms));
            return Ok(peers);
        }
        self.external_peers(agent, now_ms).await
    }

    /// Every live instance by the bucket's records alone.
    async fn bucket_peers(&self, now_ms: u64) -> Result<Vec<NodeRecord>> {
        let (peers, dead, _) = self.bucket_records(now_ms).await?;
        *self.dead.borrow_mut() = dead;
        self.list(&peers, now_ms);
        Ok(peers)
    }

    /// The live records in `nodes/`, the ids of those that are not, and the
    /// ids and modification times of the records machines' leases hold.
    async fn bucket_records(
        &self,
        now_ms: u64,
    ) -> Result<(Vec<NodeRecord>, Vec<String>, Vec<(String, u64)>)> {
        // The records are read concurrently: a lease round is bounded, and
        // reading them one after another made it time out on R2's spikes.
        let listed = self.bucket.list("nodes/").await?;
        let nodes = listed.iter().filter_map(|entry| {
            entry
                .key
                .strip_prefix("nodes/")
                .and_then(|key| key.strip_suffix(".json"))
        });
        let nodes: Vec<_> = nodes.collect();
        let fetched =
            futures::future::join_all(nodes.iter().map(|n| self.read_held(n, now_ms))).await;
        let mut peers = Vec::new();
        let mut held = Vec::new();
        for (fetched, node) in fetched.into_iter().zip(&nodes) {
            let (record, is_held) = fetched?;
            if is_held {
                let entry = listed.iter().find(|e| e.key == node_key(node));
                held.push(((*node).to_owned(), entry.map_or(now_ms, |e| e.modified_ms)));
            }
            peers.extend(record);
        }
        let dead = nodes
            .into_iter()
            .filter(|n| !peers.iter().any(|p| p.node == *n))
            .map(str::to_owned)
            .collect();
        Ok((peers, dead, held))
    }

    /// Every live instance as the liveness provider sees them, with peers
    /// that renew records of their own as the last scan of `nodes/` found
    /// them. The scan, which also finds the dead instances whose alarms
    /// survivors adopt, runs as this instance starts, once a lease's time
    /// three times over, and whenever an instance leaves the view.
    async fn external_peers(&self, agent: &Agent, now_ms: u64) -> Result<Vec<NodeRecord>> {
        let view = agent.watch().await?;
        let listed_ms = now_ms.saturating_sub(view.age_ms);
        let mut peers: Vec<NodeRecord> = view
            .instances
            .into_iter()
            .map(NodeRecord::of)
            .filter(|record| record.expires_ms > now_ms)
            .collect();
        let live: HashSet<String> = peers.iter().map(|p| p.node.clone()).collect();
        self.seen.borrow_mut().extend(live.iter().cloned());
        // A view older than half a lease may have lost live instances to its
        // age alone (their machines' leases expired in it, not in the
        // bucket): absence from it proves no death, so it starts no scan and
        // ends no instance's place in the last fresh one.
        let fresh = view.age_ms < self.ttl_ms / 2;
        let left = fresh && self.viewed.borrow().iter().any(|node| !live.contains(node));
        if fresh {
            *self.viewed.borrow_mut() = live.clone();
        }
        let due = self
            .scanned_ms
            .get()
            .is_none_or(|at| now_ms >= at + SCAN_LEASES * self.ttl_ms);
        if fresh && (left || due) {
            match self.scan(&live, listed_ms, now_ms).await {
                Ok(()) => self.scanned_ms.set(Some(now_ms)),
                Err(e) => tracing::warn!("cluster: scanning the instance records failed: {e}"),
            }
        }
        for record in self.renewing.borrow().values() {
            if record.expires_ms > now_ms && !live.contains(&record.node) {
                peers.push(record.clone());
            }
        }
        self.list(&peers, listed_ms);
        Ok(peers)
    }

    /// Reads `nodes/` for the instances no view has live: peers that renew
    /// records of their own, and the dead. A record is dead once an earlier
    /// view had it live, or once it is older than a lease before the view's
    /// listing: an instance writes it after its machine's lease names it.
    /// Dead records go once 10 leases old and without alarms.
    async fn scan(&self, live: &HashSet<String>, listed_ms: u64, now_ms: u64) -> Result<()> {
        let entries = self.bucket.list("nodes/").await?;
        let mut dead = Vec::new();
        let mut renewing = HashMap::new();
        for entry in entries {
            let Some(node) = entry
                .key
                .strip_prefix("nodes/")
                .and_then(|key| key.strip_suffix(".json"))
            else {
                continue;
            };
            if live.contains(node) || node == self.node {
                continue;
            }
            let Some((body, _)) = self.bucket.get(&entry.key).await? else {
                continue;
            };
            let Ok(record) = serde_json::from_slice::<NodeRecord>(&body) else {
                continue;
            };
            if record.machine.is_none() && record.expires_ms > now_ms {
                renewing.insert(node.to_owned(), record);
                continue;
            }
            let old = entry.modified_ms + self.ttl_ms < listed_ms;
            if !old && !self.seen.borrow().contains(node) {
                continue;
            }
            dead.push(node.to_owned());
            // A record a machine's lease backs says nothing by its expiry.
            let expired = if record.machine.is_some() {
                entry.modified_ms
            } else {
                record.expires_ms.max(entry.modified_ms)
            };
            if expired + DEAD_RECORD_TTLS * self.ttl_ms < now_ms
                && self
                    .bucket
                    .list(&format!("alarms/{node}/"))
                    .await?
                    .is_empty()
            {
                // As `read_record` deletes one: no process takes the id again.
                self.bucket.delete(&entry.key).await?;
                self.seen.borrow_mut().remove(node);
            }
        }
        *self.dead.borrow_mut() = dead;
        *self.renewing.borrow_mut() = renewing;
        Ok(())
    }

    /// Takes `peers` as the listing that started at `listed_ms`.
    fn list(&self, peers: &[NodeRecord], listed_ms: u64) {
        *self.listed.borrow_mut() = peers.iter().map(|p| (p.node.clone(), p.clone())).collect();
        // Listings overlap (`Ownership::successor` lists too): the latest
        // started is kept.
        let mut live: Vec<_> = peers.iter().map(|p| p.node.clone()).collect();
        live.sort();
        let mut listing = self.listing.borrow_mut();
        if listing.as_ref().is_none_or(|(at, _)| *at <= listed_ms) {
            *listing = Some((listed_ms, live));
        }
    }

    /// When the latest listing that succeeded started, and the live nodes it
    /// found, this one included, sorted.
    pub fn listing(&self) -> Option<(u64, Vec<String>)> {
        self.listing.borrow().clone()
    }

    /// The live nodes, this one included, as of the last listing.
    pub fn listed_nodes(&self, now_ms: u64) -> Vec<String> {
        let listed = self.listed.borrow();
        let live = listed.values().filter(|r| r.expires_ms > now_ms);
        live.map(|r| r.node.clone()).collect()
    }

    /// The live nodes that are not draining, as of the last listing: those
    /// that may take objects and alarms on.
    pub fn serving_nodes(&self, now_ms: u64) -> Vec<String> {
        let listed = self.listed.borrow();
        let live = listed
            .values()
            .filter(|r| r.expires_ms > now_ms && !r.draining);
        live.map(|r| r.node.clone()).collect()
    }

    /// Whether `node` has a record, live or not.
    pub async fn recorded(&self, node: &str) -> Result<bool> {
        Ok(self.bucket.get(&node_key(node)).await?.is_some())
    }

    /// Dead nodes whose records remain, as of the last listing.
    pub fn dead(&self) -> Vec<String> {
        self.dead.borrow().clone()
    }
}

/// The `expires_ms` of an instance whose machine's lease keeps it live
/// (`Liveness::External`): never, to a reader that judges records by their
/// own expiry, as an older workerd or one in `bucket` mode does. Such a reader
/// never takes the instance's objects over, which only waits; were it 0, it
/// would take them over from a live instance. 2^53 - 1: no reader's sum
/// overflows, and JavaScript reads it exactly. Rewritten to 0 as it leaves.
pub const HELD_BY_MACHINE: u64 = (1 << 53) - 1;

/// How many lease lifetimes past its expiry a node record is deleted.
const DEAD_RECORD_TTLS: u64 = 10;
/// How many lease lifetimes apart an instance whose provider alone says who
/// lives scans `nodes/`, if no instance left its view meanwhile.
const SCAN_LEASES: u64 = 3;
/// How many renewals of ours in flight a deregistration outlasts.
const DEREGISTER_ATTEMPTS: usize = 4;

#[cfg(test)]
#[path = "registry-test.rs"]
mod tests;
