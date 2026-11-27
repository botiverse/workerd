// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

//! Keeps this node's lease: renews its record in the bucket and decides
//! when the node must stop serving.
//!
//! A serving node keeps its lease on a thread of its own (`Thread`), with
//! its own tokio runtime and its own bucket client, so that a renewal waits
//! on the bucket alone. The server's event loop runs JavaScript, SQLite and
//! file work in turns of any length, and a renewal that waited for a turn
//! to end would lose the lease, and with it every object the node owns, to
//! one long turn.
//!
//! The lease thread shares two things with the loop thread: the lease's
//! expiry (`Held`), which the loop reads before it opens storage or starts
//! an actor, and the commands the loop sends it (`Command`). It writes the
//! node's record and nothing else; the loop reads the peers' records with a
//! registry of its own.
//!
//! The lease therefore says that the process runs and reaches the bucket,
//! not that its event loop takes turns. So once the node serves, the lease
//! thread also watches the loop (`cluster_logic::watchdog`): it reads a
//! count the loop advances while it makes progress (`Progress`), once a
//! round, and a count that stands still for `Timing::stall_ms` fences the
//! node as a lease that cannot be kept does. Not before: linking, after
//! `join`, runs in turns that last as long as it does.

use std::cell::Cell;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use cluster_logic::lease;
use cluster_logic::lease::Lease;
use cluster_logic::lease::Step;
use cluster_logic::lease::Timing;
use cluster_logic::watchdog::Watchdog;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::time::timeout;

use crate::Bucket;
use crate::Error;
use crate::Registry;
use crate::Result;
use crate::agent::Agent;
use crate::agent::Liveness;
use crate::agent::mono_ms;
use crate::cluster::now_ms;
use crate::http::Repeat;
use crate::http::prompt;

/// The lease's expiry, as its keeper last renewed it: what the loop thread
/// reads. Clones share it.
///
/// With an external liveness provider (`Liveness`), the lease is the
/// provider's grant instead, or as well: its deadline is on this process's
/// monotonic clock (`agent::mono_ms`), from when the request that won it was
/// sent, and needs no margin, as the provider's grant leaves the time peers'
/// clocks may be ahead. With both, the lease holds while both do.
#[derive(Clone)]
pub struct Held {
    /// 0 before the first renewal.
    until_ms: Arc<AtomicU64>,
    /// The grant's deadline, by `mono_ms`; 0 before the first.
    granted_ms: Arc<AtomicU64>,
    /// Whether the instance left the cluster (`Keeper::deregister`).
    left: Arc<AtomicBool>,
    timing: Timing,
    /// Whether the bucket lease counts, and whether the grant does.
    bucket: bool,
    agent: bool,
}

impl Held {
    fn new(timing: Timing, liveness: &Liveness) -> Self {
        Self {
            until_ms: Arc::default(),
            granted_ms: Arc::default(),
            left: Arc::default(),
            timing,
            bucket: liveness.renews_record(),
            agent: liveness.socket().is_some(),
        }
    }

    /// How much longer the grant holds, in ms by `mono_ms`: 0 once it does
    /// not.
    fn grant_remaining_ms(&self) -> u64 {
        self.granted_ms
            .load(Ordering::Acquire)
            .saturating_sub(mono_ms())
    }

    /// Whether the lease holds at `now_ms`, with peers' clocks allowed for.
    pub fn holds(&self, now_ms: u64) -> bool {
        (!self.bucket
            || self
                .timing
                .holds(self.until_ms.load(Ordering::Acquire), now_ms))
            && (!self.agent || self.grant_remaining_ms() > 0)
    }

    /// How much longer than `now_ms` the lease holds: zero once it does
    /// not.
    pub fn remaining(&self, now_ms: u64) -> Duration {
        let mut remaining = u64::MAX;
        if self.bucket {
            let until_ms = self.until_ms.load(Ordering::Acquire);
            remaining = self.timing.remaining_ms(until_ms, now_ms);
        }
        if self.agent {
            remaining = remaining.min(self.grant_remaining_ms());
        }
        ms(remaining)
    }

    /// [`Lease::holds_until`] of the lease its keeper keeps, by the wall
    /// clock: a grant's deadline as far past `now_ms()` as it is past
    /// `mono_ms()`.
    pub fn holds_until(&self) -> u64 {
        if self.left.load(Ordering::Acquire) {
            return 0;
        }
        let mut until = u64::MAX;
        if self.bucket {
            until = self
                .timing
                .holds_until(self.until_ms.load(Ordering::Acquire));
        }
        if self.agent {
            let remaining = self.grant_remaining_ms();
            until = until.min(if remaining == 0 {
                0
            } else {
                now_ms() + remaining
            });
        }
        until
    }
}

/// The lease and the node's record, on whichever thread renews them.
pub struct Keeper {
    /// Writes this node's record.
    registry: Rc<Registry>,
    /// Measures this node's clock against the bucket's.
    bucket: Bucket,
    /// The liveness provider that grants the lease, if one does.
    agent: Option<Agent>,
    lease: RefCell<Lease>,
    /// The provider's grant, by `mono_ms`, with no margin for peers' clocks:
    /// the provider's grant leaves it.
    grant: RefCell<Lease>,
    held: Held,
    /// The loop the rounds watch, from `watch` on.
    watched: RefCell<Option<Progress>>,
}

impl Keeper {
    /// A keeper whose lease `liveness` keeps.
    pub fn with(
        bucket: Bucket,
        registry: Rc<Registry>,
        timing: Timing,
        liveness: &Liveness,
    ) -> Self {
        Self {
            registry,
            bucket,
            agent: liveness.socket().map(|socket| Agent::new(socket.clone())),
            lease: RefCell::new(Lease::new(timing)),
            grant: RefCell::new(Lease::new(Timing {
                max_skew_ms: 0,
                ..timing
            })),
            held: Held::new(timing, liveness),
            watched: RefCell::new(None),
        }
    }

    /// From here on, the rounds watch the loop that `progress` counts.
    pub fn watch(&self, progress: Progress) {
        *self.watched.borrow_mut() = Some(progress);
    }

    pub fn held(&self) -> Held {
        self.held.clone()
    }

    /// Whether this instance left the cluster (`deregister`): it renews its
    /// lease no more. Answers no if the lease is borrowed, so that a drop
    /// that asks never panics.
    fn left(&self) -> bool {
        self.lease.try_borrow().is_ok_and(|lease| lease.left)
    }

    /// How far this node's wall clock is outside the bucket's, in ms; 0
    /// before any response.
    fn clock_skew_ms(&self) -> i64 {
        self.bucket.clock_skew_ms().unwrap_or(0)
    }

    /// Reports a renewal's outcome to the lease, and a new expiry to
    /// whoever reads `Held`.
    fn renewed(&self, result: std::result::Result<u64, String>) -> Step {
        self.renewed_at(now_ms(), result)
    }

    /// As `renewed`, for a renewal that answered at `now_ms`.
    pub(crate) fn renewed_at(&self, now_ms: u64, result: std::result::Result<u64, String>) -> Step {
        if let Ok(until_ms) = result {
            self.held.until_ms.store(until_ms, Ordering::Release);
        }
        self.lease.borrow_mut().renewed(now_ms, result)
    }

    /// Reports a heartbeat's or registration's outcome: the grant's new
    /// deadline by `mono_ms`, or why it failed. The latest answer stands,
    /// even when shorter: it is what the provider can grant now.
    fn granted(&self, result: std::result::Result<u64, String>) -> Step {
        if let Ok(deadline) = result {
            self.held.granted_ms.store(deadline, Ordering::Release);
        }
        self.grant.borrow_mut().renewed(mono_ms(), result)
    }

    /// Keeps the lease until the node must stop serving, or until the
    /// loop it watches (`watch`) makes no progress: answers why. Runs for
    /// the life of the process.
    pub async fn maintain(&self) -> String {
        let timing = self.held.timing;
        let started = std::time::Instant::now();
        // Its clock starts at its first observation, once there is a loop
        // to watch.
        let mut watchdog = Watchdog::new(&timing);
        loop {
            if self.lease.borrow().left {
                std::future::pending::<()>().await;
            }
            let (bucket, agent) = futures::join!(self.bucket_round(), self.agent_round());
            // The instance left meanwhile, which may have failed this
            // renewal: what it found no longer matters.
            if self.lease.borrow().left {
                continue;
            }
            let delay = match worse(bucket, agent) {
                Step::Fence(reason) => return lease_lost(reason),
                Step::Retry(reason) => {
                    tracing::warn!("renewing the cluster lease failed; retrying: {reason}");
                    timing.retry_ms
                }
                Step::Renew => timing.interval_ms,
            };
            // After the lease: a round that a stop of the whole process
            // delayed finds the lease lapsed, and says so.
            let now_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
            let stalled = {
                let watched = self.watched.borrow();
                let progress = watched.as_ref();
                progress.and_then(|progress| watchdog.observe(now_ms, progress()))
            };
            if let Some(reason) = stalled {
                return reason;
            }
            tokio::time::sleep(ms(delay)).await;
        }
    }

    /// A round's renewal of the record in the bucket, if the instance renews
    /// one.
    async fn bucket_round(&self) -> Step {
        if !self.held.bucket {
            return Step::Renew;
        }
        let timing = self.held.timing;
        let observed = lease::Observed {
            now_ms: now_ms(),
            skew_ms: self.clock_skew_ms(),
        };
        let step = self.lease.borrow().before_renew(&observed);
        if step != Step::Renew {
            return step;
        }
        let renew = timeout(ms(timing.renew_timeout_ms), self.renew_round());
        let result = match renew.await {
            Ok(Ok(until)) => Ok(until),
            Ok(Err(e)) => Err(e.to_string()),
            Err(_) => Err("renewal timed out".into()),
        };
        if self.lease.borrow().left {
            return Step::Renew;
        }
        self.renewed(result)
    }

    /// A round's heartbeat to the liveness provider, if one grants the
    /// lease. The provider checks the machine's clock against the bucket's.
    async fn agent_round(&self) -> Step {
        if self.agent.is_none() {
            return Step::Renew;
        }
        let observed = lease::Observed {
            now_ms: mono_ms(),
            skew_ms: 0,
        };
        let step = self.grant.borrow().before_renew(&observed);
        if step != Step::Renew {
            return step;
        }
        let result = self.heartbeat().await;
        if self.lease.borrow().left {
            return Step::Renew;
        }
        // A provider that grants nothing (its lease lapsed, or this machine's
        // clock is off the bucket's) says to stop now, not at the next round.
        if let Ok((sent, deadline)) = result
            && deadline <= sent
        {
            drop(self.granted(Ok(deadline)));
            return Step::Fence("the liveness provider grants no lease".into());
        }
        self.granted(result.map(|(_, deadline)| deadline))
    }

    /// A heartbeat, bounded as a renewal is: when it was sent and the grant's
    /// deadline from then, or why it failed.
    async fn heartbeat(&self) -> std::result::Result<(u64, u64), String> {
        let Some(agent) = &self.agent else {
            return Err("no liveness provider".into());
        };
        let progress = self
            .watched
            .borrow()
            .as_ref()
            .map_or(0, |progress| progress());
        let sent = mono_ms();
        let beat = agent.heartbeat(self.registry.node(), progress, self.registry.draining());
        match timeout(ms(self.held.timing.renew_timeout_ms), beat).await {
            Ok(Ok(grant)) => Ok((sent, sent.saturating_add(grant.valid_for_ms))),
            Ok(Err(e)) => Err(e.to_string()),
            Err(_) => Err("the liveness provider did not answer in time".into()),
        }
    }

    /// A lease round's renewal. A request of it that stalls is sent once
    /// more (`http::prompt`), so that one slow answer does not time the
    /// round out: two rounds timed out in a row fence the node. The
    /// renewal's guarded writes may be: it rereads on a refusal
    /// (`Registry::renew`).
    async fn renew_round(&self) -> Result<u64> {
        let repeat = &[Repeat::Read, Repeat::GuardedWrite];
        let attempt = ms(self.held.timing.renew_attempt_ms);
        prompt(attempt, repeat, self.registry.renew(now_ms())).await
    }

    /// Renews the lease outside the maintenance rounds, whether or not it
    /// lapsed: as the node joins, before the rounds start. With a liveness
    /// provider, registers with it first; one that is not up yet, or has no
    /// lease to grant from yet, as its machine starts, is asked again for up
    /// to a lease.
    /// Answers the machine the provider placed the instance on.
    pub async fn take_lease(&self) -> Result<Option<String>> {
        let mut machine = None;
        if let Some(agent) = &self.agent {
            let timing = self.held.timing;
            let deadline = mono_ms() + timing.lease_ms;
            let grant = loop {
                let sent = mono_ms();
                let registered = agent
                    .register(
                        self.registry.node(),
                        self.registry.advertise(),
                        self.registry.draining(),
                    )
                    .await;
                match registered {
                    Ok(grant) if grant.valid_for_ms > 0 => break (sent, grant),
                    Ok(_) if mono_ms() < deadline => {}
                    // Not listening yet, or not holding its own lease yet.
                    Err(Error::Http(_)) if mono_ms() < deadline => {}
                    Ok(_) => {
                        return Err(Error::Http("the liveness provider grants no lease".into()));
                    }
                    Err(e) => return Err(e),
                }
                tokio::time::sleep(ms(timing.retry_ms)).await;
            };
            let (sent, grant) = grant;
            machine = grant.machine.clone();
            self.registry.set_machine(machine.clone());
            self.granted(Ok(sent.saturating_add(grant.valid_for_ms)));
        }
        if self.held.bucket {
            let until = self.registry.renew(now_ms()).await?;
            self.renewed(Ok(until));
        }
        Ok(machine)
    }

    /// The record says that this instance drains from its next renewal.
    pub fn start_draining(&self) {
        self.registry.start_draining();
    }

    /// Publishes that this instance drains, so that peers choose it for no
    /// object or alarm.
    pub async fn announce_draining(&self) {
        if self.held.bucket {
            let renew = timeout(ms(self.held.timing.renew_timeout_ms), self.renew_round());
            match renew.await {
                Ok(Ok(_)) => {}
                // The node left the cluster first: `deregister`, which runs at
                // once on a node with nothing to hand off, published its record
                // expired and draining.
                Ok(Err(Error::NotHere(_))) if self.registry.left() => {}
                Ok(Err(e)) => tracing::warn!("cluster: publishing the drain failed: {e}"),
                Err(_) => tracing::warn!("cluster: publishing the drain timed out"),
            }
        }
        if self.agent.is_some() && !self.lease.borrow().left {
            match self.heartbeat().await {
                Ok((_, deadline)) => drop(self.granted(Ok(deadline))),
                Err(e) => tracing::warn!("cluster: publishing the drain failed: {e}"),
            }
        }
    }

    /// Expires this instance's membership: it renews its lease no more.
    pub async fn deregister(&self) -> Result<()> {
        self.lease.borrow_mut().left = true;
        self.held.left.store(true, Ordering::Release);
        let left = match &self.agent {
            Some(agent) => agent.deregister(self.registry.node()).await,
            None => Ok(()),
        };
        // A record the instance never wrote (`Liveness::External`) is left
        // to the loop thread, which wrote it.
        self.registry.deregister().await.and(left)
    }
}

/// The step that two rounds' outcomes make together: a fence if either
/// fences, a retry if either failed.
fn worse(a: Step, b: Step) -> Step {
    match (a, b) {
        (Step::Fence(reason), _) | (_, Step::Fence(reason)) => Step::Fence(reason),
        (Step::Retry(reason), _) | (_, Step::Retry(reason)) => Step::Retry(reason),
        (Step::Renew, Step::Renew) => Step::Renew,
    }
}

/// What the loop thread asks of the lease thread. Each runs as a task
/// beside the maintenance rounds, so none waits for a round's renewal.
enum Command {
    /// `Keeper::take_lease`.
    Take(oneshot::Sender<Result<Option<String>>>),
    /// Start `Keeper::maintain`, once.
    Maintain,
    /// `Keeper::watch`.
    Watch(Progress),
    /// `Keeper::start_draining`.
    Drain,
    /// `Keeper::announce_draining`.
    Announce(oneshot::Sender<()>),
    /// `Keeper::deregister`.
    Leave(oneshot::Sender<Result<()>>),
    /// End the thread, cancelling whatever it was doing.
    Stop,
    /// Panic in the thread's body, outside any task.
    #[cfg(test)]
    Panic,
}

/// Called on the lease thread, at most once, when the lease cannot be kept
/// or the loop makes no progress.
///
/// That is when its rounds end, or when the thread ends while they run
/// without being told to stop. Called with the reason, which says which
/// ("its lease cannot be kept: ...", "its event loop made no progress ..."),
/// and how much longer the lease holds, it must end the process itself,
/// within that time.
///
/// The process must be gone before peers may take the node's objects over,
/// and the loop thread may be in a turn that never ends.
pub type Fence = Box<dyn FnOnce(String, Duration) + Send>;

/// Runs the lease thread's body, on the lease thread, inside what the
/// process sets up for a thread of its own: its logger.
pub type Within = Box<dyn FnOnce(Box<dyn FnOnce() + Send>) + Send>;

/// Reads, on the lease thread, a count that the server's event loop
/// advances while it makes progress.
///
/// It stands still while one turn holds the loop, and through the busy
/// stretches that `kj_rs_tokio::Progress` names.
pub type Progress = Box<dyn Fn() -> u64 + Send>;

/// How long a lease thread told to stop may take to end. Stopping cancels
/// its requests, but tokio waits for work on its blocking pool, such as a
/// DNS lookup, that no cancellation reaches: the thread leaves such work
/// behind after `SHUTDOWN`, and its owner leaves the thread behind after
/// `STOP`.
const SHUTDOWN: Duration = Duration::from_millis(500);
const STOP: Duration = Duration::from_secs(1);

/// The lease thread, as the loop thread holds it. Dropping it ends the
/// thread and waits for it, within `STOP`.
pub struct Thread {
    commands: mpsc::UnboundedSender<Command>,
    held: Held,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl Thread {
    /// Starts the lease thread for `node` at `address`, its body `within`
    /// the process's setup. `open` opens the thread's bucket client on it;
    /// what it answers is this call's answer.
    pub fn spawn(
        open: impl FnOnce() -> Result<Bucket> + Send + 'static,
        node: String,
        address: String,
        timing: Timing,
        liveness: Liveness,
        fence: Fence,
        within: Within,
    ) -> Result<Self> {
        let (commands, received) = mpsc::unbounded_channel();
        let (started, start) = std::sync::mpsc::channel();
        let thread = std::thread::Builder::new().name("cluster-lease".into());
        let thread = thread.spawn(move || {
            within(Box::new(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build();
                let keeper = runtime.map_err(Error::Io).and_then(|runtime| {
                    // A bucket's client is made inside the runtime it runs on.
                    let bucket = {
                        let _runtime = runtime.enter();
                        open()?
                    };
                    let registry = Registry::new(bucket.clone(), &node, &address, timing.lease_ms)
                        .contended_pause(ms(timing.contended_pause_ms));
                    let keeper = Keeper::with(bucket, Rc::new(registry), timing, &liveness);
                    Ok((runtime, keeper))
                });
                let (runtime, keeper) = match keeper {
                    Ok(keeper) => keeper,
                    Err(e) => {
                        let _ = started.send(Err(e));
                        return;
                    }
                };
                let _ = started.send(Ok(keeper.held()));
                let keeper = Rc::new(keeper);
                let armed = Armed::default();
                let tasks = tokio::task::LocalSet::new();
                // Declared after the tasks, so that a panic drops it first.
                let mut watch = Watch {
                    keeper: Rc::clone(&keeper),
                    armed: armed.clone(),
                    stopped: false,
                };
                let serving = serve(keeper, received, fence, armed);
                watch.stopped = tasks.block_on(&runtime, serving);
                // A body that ends any other way fences here, before it
                // waits for the tasks and the runtime to end.
                drop(watch);
                // The tasks' requests end with them.
                drop(tasks);
                runtime.shutdown_timeout(SHUTDOWN);
            }));
        })?;
        let held = start.recv().map_err(|_| stopped())??;
        Ok(Self {
            commands,
            held,
            handle: Some(thread),
        })
    }

    pub fn held(&self) -> &Held {
        &self.held
    }

    pub async fn take_lease(&self) -> Result<Option<String>> {
        self.ask(Command::Take).await?
    }

    /// Starts the maintenance rounds: from here on, the lease is renewed
    /// until the node leaves, and a lease that cannot be kept ends the
    /// process (`Fence`).
    pub fn maintain(&self) {
        self.send(Command::Maintain);
    }

    /// From here on, the rounds watch the loop that `progress` counts: one
    /// that makes no progress for `Timing::stall_ms` ends the process
    /// (`Fence`).
    pub fn watch(&self, progress: Progress) {
        self.send(Command::Watch(progress));
    }

    pub fn start_draining(&self) {
        self.send(Command::Drain);
    }

    pub async fn announce_draining(&self) {
        if self.ask(Command::Announce).await.is_err() {
            tracing::warn!("cluster: publishing the drain failed: {}", stopped());
        }
    }

    pub async fn deregister(&self) -> Result<()> {
        self.ask(Command::Leave).await?
    }

    fn send(&self, command: Command) {
        // A lease thread that is gone answers nothing, which `ask` reports;
        // what it no longer renews, `Held` stops holding.
        let _ = self.commands.send(command);
    }

    /// Sends the command `command` makes of a reply's sender, and waits
    /// for the reply.
    async fn ask<T>(&self, command: impl FnOnce(oneshot::Sender<T>) -> Command) -> Result<T> {
        let (reply, answer) = oneshot::channel();
        self.send(command(reply));
        answer.await.map_err(|_| stopped())
    }
}

impl Drop for Thread {
    fn drop(&mut self) {
        self.send(Command::Stop);
        let Some(handle) = self.handle.take() else {
            return;
        };
        // A thread that does not end in time is left to the process's exit.
        let deadline = std::time::Instant::now() + STOP;
        while !handle.is_finished() {
            if std::time::Instant::now() >= deadline {
                return;
            }
            std::thread::sleep(ms(1));
        }
        let _ = handle.join();
    }
}

/// What fences a node whose lease cannot be kept, for `reason`.
fn lease_lost(reason: impl std::fmt::Display) -> String {
    format!("its lease cannot be kept: {reason}")
}

fn stopped() -> Error {
    Error::Replica("the cluster lease thread stopped".into())
}

/// The fence, from when the maintenance rounds start: called by whichever
/// ends first, the rounds or the lease thread (`Watch`). Clones share it.
#[derive(Clone, Default)]
struct Armed(Rc<Cell<Option<Fence>>>);

impl Armed {
    fn arm(&self, fence: Fence) {
        self.0.set(Some(fence));
    }

    /// Calls the fence, unless it is not armed or was called already.
    fn fence(&self, reason: String, held: &Held) {
        if let Some(fence) = self.0.take() {
            fence(reason, held.remaining(now_ms()));
        }
    }
}

/// Fences, as it drops, a lease thread whose body ends other than by
/// `Command::Stop`: by a panic outside the tasks, or with its commands' channel
/// closed. Renewals that end with the thread end the process, as do rounds
/// that end. A node whose rounds have not started, or that left the cluster,
/// renews no lease that needs keeping, and is not fenced.
struct Watch {
    keeper: Rc<Keeper>,
    armed: Armed,
    /// The body ends by `Command::Stop`.
    stopped: bool,
}

impl Drop for Watch {
    fn drop(&mut self) {
        if !self.stopped && !self.keeper.left() {
            let reason = lease_lost("the lease thread stopped");
            self.armed.fence(reason, &self.keeper.held);
        }
    }
}

/// The lease thread's work: runs each command as a task of its own until
/// the loop thread drops the `Thread`. Answers whether it ended by
/// `Command::Stop`.
async fn serve(
    keeper: Rc<Keeper>,
    mut commands: mpsc::UnboundedReceiver<Command>,
    fence: Fence,
    armed: Armed,
) -> bool {
    let mut fence = Some(fence);
    while let Some(command) = commands.recv().await {
        let keeper = Rc::clone(&keeper);
        match command {
            Command::Take(reply) => {
                tokio::task::spawn_local(async move {
                    let _ = reply.send(keeper.take_lease().await);
                });
            }
            Command::Maintain => {
                let Some(fence) = fence.take() else { continue };
                armed.arm(fence);
                let armed = armed.clone();
                let held = keeper.held();
                let rounds = tokio::task::spawn_local(async move { keeper.maintain().await });
                tokio::task::spawn_local(async move {
                    // Rounds that ended any other way renew no more either.
                    let reason = rounds
                        .await
                        .unwrap_or_else(|e| lease_lost(format!("the lease's task: {e}")));
                    armed.fence(reason, &held);
                });
            }
            Command::Watch(progress) => keeper.watch(progress),
            Command::Drain => keeper.start_draining(),
            Command::Announce(reply) => {
                tokio::task::spawn_local(async move {
                    keeper.announce_draining().await;
                    let _ = reply.send(());
                });
            }
            Command::Leave(reply) => {
                tokio::task::spawn_local(async move {
                    let _ = reply.send(keeper.deregister().await);
                });
            }
            Command::Stop => return true,
            #[cfg(test)]
            Command::Panic => panic!("the lease thread panics"),
        }
    }
    false
}

/// Where this node's lease is kept.
pub enum Lessee {
    /// On the lease thread: a serving node's.
    Thread(Thread),
    /// On the caller's thread, with the cluster's own bucket and registry:
    /// a test's, which decides the time of each renewal itself and has no
    /// rounds.
    #[cfg(test)]
    Here(Keeper),
}

impl Lessee {
    pub fn held(&self) -> &Held {
        match self {
            Self::Thread(thread) => thread.held(),
            #[cfg(test)]
            Self::Here(keeper) => &keeper.held,
        }
    }

    pub async fn take_lease(&self) -> Result<Option<String>> {
        match self {
            Self::Thread(thread) => thread.take_lease().await,
            #[cfg(test)]
            Self::Here(keeper) => keeper.take_lease().await,
        }
    }

    pub fn watch(&self, progress: Progress) {
        match self {
            Self::Thread(thread) => thread.watch(progress),
            #[cfg(test)]
            Self::Here(keeper) => keeper.watch(progress),
        }
    }

    pub fn start_draining(&self) {
        match self {
            Self::Thread(thread) => thread.start_draining(),
            #[cfg(test)]
            Self::Here(keeper) => keeper.start_draining(),
        }
    }

    pub async fn announce_draining(&self) {
        match self {
            Self::Thread(thread) => thread.announce_draining().await,
            #[cfg(test)]
            Self::Here(keeper) => keeper.announce_draining().await,
        }
    }

    pub async fn deregister(&self) -> Result<()> {
        match self {
            Self::Thread(thread) => thread.deregister().await,
            #[cfg(test)]
            Self::Here(keeper) => keeper.deregister().await,
        }
    }
}

const fn ms(ms: u64) -> Duration {
    Duration::from_millis(ms)
}

#[cfg(test)]
#[path = "keeper-test.rs"]
mod tests;
