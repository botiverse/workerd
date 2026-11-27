// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

//! An instance's lease in the bucket: when to renew and when to stop
//! serving (fence).
//!
//! The executor runs one round every `interval_ms` (`retry_ms` after a
//! failed renewal): it asks `before_renew`, renews if told to and reports
//! the result to `renewed`. It runs on a thread of its own, so a round
//! waits on the bucket alone, never on the server's event loop.

/// A lease's times, in ms. Each is sized against the others, so a test
/// that waits out leases divides them all by one number (`divided`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Timing {
    /// A node's lease. A peer whose record has not been renewed for a full
    /// lease is dead, so the lease spans several rounds.
    pub lease_ms: u64,
    /// Renewals every 3 s, each bounded at 4 s, a failed one retried every
    /// second.
    ///
    /// A node rides out about 8 s of bucket trouble and fences while its
    /// lease holds. R2 stalls writes ~5.5 s at every 5-minute mark; a 2 s
    /// bound cancelled them and fenced every node.
    pub interval_ms: u64,
    /// How often the peers are listed: a LIST of the node records and a
    /// read of each. A listing that comes late only delays routing to a
    /// new peer and adopting a dead one's alarms; a peer's record read in
    /// the meantime stands in for it while its lease holds.
    pub list_interval_ms: u64,
    pub renew_timeout_ms: u64,
    pub list_timeout_ms: u64,
    pub retry_ms: u64,
    /// A round's bucket request that has not answered within its round's
    /// bound is sent once more, the first copy kept in flight
    /// (`http::prompt`). About one request in 8,000 to a bucket stalls for
    /// seconds, and the same request sent again answers in its usual time;
    /// a bucket slow to answer every request answers the first copy
    /// soonest. A listing's LIST answers in 32 ms at the median and 241 ms
    /// at the 99.9th percentile, and R2's usual answers take up to a
    /// second, which no bound cuts short. Each bound is below its round's
    /// deadline.
    ///
    /// The bounds are a request's wall time, so a slowdown of the whole
    /// bucket doubles a round's requests. A listing runs on the server's
    /// event loop, so a stall of the loop past its bound looks like a
    /// stalled bucket and costs a resend.
    ///
    /// A renewal is a write, and after a refusal a reread and a write
    /// again. A stalled write, its copy's answer, the reread and the
    /// write take 1.5 s and three usual answers, within 4 s while those
    /// take under 830 ms. A copy refused while the first is out waits up to
    /// another 1.5 s for the first's answer, the round's success if the
    /// first landed; if it never answers, the reread and the write have
    /// what is left of the 4 s, about 1 s.
    pub renew_attempt_ms: u64,
    /// A listing is a LIST, a request for each page past the first, and
    /// the records' reads side by side. One stalled request among them
    /// takes 1 s and its copy's answer, within the listing's 2 s. Two in a
    /// row, a stalled LIST and then a stalled read or page, take 2 s and
    /// more: the listing times out, which only delays routing to a new
    /// peer until the next.
    pub list_attempt_ms: u64,
    /// How long a renewal waits to write again when its guard failed on a
    /// version that is still current: a write of ours to it is in
    /// progress. R2 refuses more than about one write a second to one key.
    pub contended_pause_ms: u64,
    /// Peers judge this node's lease by their own clocks; one off from the
    /// bucket's clock by more than this exits.
    pub max_skew_ms: u64,
    /// A node whose event loop makes no progress for this long fences
    /// (`watchdog`). The lease is kept on a thread of its own, so a turn of
    /// the loop longer than a lease does not lose it; a loop stuck for good
    /// would keep it, and the node's objects unreachable, for good. Three
    /// leases leaves room for turns longer than a lease, which the lease
    /// thread rides out, and is short enough that peers take a stuck node's
    /// objects over within a few leases more.
    pub stall_ms: u64,
}

impl Timing {
    pub const DEFAULT: Self = Self {
        lease_ms: 20_000,
        interval_ms: 3_000,
        list_interval_ms: 3_000,
        renew_timeout_ms: 4_000,
        list_timeout_ms: 2_000,
        retry_ms: 1_000,
        renew_attempt_ms: 1_500,
        list_attempt_ms: 1_000,
        contended_pause_ms: 1_000,
        max_skew_ms: 2_000,
        stall_ms: 60_000,
    };

    /// `DEFAULT` with every time divided by `divisor`: for tests, which
    /// would otherwise wait out a full lease. `None` if `divisor` is 0 or
    /// rounds a time down to 0. A bucket that stalls for longer than the
    /// shortened bounds fences every node.
    #[must_use]
    pub fn divided(divisor: u64) -> Option<Self> {
        let t = Self::DEFAULT;
        let divided = Self {
            lease_ms: t.lease_ms.checked_div(divisor)?,
            interval_ms: t.interval_ms / divisor,
            list_interval_ms: t.list_interval_ms / divisor,
            renew_timeout_ms: t.renew_timeout_ms / divisor,
            list_timeout_ms: t.list_timeout_ms / divisor,
            retry_ms: t.retry_ms / divisor,
            renew_attempt_ms: t.renew_attempt_ms / divisor,
            list_attempt_ms: t.list_attempt_ms / divisor,
            contended_pause_ms: t.contended_pause_ms / divisor,
            max_skew_ms: t.max_skew_ms / divisor,
            stall_ms: t.stall_ms / divisor,
        };
        (!divided.times().contains(&0)).then_some(divided)
    }

    /// `DEFAULT` with the lease, the rounds that renew it and the rounds
    /// that list the peers set from the cluster's config, for a bucket that
    /// bills each request: a longer lease renewed less often rides out
    /// longer bucket trouble, and a crashed node's objects wait longer for a
    /// peer to take them over. The bounds on a round are not changed. The
    /// watchdog waits out at least three leases (`stall_ms`).
    ///
    /// Refused, saying why, unless a lease spans at least three renewals,
    /// a round and the margin it fences with fit in it (`before_renew`),
    /// and a peer's record, renewed a round before a listing, still holds
    /// at the next listing's end, so that no live peer drops out of the
    /// listed ones between listings.
    pub fn configured(
        lease_ms: u64,
        interval_ms: u64,
        list_interval_ms: u64,
    ) -> Result<Self, String> {
        let t = Self {
            lease_ms,
            interval_ms,
            list_interval_ms,
            stall_ms: Self::DEFAULT.stall_ms.max(lease_ms.saturating_mul(3)),
            ..Self::DEFAULT
        };
        let within = |what: &str, ms: u64, (min, max): (u64, u64)| {
            if (min..=max).contains(&ms) {
                Ok(())
            } else {
                Err(format!("{what} {ms} is not within {min} to {max}"))
            }
        };
        within("leaseMs", lease_ms, CONFIGURED_LEASE_MS)?;
        within("renewIntervalMs", interval_ms, CONFIGURED_INTERVAL_MS)?;
        within("listIntervalMs", list_interval_ms, CONFIGURED_INTERVAL_MS)?;
        if interval_ms.saturating_mul(3) > lease_ms {
            return Err(format!(
                "leaseMs {lease_ms} is less than three renewIntervalMs ({interval_ms})"
            ));
        }
        let round = interval_ms + t.renew_timeout_ms + t.safety_ms();
        if round >= lease_ms {
            return Err(format!(
                "leaseMs {lease_ms} is not more than renewIntervalMs {interval_ms}, a renewal's \
                 bound ({} ms) and the margin a node fences with ({} ms)",
                t.renew_timeout_ms,
                t.safety_ms()
            ));
        }
        if interval_ms + list_interval_ms + t.list_timeout_ms >= lease_ms {
            return Err(format!(
                "leaseMs {lease_ms} is not more than renewIntervalMs {interval_ms}, \
                 listIntervalMs {list_interval_ms} and a listing's bound ({} ms)",
                t.list_timeout_ms
            ));
        }
        Ok(t)
    }

    const fn times(self) -> [u64; 11] {
        [
            self.lease_ms,
            self.interval_ms,
            self.list_interval_ms,
            self.renew_timeout_ms,
            self.list_timeout_ms,
            self.retry_ms,
            self.renew_attempt_ms,
            self.list_attempt_ms,
            self.contended_pause_ms,
            self.max_skew_ms,
            self.stall_ms,
        ]
    }

    /// Peers judge a lease by their own clocks, each within `max_skew_ms`
    /// of the bucket's, so the owner stops serving this long before it
    /// expires.
    const fn safety_ms(&self) -> u64 {
        2 * self.max_skew_ms
    }

    /// Whether a lease that expires at `until_ms` holds at `now_ms`, with
    /// peers' clocks allowed for.
    #[must_use]
    pub const fn holds(&self, until_ms: u64, now_ms: u64) -> bool {
        now_ms + self.safety_ms() < until_ms
    }

    /// How much longer than `now_ms` a lease that expires at `until_ms`
    /// holds (`holds`), in ms: 0 once it does not.
    #[must_use]
    pub const fn remaining_ms(&self, until_ms: u64, now_ms: u64) -> u64 {
        until_ms.saturating_sub(now_ms + self.safety_ms())
    }

    /// When peers, by clocks up to `safety_ms` ahead, may judge a lease that
    /// expires at `until_ms` expired: it `holds` before.
    #[must_use]
    pub const fn holds_until(&self, until_ms: u64) -> u64 {
        until_ms.saturating_sub(self.safety_ms())
    }
}

/// The leases `Timing::configured` accepts, in ms: from half the default,
/// which still rides out a few seconds of bucket trouble, to two minutes.
/// A node fences `safety_ms` before its lease expires, which covers clocks
/// that drift apart by up to about 2% of a lease while it runs (the 1%
/// either way that `cache-test.rs` simulates) only up to about 3 minutes.
pub const CONFIGURED_LEASE_MS: (u64, u64) = (10_000, 120_000);
/// The renewal and listing intervals `Timing::configured` accepts, in ms.
pub const CONFIGURED_INTERVAL_MS: (u64, u64) = (1_000, 60_000);

impl Default for Timing {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// What the executor observed before a round's renewal.
pub struct Observed {
    pub now_ms: u64,
    /// This node's clock less the bucket's.
    pub skew_ms: i64,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Step {
    /// Renew now.
    Renew,
    /// Stop serving: the process exits before peers may take over.
    Fence(String),
    /// Try again after `retry_ms`; the lease still holds.
    Retry(String),
}

#[derive(Default)]
pub struct Lease {
    pub timing: Timing,
    /// The lease expires at this time; 0 before the first renewal.
    until_ms: u64,
    /// The instance left the cluster: it renews no more.
    pub left: bool,
}

impl Lease {
    #[must_use]
    pub fn new(timing: Timing) -> Self {
        Self {
            timing,
            ..Self::default()
        }
    }

    pub fn before_renew(&self, o: &Observed) -> Step {
        if o.skew_ms.unsigned_abs() > self.timing.max_skew_ms {
            return Step::Fence(format!("clock off from the bucket's by {} ms", o.skew_ms));
        }
        // A node stalled past its lease (a paused process, a lease thread
        // that got no time) may have been taken over: it must not renew
        // and serve on.
        if self.until_ms != 0 && !self.holds(o.now_ms + self.timing.renew_timeout_ms) {
            return Step::Fence("the lease lapsed before it could be renewed".into());
        }
        Step::Renew
    }

    /// The renewal's outcome: the new expiry, or why it failed. `Renew`
    /// here means the round succeeded.
    pub fn renewed(&mut self, now_ms: u64, result: Result<u64, String>) -> Step {
        match result {
            Ok(until_ms) => {
                self.until_ms = until_ms;
                Step::Renew
            }
            // Fence while the lease still holds, so the node is gone before
            // a peer may take over.
            Err(e) if !self.holds(now_ms + self.timing.renew_timeout_ms) => Step::Fence(e),
            Err(e) => Step::Retry(e),
        }
    }

    /// Whether the lease holds at `now_ms`, with peers' clocks allowed
    /// for: an actor serves only while it does.
    pub fn holds(&self, now_ms: u64) -> bool {
        self.timing.holds(self.until_ms, now_ms)
    }

    /// When peers, by clocks up to `safety_ms` ahead, may judge this lease expired: it
    /// [`Lease::holds`] before. 0 once it left.
    #[must_use]
    pub fn holds_until(&self) -> u64 {
        if self.left {
            return 0;
        }
        self.timing.holds_until(self.until_ms)
    }
}

#[cfg(test)]
#[path = "lease-test.rs"]
mod tests;
