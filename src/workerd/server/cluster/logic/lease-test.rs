// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

use super::*;

/// 1 for production's times; the others as tests divide them.
const DIVISORS: [u64; 4] = [1, 2, 4, 10];

fn timings() -> [Timing; 4] {
    DIVISORS.map(|d| Timing::divided(d).unwrap())
}

fn seen(now_ms: u64) -> Observed {
    Observed { now_ms, skew_ms: 0 }
}

#[test]
fn divided_times_keep_their_proportions() {
    let t = Timing::DEFAULT;
    for (d, divided) in DIVISORS.into_iter().zip(timings()) {
        assert_eq!(divided.times().map(|ms| ms * d), t.times(), "{divided:?}");
        // A round renews before the lease needs to fence.
        let round = divided.interval_ms + divided.renew_timeout_ms;
        assert!(
            round + divided.safety_ms() < divided.lease_ms,
            "{divided:?}"
        );
        // A turn of the loop that outlasts the lease keeps it, and the
        // node: the watchdog waits out a longer stall than that.
        assert!(divided.stall_ms >= 3 * divided.lease_ms, "{divided:?}");
        // An idle loop beats once a round, which the keeper reads once a
        // round: either late by a round leaves it far from a stall.
        assert!(4 * divided.interval_ms < divided.stall_ms, "{divided:?}");
    }
}

#[test]
fn every_divided_timing_sends_a_stalled_request_again_within_its_round() {
    // Every divisor up to the shortest time, which rounds it to 1 ms; none
    // past it (`a_divisor_that_rounds_a_time_to_zero_is_refused`).
    let shortest = Timing::DEFAULT.times().into_iter().min().unwrap();
    for t in (1..=shortest).map(|d| Timing::divided(d).unwrap()) {
        assert!(t.renew_attempt_ms < t.renew_timeout_ms, "{t:?}");
        assert!(t.list_attempt_ms < t.list_timeout_ms, "{t:?}");
    }
}

#[test]
fn a_divisor_that_rounds_a_time_to_zero_is_refused() {
    let shortest = Timing::DEFAULT.times().into_iter().min().unwrap();
    assert_eq!(Timing::divided(0), None);
    assert!(Timing::divided(shortest).is_some());
    assert_eq!(Timing::divided(shortest + 1), None);
}

#[test]
fn renews_and_holds_until_the_safety_margin() {
    for t in timings() {
        let mut lease = Lease::new(t);
        assert_eq!(lease.before_renew(&seen(0)), Step::Renew);
        assert_eq!(lease.renewed(0, Ok(t.lease_ms)), Step::Renew);
        let last = t.lease_ms - t.safety_ms();
        assert!(lease.holds(last - 1) && !lease.holds(last), "{t:?}");
    }
}

#[test]
fn a_failed_renewal_retries_until_the_lease_nearly_lapses() {
    for t in timings() {
        let mut lease = Lease::new(t);
        lease.renewed(0, Ok(t.lease_ms));
        assert!(matches!(
            lease.renewed(t.lease_ms / 4, Err("x".into())),
            Step::Retry(_)
        ));
        let last = t.lease_ms - t.safety_ms() - t.renew_timeout_ms;
        assert!(matches!(
            lease.renewed(last - 1, Err("x".into())),
            Step::Retry(_)
        ));
        assert!(matches!(
            lease.renewed(last, Err("x".into())),
            Step::Fence(_)
        ));
    }
}

#[test]
fn a_node_stalled_past_its_lease_fences_instead_of_renewing() {
    for t in timings() {
        let mut lease = Lease::new(t);
        lease.renewed(0, Ok(t.lease_ms));
        assert_eq!(lease.before_renew(&seen(t.interval_ms)), Step::Renew);
        let step = lease.before_renew(&seen(t.lease_ms + 1));
        assert!(matches!(step, Step::Fence(_)));
    }
}

#[test]
fn skew_fences_at_once() {
    for t in timings() {
        let lease = Lease::new(t);
        let max = i64::try_from(t.max_skew_ms).unwrap();
        let skewed = |skew_ms| lease.before_renew(&Observed { skew_ms, ..seen(0) });
        assert_eq!(skewed(-max), Step::Renew);
        assert!(matches!(skewed(max + 1), Step::Fence(_)));
        assert!(matches!(skewed(-max - 1), Step::Fence(_)));
    }
}

#[test]
fn a_lease_holds_for_as_long_as_it_has_time_remaining() {
    for t in timings() {
        let until = 1_000_000;
        for now in [
            0,
            until - t.safety_ms() - 1,
            until - t.safety_ms(),
            until,
            2 * until,
        ] {
            let remaining = t.remaining_ms(until, now);
            assert_eq!(t.holds(until, now), remaining > 0, "{t:?} at {now}");
            if remaining > 0 {
                assert!(t.holds(until, now + remaining - 1), "{t:?} at {now}");
                assert!(!t.holds(until, now + remaining), "{t:?} at {now}");
            }
        }
    }
}

/// Leases a config may set: the default's, the shortest and the longest
/// that the bounds allow, and some between.
const CONFIGURED: [(u64, u64, u64); 6] = [
    (20_000, 3_000, 3_000),
    (60_000, 10_000, 30_000),
    (60_000, 20_000, 20_000),
    (30_000, 5_000, 10_000),
    (10_000, 1_000, 1_000),
    (120_000, 40_000, 60_000),
];

#[test]
fn the_default_lease_configured_is_the_default() {
    assert_eq!(
        Timing::configured(20_000, 3_000, 3_000),
        Ok(Timing::DEFAULT)
    );
}

#[test]
fn a_configured_lease_keeps_the_defaults_proportions() {
    for (lease_ms, interval_ms, list_interval_ms) in CONFIGURED {
        let t = Timing::configured(lease_ms, interval_ms, list_interval_ms).unwrap();
        assert_eq!(
            (t.lease_ms, t.interval_ms, t.list_interval_ms),
            (lease_ms, interval_ms, list_interval_ms)
        );
        // What `divided_times_keep_their_proportions` checks.
        let round = t.interval_ms + t.renew_timeout_ms;
        assert!(round + t.safety_ms() < t.lease_ms, "{t:?}");
        assert!(t.stall_ms >= 3 * t.lease_ms, "{t:?}");
        assert!(4 * t.interval_ms < t.stall_ms, "{t:?}");
        // A live peer, renewed a round before a listing, is still live in
        // that listing when the next one returns.
        let next = t.list_interval_ms + t.list_timeout_ms;
        assert!(t.interval_ms + next < t.lease_ms, "{t:?}");
        // The bounds on a round are the default's.
        let d = Timing::DEFAULT;
        assert_eq!(
            (
                t.renew_timeout_ms,
                t.list_timeout_ms,
                t.retry_ms,
                t.max_skew_ms
            ),
            (
                d.renew_timeout_ms,
                d.list_timeout_ms,
                d.retry_ms,
                d.max_skew_ms
            )
        );
    }
}

#[test]
fn a_configured_lease_renews_retries_and_fences_as_the_default() {
    for (lease_ms, interval_ms, list_interval_ms) in CONFIGURED {
        let t = Timing::configured(lease_ms, interval_ms, list_interval_ms).unwrap();
        let mut lease = Lease::new(t);
        assert_eq!(lease.renewed(0, Ok(t.lease_ms)), Step::Renew);
        // A failed renewal a round later is retried, not fenced.
        assert!(matches!(
            lease.renewed(t.interval_ms, Err("x".into())),
            Step::Retry(_)
        ));
        let last = t.lease_ms - t.safety_ms() - t.renew_timeout_ms;
        assert!(matches!(
            lease.renewed(last, Err("x".into())),
            Step::Fence(_)
        ));
        // A round's renewal that starts on time finds the lease holding.
        assert_eq!(lease.before_renew(&seen(t.interval_ms)), Step::Renew);
    }
}

#[test]
fn a_configured_lease_out_of_bounds_is_refused() {
    let refused = |lease_ms, interval_ms, list_interval_ms, why: &str| {
        let e = Timing::configured(lease_ms, interval_ms, list_interval_ms).unwrap_err();
        assert!(
            e.contains(why),
            "{lease_ms}/{interval_ms}/{list_interval_ms}: {e}"
        );
    };
    refused(9_999, 1_000, 1_000, "leaseMs 9999 is not within");
    refused(120_001, 3_000, 3_000, "leaseMs 120001 is not within");
    refused(20_000, 999, 3_000, "renewIntervalMs 999 is not within");
    refused(20_000, 0, 3_000, "renewIntervalMs 0 is not within");
    refused(20_000, 3_000, 0, "listIntervalMs 0 is not within");
    refused(
        120_000,
        60_001,
        3_000,
        "renewIntervalMs 60001 is not within",
    );
    refused(120_000, 3_000, 60_001, "listIntervalMs 60001 is not within");
    // Fewer than three renewals in a lease.
    refused(20_000, 7_000, 3_000, "less than three renewIntervalMs");
    // Three renewals, but a round and the margin don't fit.
    refused(12_000, 4_000, 1_000, "a renewal's bound");
    // A listing too far from the next.
    refused(60_000, 10_000, 48_000, "a listing's bound");
    assert!(Timing::configured(60_000, 10_000, 47_999).is_ok());
}
