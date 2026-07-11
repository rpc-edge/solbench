//! Slot-freshness ("slot-lag") estimation, corrected for network round-trip.
//!
//! # Why a round-trip correction is needed
//!
//! An RPC server reads its slot when it *processes* a request — at roughly
//! `send + one_way_delay` — not at the moment the client sent it. So a distant
//! endpoint reads the chain *later in wall-clock time* than a co-located one and,
//! purely because of that head start, reports a **higher** slot.
//!
//! Comparing slots that were merely *sent* on the same tick therefore rewards
//! distance and penalises proximity: at Solana's ~400ms slot time, an endpoint
//! 600ms away reads the chain ~300ms (~0.75 slots) later than one 4ms away, and
//! wins a "freshness" comparison it did nothing to earn. That is backwards for a
//! benchmark whose purpose is to surface co-located infrastructure.
//!
//! This estimator timestamps every observation at the moment the *server* is
//! estimated to have read the chain (`send + rtt/2`, see [`observed_at_ns`]) and
//! scores each endpoint against the best chain state anyone had observed **by
//! that moment**.
//!
//! # What this number does and does not mean
//!
//! - Lag is **relative to the best-informed endpoint in the run**, never absolute
//!   chain truth. A single endpoint always measures 0: there is nothing to be
//!   behind.
//! - `rtt/2` assumes a symmetric path and folds server processing time into the
//!   network estimate. It is an *estimate* of the read moment, not a measurement.
//! - The reference is only as fresh as the most recent observation at or before
//!   `observed_at_ns`, so lag is a **conservative lower bound** on true staleness:
//!   it under-reports rather than over-reports. Shorter `--interval-ms` (or more
//!   endpoints) tightens it.

use serde::Serialize;

/// One endpoint's slot reading, timestamped at its estimated *server-read* moment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlotObservation {
    /// Index of the endpoint this reading came from.
    pub endpoint: usize,
    /// Monotonic nanosecond offset, from a run-wide origin shared by every
    /// endpoint, at which the server is estimated to have read the chain.
    /// Build it with [`observed_at_ns`].
    pub observed_at_ns: u64,
    /// The slot the endpoint reported.
    pub slot: u64,
}

/// Estimated moment the server read the chain: the request's send time plus the
/// one-way delay, approximated as half the round-trip.
///
/// `send_offset_ns` is measured from the run-wide origin shared by all endpoints;
/// mixing origins across endpoints would make the comparison meaningless.
pub fn observed_at_ns(send_offset_ns: u64, rtt_ns: u64) -> u64 {
    send_offset_ns.saturating_add(rtt_ns / 2)
}

/// How far behind the best-informed endpoint a given endpoint ran.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct SlotLagSummary {
    /// Mean slots behind the best chain state known at each read moment
    /// (`0.0` == never behind).
    pub avg: f64,
    /// Worst-case slots behind at any single reading.
    pub max: u64,
    /// Readings this summary is built from.
    pub ticks: usize,
}

/// Accumulates [`SlotObservation`]s across endpoints and scores each one against
/// the chain state that was demonstrably known at the moment it read.
#[derive(Debug, Default, Clone)]
pub struct SlotLagEstimator {
    obs: Vec<SlotObservation>,
}

impl SlotLagEstimator {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            obs: Vec::with_capacity(cap),
        }
    }

    /// Record one endpoint's reading. `observed_at_ns` should come from
    /// [`observed_at_ns`].
    pub fn observe(&mut self, endpoint: usize, observed_at_ns: u64, slot: u64) {
        self.obs.push(SlotObservation {
            endpoint,
            observed_at_ns,
            slot,
        });
    }

    pub fn len(&self) -> usize {
        self.obs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.obs.is_empty()
    }

    /// Per-endpoint lag, indexed by `SlotObservation::endpoint`; `None` for an
    /// endpoint that produced no readings. `endpoints` is the total endpoint count.
    ///
    /// The reference is the running maximum slot over observations ordered by read
    /// moment — the highest slot the chain is *known* to have reached by time `t`.
    /// Slots advance monotonically, so this is a sound lower bound on the true
    /// chain state, and it is built without assuming any slot rate.
    pub fn summaries(&self, endpoints: usize) -> Vec<Option<SlotLagSummary>> {
        let mut order: Vec<&SlotObservation> = self.obs.iter().collect();
        // Ascending by read moment. On an exact tie, the *higher* slot sorts first
        // so it raises the reference before the lower reading is scored against it:
        // two endpoints reading the same instant, one a slot behind, must show that.
        order.sort_by(|a, b| {
            a.observed_at_ns
                .cmp(&b.observed_at_ns)
                .then(b.slot.cmp(&a.slot))
        });

        // (sum_lag, max_lag, count) per endpoint.
        let mut acc: Vec<Option<(u64, u64, usize)>> = vec![None; endpoints];
        let mut known_slot = 0u64;
        for o in order {
            known_slot = known_slot.max(o.slot);
            // Sound by construction: the running max includes this reading itself.
            let lag = known_slot - o.slot;
            if let Some(slot) = acc.get_mut(o.endpoint) {
                let e = slot.get_or_insert((0, 0, 0));
                e.0 += lag;
                e.1 = e.1.max(lag);
                e.2 += 1;
            }
        }

        acc.into_iter()
            .map(|a| {
                a.map(|(sum, max, ticks)| SlotLagSummary {
                    avg: sum as f64 / ticks as f64,
                    max,
                    ticks,
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SLOT_MS: u64 = 400;
    const MS: u64 = 1_000_000;

    /// Slot of a chain that ticks every 400ms, read at `t_ms`.
    fn chain_slot(base: u64, t_ms: u64) -> u64 {
        base + t_ms / SLOT_MS
    }

    /// Simulate a run: every endpoint reads the SAME chain, honestly, at
    /// `send + rtt/2`. Only network distance differs, so a fair metric must
    /// report zero lag for all of them.
    fn equally_fresh_run(rtts_ms: &[u64], ticks: usize, interval_ms: u64) -> Vec<SlotLagSummary> {
        let base = 400_000_000u64;
        let mut est = SlotLagEstimator::new();
        for tick in 0..ticks {
            let send_ms = tick as u64 * interval_ms;
            for (ep, &rtt_ms) in rtts_ms.iter().enumerate() {
                let read_ms = send_ms + rtt_ms / 2;
                est.observe(
                    ep,
                    observed_at_ns(send_ms * MS, rtt_ms * MS),
                    chain_slot(base, read_ms),
                );
            }
        }
        est.summaries(rtts_ms.len())
            .into_iter()
            .map(|s| s.expect("every endpoint observed"))
            .collect()
    }

    #[test]
    fn distance_alone_is_not_lag() {
        // Co-located (4ms) vs distant (600ms), same chain, equal freshness.
        // The naive "compare slots sent on the same tick" metric reports the
        // co-located endpoint ~0.75 slots behind. This one must report zero.
        let s = equally_fresh_run(&[4, 600], 12, 250);
        assert_eq!(s[0].avg, 0.0, "co-located endpoint must not be penalised");
        assert_eq!(s[1].avg, 0.0, "distant endpoint must not be rewarded");
        assert_eq!(s[0].max, 0);
        assert_eq!(s[1].max, 0);
    }

    #[test]
    fn a_wide_rtt_spread_still_measures_zero() {
        let s = equally_fresh_run(&[2, 40, 250, 900], 20, 150);
        for (ep, summary) in s.iter().enumerate() {
            assert_eq!(summary.avg, 0.0, "endpoint {ep} is equally fresh");
        }
    }

    #[test]
    fn a_genuinely_stale_endpoint_is_caught() {
        // Endpoint 1 is co-located but its node trails the chain by 3 slots.
        let base = 400_000_000u64;
        let (ticks, interval_ms) = (16usize, 100u64);
        let mut est = SlotLagEstimator::new();
        for tick in 0..ticks {
            let send_ms = tick as u64 * interval_ms;
            // 0: healthy, 4ms away.
            est.observe(
                0,
                observed_at_ns(send_ms * MS, 4 * MS),
                chain_slot(base, send_ms + 2),
            );
            // 1: same distance, but 3 slots behind the chain.
            est.observe(
                1,
                observed_at_ns(send_ms * MS, 4 * MS),
                chain_slot(base, send_ms + 2) - 3,
            );
        }
        let s = est.summaries(2);
        let healthy = s[0].expect("healthy observed");
        let stale = s[1].expect("stale observed");
        assert_eq!(healthy.avg, 0.0, "healthy endpoint leads");
        assert!(
            stale.avg >= 3.0,
            "a 3-slot-behind node must read at least 3 slots behind, got {}",
            stale.avg
        );
        assert!(stale.max >= 3);
    }

    #[test]
    fn staleness_is_not_masked_by_being_far_away() {
        // The pointed case: a distant endpoint (600ms) whose node is ALSO 2 slots
        // behind. Its distance buys it ~0.75 slots of free chain advance; the
        // correction must strip that out and still surface real staleness.
        let base = 400_000_000u64;
        let mut est = SlotLagEstimator::new();
        for tick in 0..20u64 {
            let send_ms = tick * 100;
            est.observe(
                0,
                observed_at_ns(send_ms * MS, 4 * MS),
                chain_slot(base, send_ms + 2),
            );
            est.observe(
                1,
                observed_at_ns(send_ms * MS, 600 * MS),
                chain_slot(base, send_ms + 300) - 2,
            );
        }
        let s = est.summaries(2);
        assert_eq!(s[0].expect("healthy").avg, 0.0);
        assert!(
            s[1].expect("far+stale").avg >= 1.0,
            "distance must not launder staleness"
        );
    }

    #[test]
    fn same_instant_tie_scores_the_lower_slot_as_behind() {
        let mut est = SlotLagEstimator::new();
        est.observe(0, 1_000, 500);
        est.observe(1, 1_000, 499); // identical read moment, one slot behind
        let s = est.summaries(2);
        assert_eq!(s[0].expect("leader").avg, 0.0);
        assert_eq!(s[1].expect("trailer").avg, 1.0);
    }

    #[test]
    fn a_lone_endpoint_has_nothing_to_be_behind() {
        let mut est = SlotLagEstimator::new();
        est.observe(0, 0, 100);
        est.observe(0, 400 * MS, 101);
        let s = est.summaries(1);
        assert_eq!(s[0].expect("observed").avg, 0.0);
        assert_eq!(s[0].expect("observed").ticks, 2);
    }

    #[test]
    fn silent_endpoints_summarise_to_none() {
        let mut est = SlotLagEstimator::new();
        est.observe(1, 0, 42);
        let s = est.summaries(3);
        assert!(s[0].is_none(), "endpoint 0 never replied");
        assert!(s[1].is_some());
        assert!(s[2].is_none(), "endpoint 2 never replied");
    }

    #[test]
    fn empty_estimator_yields_no_summaries() {
        let est = SlotLagEstimator::new();
        assert!(est.is_empty());
        assert_eq!(est.summaries(2), vec![None, None]);
    }

    #[test]
    fn observed_at_is_send_plus_one_way_delay() {
        assert_eq!(observed_at_ns(1_000, 600), 1_300);
        assert_eq!(observed_at_ns(0, 0), 0);
        assert_eq!(observed_at_ns(u64::MAX, 2), u64::MAX, "saturates");
    }
}
