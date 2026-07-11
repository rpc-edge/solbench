//! Latency sample collection and exact-percentile summaries.

use serde::Serialize;
use std::time::Duration;

/// Exact-percentile summary of a batch of latency samples, in nanoseconds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LatencySummary {
    pub count: usize,
    pub min_ns: u64,
    pub max_ns: u64,
    pub mean_ns: u64,
    /// Population standard deviation of the samples ("jitter"/consistency). A high
    /// value relative to the median means an inconsistent endpoint - which for
    /// trading matters as much as a low median.
    pub stddev_ns: u64,
    pub p50_ns: u64,
    pub p90_ns: u64,
    pub p95_ns: u64,
    pub p99_ns: u64,
    pub p999_ns: u64,
}

/// Nearest-rank percentile over an ascending-sorted, non-empty slice.
/// `q` is a quantile in `[0.0, 1.0]`.
fn percentile_ns(sorted: &[u64], q: f64) -> u64 {
    debug_assert!(
        !sorted.is_empty(),
        "percentile_ns requires a non-empty slice"
    );
    let n = sorted.len();
    if q <= 0.0 {
        return sorted[0];
    }
    // Nearest-rank: rank = ceil(q * n), 1-indexed.
    let rank = (q * n as f64).ceil() as usize;
    let idx = rank.saturating_sub(1).min(n - 1);
    sorted[idx]
}

/// Collects latency samples and produces a [`LatencySummary`].
///
/// Cheap to feed one sample at a time on a hot path; sorting/percentiles happen
/// once, when you ask for the [`summary`](LatencyRecorder::summary).
#[derive(Debug, Default, Clone)]
pub struct LatencyRecorder {
    samples_ns: Vec<u64>,
}

impl LatencyRecorder {
    pub fn new() -> Self {
        Self {
            samples_ns: Vec::new(),
        }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            samples_ns: Vec::with_capacity(cap),
        }
    }

    /// Record a sample from a [`Duration`] (saturates at `u64::MAX` nanoseconds).
    pub fn record(&mut self, d: Duration) {
        self.record_ns(d.as_nanos().min(u64::MAX as u128) as u64);
    }

    /// Record a raw nanosecond sample.
    pub fn record_ns(&mut self, ns: u64) {
        self.samples_ns.push(ns);
    }

    pub fn len(&self) -> usize {
        self.samples_ns.len()
    }

    pub fn is_empty(&self) -> bool {
        self.samples_ns.is_empty()
    }

    /// Compute the summary. Returns `None` when no samples were recorded.
    pub fn summary(&self) -> Option<LatencySummary> {
        if self.samples_ns.is_empty() {
            return None;
        }
        let mut sorted = self.samples_ns.clone();
        sorted.sort_unstable();
        let count = sorted.len();
        let sum: u128 = sorted.iter().map(|&x| x as u128).sum();
        let mean_f = sum as f64 / count as f64;
        let mean_ns = mean_f as u64;
        let variance = sorted
            .iter()
            .map(|&x| {
                let d = x as f64 - mean_f;
                d * d
            })
            .sum::<f64>()
            / count as f64;
        let stddev_ns = variance.sqrt() as u64;
        Some(LatencySummary {
            count,
            min_ns: sorted[0],
            max_ns: sorted[count - 1],
            mean_ns,
            stddev_ns,
            p50_ns: percentile_ns(&sorted, 0.50),
            p90_ns: percentile_ns(&sorted, 0.90),
            p95_ns: percentile_ns(&sorted, 0.95),
            p99_ns: percentile_ns(&sorted, 0.99),
            p999_ns: percentile_ns(&sorted, 0.999),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_recorder_has_no_summary() {
        assert!(LatencyRecorder::new().summary().is_none());
    }

    #[test]
    fn summary_over_1_to_100() {
        let mut r = LatencyRecorder::new();
        for ns in 1..=100u64 {
            r.record_ns(ns);
        }
        let s = r.summary().unwrap();
        assert_eq!(s.count, 100);
        assert_eq!(s.min_ns, 1);
        assert_eq!(s.max_ns, 100);
        assert_eq!(s.mean_ns, 50); // 5050 / 100 == 50 (integer)
        assert_eq!(s.stddev_ns, 28); // population stddev of 1..=100 ≈ 28.87
        assert_eq!(s.p50_ns, 50);
        assert_eq!(s.p90_ns, 90);
        assert_eq!(s.p95_ns, 95);
        assert_eq!(s.p99_ns, 99);
        assert_eq!(s.p999_ns, 100);
    }

    #[test]
    fn single_sample_is_every_percentile() {
        let mut r = LatencyRecorder::new();
        r.record(Duration::from_nanos(42));
        let s = r.summary().unwrap();
        assert_eq!(s.count, 1);
        assert_eq!(s.min_ns, 42);
        assert_eq!(s.max_ns, 42);
        assert_eq!(s.stddev_ns, 0);
        assert_eq!(s.p50_ns, 42);
        assert_eq!(s.p999_ns, 42);
    }
}
