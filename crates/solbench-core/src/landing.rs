//! Transaction landing-rate and landed-slot tracking under load.

use serde::Serialize;

/// Distribution of landed-slot deltas (`landed_slot - target_slot`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SlotDeltaSummary {
    pub count: usize,
    pub min: i64,
    pub max: i64,
    pub mean: i64,
    pub median: i64,
}

/// Tracks transaction landing outcomes: how many attempts landed, how many
/// failed, and, for those that landed, how far off the intended slot they were.
#[derive(Debug, Default, Clone, Serialize)]
pub struct LandingTracker {
    pub attempts: u64,
    pub landed: u64,
    pub failed: u64,
    #[serde(skip)]
    slot_deltas: Vec<i64>,
}

impl LandingTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a landed transaction. `slot_delta` is `landed_slot - target_slot`
    /// (`0` == landed in the intended slot; positive == landed late).
    pub fn record_landed(&mut self, slot_delta: i64) {
        self.attempts += 1;
        self.landed += 1;
        self.slot_deltas.push(slot_delta);
    }

    /// Record a transaction that never landed (dropped, expired, or rejected).
    pub fn record_failed(&mut self) {
        self.attempts += 1;
        self.failed += 1;
    }

    /// Landed / attempts, in `[0.0, 1.0]`. Returns `0.0` when there were no attempts.
    pub fn landing_rate(&self) -> f64 {
        if self.attempts == 0 {
            0.0
        } else {
            self.landed as f64 / self.attempts as f64
        }
    }

    /// Distribution of landed-slot deltas, or `None` if nothing landed.
    pub fn slot_delta_summary(&self) -> Option<SlotDeltaSummary> {
        if self.slot_deltas.is_empty() {
            return None;
        }
        let mut sorted = self.slot_deltas.clone();
        sorted.sort_unstable();
        let count = sorted.len();
        let sum: i64 = sorted.iter().sum();
        Some(SlotDeltaSummary {
            count,
            min: sorted[0],
            max: sorted[count - 1],
            mean: sum / count as i64,
            median: sorted[count / 2],
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rate_and_slot_deltas() {
        let mut t = LandingTracker::new();
        t.record_landed(0);
        t.record_landed(1);
        t.record_landed(2);
        t.record_failed();

        assert_eq!(t.attempts, 4);
        assert_eq!(t.landed, 3);
        assert_eq!(t.failed, 1);
        assert!((t.landing_rate() - 0.75).abs() < f64::EPSILON);

        let s = t.slot_delta_summary().unwrap();
        assert_eq!(
            s,
            SlotDeltaSummary {
                count: 3,
                min: 0,
                max: 2,
                mean: 1,
                median: 1
            }
        );
    }

    #[test]
    fn empty_tracker_is_zero_and_none() {
        let t = LandingTracker::new();
        assert_eq!(t.landing_rate(), 0.0);
        assert!(t.slot_delta_summary().is_none());
    }
}
