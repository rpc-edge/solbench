//! Per-operation event timelines for hot-path latency measurement.

use std::time::{Duration, Instant};

/// Records monotonic timestamps for the named stages of a single operation's
/// hot path (e.g. `first_seen` -> `decided` -> `submitted` -> `acked` -> `landed`)
/// and computes the deltas between them.
///
/// Used by solbench probes and by downstream latency harnesses: the CLOB
/// market-maker instruments each quote/cancel cycle with one of these to report
/// quote-to-ack and cancel latency.
#[derive(Debug, Clone)]
pub struct EventTimeline {
    start: Instant,
    marks: Vec<(String, Instant)>,
}

impl EventTimeline {
    /// Begin a timeline at the current monotonic instant.
    pub fn start() -> Self {
        Self {
            start: Instant::now(),
            marks: Vec::new(),
        }
    }

    /// Record the current instant under `stage`.
    pub fn mark(&mut self, stage: impl Into<String>) {
        self.marks.push((stage.into(), Instant::now()));
    }

    /// Total time since the timeline began.
    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }

    fn instant_of(&self, stage: &str) -> Option<Instant> {
        self.marks
            .iter()
            .find(|(name, _)| name == stage)
            .map(|(_, t)| *t)
    }

    /// Duration from timeline start to when `stage` was marked.
    pub fn since_start(&self, stage: &str) -> Option<Duration> {
        self.instant_of(stage).map(|t| t.duration_since(self.start))
    }

    /// Duration between two marked stages. `None` if either stage is missing or
    /// `to` was marked before `from`.
    pub fn between(&self, from: &str, to: &str) -> Option<Duration> {
        let a = self.instant_of(from)?;
        let b = self.instant_of(to)?;
        b.checked_duration_since(a)
    }

    /// The recorded stage names, in the order they were marked.
    pub fn stages(&self) -> impl Iterator<Item = &str> {
        self.marks.iter().map(|(name, _)| name.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn marks_deltas_and_ordering() {
        let mut tl = EventTimeline::start();
        tl.mark("first_seen");
        tl.mark("acked");

        assert!(tl.since_start("first_seen").is_some());
        assert!(tl.between("first_seen", "acked").is_some());

        // Monotonic: acked is marked after first_seen.
        assert!(tl.since_start("acked").unwrap() >= tl.since_start("first_seen").unwrap());

        // Missing stage.
        assert!(tl.between("first_seen", "landed").is_none());

        // Reversed order is never a positive duration.
        match tl.between("acked", "first_seen") {
            None => {}
            Some(d) => assert_eq!(d, Duration::ZERO),
        }

        let stages: Vec<&str> = tl.stages().collect();
        assert_eq!(stages, vec!["first_seen", "acked"]);
    }

    #[test]
    fn unknown_stage_returns_none() {
        let tl = EventTimeline::start();
        assert!(tl.since_start("nope").is_none());
        assert!(tl.between("a", "b").is_none());
    }
}
