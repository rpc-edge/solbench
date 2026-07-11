//! Network-free transaction-stream matching and timing analysis.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClockComparability {
    Verified,
    Unverified,
    Incompatible,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamObservation {
    pub signature: String,
    pub slot: u64,
    pub source: String,
    pub receive_offset_ns: u64,
    pub receive_unix_ns: u64,
    pub created_at_unix_ns: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceArrival {
    pub slot: u64,
    pub receive_offset_ns: u64,
    pub receive_unix_ns: u64,
    pub created_at_unix_ns: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MatchedStreamEvent {
    pub signature: String,
    pub arrivals: BTreeMap<String, SourceArrival>,
}

#[derive(Debug, Default)]
pub struct StreamMatchBook {
    minimum_sources: usize,
    events: HashMap<String, MatchedStreamEvent>,
    matched: usize,
    duplicates: BTreeMap<String, u64>,
}

impl StreamMatchBook {
    pub fn new(minimum_sources: usize) -> Self {
        Self {
            minimum_sources,
            ..Self::default()
        }
    }
    pub fn observe(&mut self, obs: StreamObservation) -> bool {
        let event =
            self.events
                .entry(obs.signature.clone())
                .or_insert_with(|| MatchedStreamEvent {
                    signature: obs.signature.clone(),
                    arrivals: BTreeMap::new(),
                });
        if event.arrivals.contains_key(&obs.source) {
            *self.duplicates.entry(obs.source).or_default() += 1;
            return false;
        }
        let before = event.arrivals.len() >= self.minimum_sources;
        event.arrivals.insert(
            obs.source,
            SourceArrival {
                slot: obs.slot,
                receive_offset_ns: obs.receive_offset_ns,
                receive_unix_ns: obs.receive_unix_ns,
                created_at_unix_ns: obs.created_at_unix_ns,
            },
        );
        let newly = !before && event.arrivals.len() >= self.minimum_sources;
        if newly {
            self.matched += 1;
        }
        newly
    }
    pub fn matched_count(&self) -> usize {
        self.matched
    }
    pub fn duplicates(&self) -> &BTreeMap<String, u64> {
        &self.duplicates
    }
    pub fn events(&self) -> impl Iterator<Item = &MatchedStreamEvent> {
        self.events.values()
    }
    pub fn into_events(self) -> Vec<MatchedStreamEvent> {
        self.events
            .into_values()
            .filter(|e| e.arrivals.len() >= self.minimum_sources)
            .collect()
    }
}

pub fn source_to_client_age(receive_unix_ns: u64, created_at_unix_ns: Option<u64>) -> Option<u64> {
    receive_unix_ns.checked_sub(created_at_unix_ns?)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn obs(sig: &str, source: &str, at: u64) -> StreamObservation {
        StreamObservation {
            signature: sig.into(),
            slot: 1,
            source: source.into(),
            receive_offset_ns: at,
            receive_unix_ns: 100 + at,
            created_at_unix_ns: Some(90),
        }
    }
    #[test]
    fn matches_once_and_deduplicates() {
        let mut b = StreamMatchBook::new(2);
        assert!(!b.observe(obs("s", "a", 1)));
        assert!(!b.observe(obs("s", "a", 2)));
        assert!(b.observe(obs("s", "b", 3)));
        assert!(!b.observe(obs("s", "c", 4)));
        assert_eq!(b.matched_count(), 1);
        assert_eq!(b.duplicates()["a"], 1);
    }
    #[test]
    fn timestamps_fail_closed() {
        assert_eq!(source_to_client_age(100, Some(90)), Some(10));
        assert_eq!(source_to_client_age(90, Some(100)), None);
        assert_eq!(source_to_client_age(90, None), None);
    }
}
