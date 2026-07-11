#[cfg(feature = "grpc")]
use super::{
    artifacts::{ArtifactWriter, AttemptStatus, Manifest, PublicSource, SCHEMA_VERSION},
    config::StreamConfig,
    sources::SourceEvent,
};
use anyhow::{bail, Result};
#[cfg(feature = "grpc")]
use sha2::{Digest, Sha256};
#[cfg(not(feature = "grpc"))]
use std::path::Path;
#[cfg(feature = "grpc")]
use std::{
    collections::HashSet,
    fs,
    path::Path,
    time::{Duration, Instant},
};
pub fn run(config_path: &Path, root: &Path, smoke: Option<usize>) -> Result<()> {
    #[cfg(not(feature = "grpc"))]
    {
        let _ = (config_path, root, smoke);
        bail!("built without grpc feature")
    };
    #[cfg(feature = "grpc")]
    {
        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(run_async(config_path, root, smoke))
    }
}
#[cfg(feature = "grpc")]
async fn run_async(config_path: &Path, root: &Path, smoke: Option<usize>) -> Result<()> {
    let mut cfg = StreamConfig::load(config_path)?;
    if let Some(v) = smoke {
        if v == 0 || v > cfg.run.target_matched_signatures {
            bail!("smoke target must be between 1 and configured target")
        };
        cfg.run.target_matched_signatures = v
    };
    let raw = fs::read(config_path)?;
    let hash = hex::encode(Sha256::digest(&raw));
    let attempt_id = format!("stream-{}", chrono::Utc::now().format("%Y%m%dT%H%M%SZ"));
    let epoch = Instant::now();
    let manifest = Manifest {
        schema_version: SCHEMA_VERSION,
        attempt_id: attempt_id.clone(),
        status: AttemptStatus::Created,
        failure_reason: None,
        started_at: chrono::Utc::now().to_rfc3339(),
        ended_at: None,
        duration_ns: None,
        config_sha256: hash,
        profile: cfg.run.profile.clone(),
        target_matched_signatures: cfg.run.target_matched_signatures,
        minimum_match_sources: cfg.run.minimum_match_sources,
        matched_signatures: 0,
        measurement: cfg.measurement.clone(),
        sources: cfg
            .sources
            .iter()
            .map(|s| PublicSource {
                name: s.name.clone(),
                provider: s.provider.clone(),
                kind: s.kind,
                endpoint_host: "resolved at runtime".into(),
                resolved_ips: vec![],
                clock_comparability: s.clock_comparability,
            })
            .collect(),
    };
    let redacted = toml::to_string_pretty(&cfg)?;
    let mut artifacts = ArtifactWriter::create(root, manifest, &redacted)?;
    artifacts.manifest.status = AttemptStatus::Running;
    let (tx, mut rx) = tokio::sync::mpsc::channel(8192);
    let profile = super::profile::profile_by_id(&cfg.run.profile)?;
    for s in cfg.sources.clone() {
        tokio::spawn(super::sources::subscribe(
            s,
            profile.clone(),
            epoch,
            tx.clone(),
        ));
    }
    drop(tx);
    let required = cfg
        .sources
        .iter()
        .filter(|s| s.required)
        .map(|s| s.name.clone())
        .collect::<HashSet<_>>();
    let mut connected = HashSet::new();
    let mut warm = HashSet::new();
    let mut book = solbench_core::StreamMatchBook::new(cfg.run.minimum_match_sources);
    let startup = tokio::time::sleep(Duration::from_secs(cfg.run.startup_timeout_seconds));
    tokio::pin!(startup);
    let wall = tokio::time::sleep(Duration::from_secs(cfg.run.maximum_wall_seconds));
    tokio::pin!(wall);
    let mut grace: Option<std::pin::Pin<Box<tokio::time::Sleep>>> = None;
    let mut failure = None;
    loop {
        tokio::select! {_=&mut startup,if !required.is_subset(&warm)=>{failure=Some(format!("startup timeout; ready {:?}, required {:?}",warm,required));break},_=&mut wall=>{failure=Some("maximum wall time reached before completion".into());break},_=async{if let Some(g)=grace.as_mut(){g.await}},if grace.is_some()=>break,event=rx.recv()=>{match event{Some(SourceEvent::Connected{source,endpoint_host,resolved_ips,connect_ns})=>{connected.insert(source.clone());let h=artifacts.health.entry(source.clone()).or_default();h.connects+=1;h.connect_duration_ns=Some(connect_ns);if let Some(m)=artifacts.manifest.sources.iter_mut().find(|v|v.name==source){m.endpoint_host=endpoint_host;m.resolved_ips=resolved_ips}},Some(SourceEvent::Observation(o))=>{let h=artifacts.health.entry(o.source.clone()).or_default();h.messages+=1;h.first_event_offset_ns.get_or_insert(o.receive_offset_ns);h.last_event_offset_ns=Some(o.receive_offset_ns);if !required.is_subset(&warm){warm.insert(o.source.clone());continue}artifacts.observation(&o)?;let newly=book.observe(o);if newly&&book.matched_count()>=cfg.run.target_matched_signatures&&grace.is_none(){grace=Some(Box::pin(tokio::time::sleep(Duration::from_secs(cfg.run.completion_grace_seconds))))}},Some(SourceEvent::Failed{source,reason})=>{artifacts.health.entry(source.clone()).or_default().errors.push(reason.clone());if required.contains(&source){failure=Some(format!("required source `{source}` failed: {reason}"));break}},None=>{failure=Some("all source tasks ended".into());break}}}}
    }
    for (s, n) in book.duplicates() {
        artifacts.health.entry(s.clone()).or_default().duplicates = *n
    }
    let matched = book.matched_count();
    for event in book.into_events() {
        artifacts.matched(&event)?
    }
    let status = if failure.is_none() && matched >= cfg.run.target_matched_signatures {
        AttemptStatus::Completed
    } else {
        AttemptStatus::Aborted
    };
    let dir = artifacts.finish(
        status.clone(),
        failure.clone(),
        matched,
        epoch.elapsed().as_nanos() as u64,
    )?;
    println!(
        "attempt {attempt_id}: {:?}; {matched}/{} matched; artifacts {}",
        status,
        cfg.run.target_matched_signatures,
        dir.display()
    );
    if status == AttemptStatus::Aborted {
        bail!(
            "{}",
            failure.unwrap_or_else(|| "attempt did not meet target".into())
        )
    };
    Ok(())
}
