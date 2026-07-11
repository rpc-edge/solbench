use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use solbench_core::{MatchedStreamEvent, StreamObservation};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
};

pub const SCHEMA_VERSION: u32 = 1;
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AttemptStatus {
    Created,
    Running,
    Completed,
    Aborted,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub schema_version: u32,
    pub attempt_id: String,
    pub status: AttemptStatus,
    pub failure_reason: Option<String>,
    pub started_at: String,
    pub ended_at: Option<String>,
    pub duration_ns: Option<u64>,
    pub config_sha256: String,
    pub profile: String,
    pub target_matched_signatures: usize,
    pub minimum_match_sources: usize,
    pub matched_signatures: usize,
    pub measurement: super::config::MeasurementHost,
    pub sources: Vec<PublicSource>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublicSource {
    pub name: String,
    pub provider: String,
    pub kind: super::config::StreamKind,
    pub endpoint_host: String,
    pub resolved_ips: Vec<String>,
    pub clock_comparability: solbench_core::ClockComparability,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SourceHealth {
    pub messages: u64,
    pub duplicates: u64,
    pub connects: u64,
    pub connect_duration_ns: Option<u64>,
    pub disconnects: u64,
    pub errors: Vec<String>,
    pub first_event_offset_ns: Option<u64>,
    pub last_event_offset_ns: Option<u64>,
}

pub struct ArtifactWriter {
    pub dir: PathBuf,
    pub manifest: Manifest,
    observations: BufWriter<File>,
    matched: BufWriter<File>,
    pub health: BTreeMap<String, SourceHealth>,
}
impl ArtifactWriter {
    pub fn create(root: &Path, manifest: Manifest, redacted_config: &str) -> Result<Self> {
        let dir = root.join(&manifest.attempt_id);
        fs::create_dir_all(&dir)?;
        atomic_json(&dir.join("manifest.json"), &manifest)?;
        fs::write(dir.join("config.redacted.toml"), redacted_config)?;
        Ok(Self {
            observations: BufWriter::new(File::create(dir.join("observations.ndjson"))?),
            matched: BufWriter::new(File::create(dir.join("matched-events.ndjson"))?),
            health: BTreeMap::new(),
            dir,
            manifest,
        })
    }
    pub fn observation(&mut self, v: &StreamObservation) -> Result<()> {
        serde_json::to_writer(&mut self.observations, v)?;
        self.observations.write_all(b"\n")?;
        Ok(())
    }
    pub fn matched(&mut self, v: &MatchedStreamEvent) -> Result<()> {
        serde_json::to_writer(&mut self.matched, v)?;
        self.matched.write_all(b"\n")?;
        Ok(())
    }
    pub fn finish(
        mut self,
        status: AttemptStatus,
        reason: Option<String>,
        matched: usize,
        duration_ns: u64,
    ) -> Result<PathBuf> {
        self.observations.flush()?;
        self.matched.flush()?;
        self.manifest.status = status;
        self.manifest.failure_reason = reason;
        self.manifest.matched_signatures = matched;
        self.manifest.duration_ns = Some(duration_ns);
        self.manifest.ended_at = Some(chrono::Utc::now().to_rfc3339());
        atomic_json(&self.dir.join("manifest.json"), &self.manifest)?;
        atomic_json(&self.dir.join("source-health.json"), &self.health)?;
        write_checksums(&self.dir)?;
        Ok(self.dir)
    }
}
pub fn atomic_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let tmp = path.with_extension("tmp");
    let mut f = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&tmp)?;
    serde_json::to_writer_pretty(&mut f, value)?;
    f.write_all(b"\n")?;
    f.sync_all()?;
    fs::rename(tmp, path)?;
    Ok(())
}
pub fn write_checksums(dir: &Path) -> Result<()> {
    use sha2::{Digest, Sha256};
    let mut entries = Vec::new();
    for e in fs::read_dir(dir)? {
        let e = e?;
        if e.file_type()?.is_file() && e.file_name() != "checksums.sha256" {
            let data = fs::read(e.path())?;
            entries.push((
                e.file_name().to_string_lossy().into_owned(),
                hex::encode(Sha256::digest(data)),
            ))
        }
    }
    entries.sort();
    let body = entries
        .into_iter()
        .map(|(n, h)| format!("{h}  {n}\n"))
        .collect::<String>();
    fs::write(dir.join("checksums.sha256"), body).context("write checksums")
}
pub fn read_ndjson<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<Vec<T>> {
    let raw = fs::read_to_string(path)?;
    raw.lines()
        .enumerate()
        .map(|(i, l)| {
            serde_json::from_str(l).with_context(|| format!("{} line {}", path.display(), i + 1))
        })
        .collect()
}
