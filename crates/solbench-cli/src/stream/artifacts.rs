use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
#[cfg(feature = "grpc")]
use solbench_core::{MatchedStreamEvent, StreamObservation};
#[cfg(feature = "grpc")]
use std::{collections::BTreeMap, fs::File, io::BufWriter, path::PathBuf};
use std::{
    fs::{self, File as StdFile, OpenOptions},
    io::{BufRead, BufReader, Read, Write},
    path::Path,
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

/// Online attempt writer (requires `grpc` feature used by `stream run`).
#[cfg(feature = "grpc")]
pub struct ArtifactWriter {
    pub dir: PathBuf,
    pub manifest: Manifest,
    observations: BufWriter<File>,
    matched: BufWriter<File>,
    pub health: BTreeMap<String, SourceHealth>,
}

#[cfg(feature = "grpc")]
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
            let path = e.path();
            let mut file = StdFile::open(&path)
                .with_context(|| format!("open for checksum {}", path.display()))?;
            let mut hasher = Sha256::new();
            let mut buf = [0u8; 64 * 1024];
            loop {
                let n = file.read(&mut buf)?;
                if n == 0 {
                    break;
                }
                hasher.update(&buf[..n]);
            }
            entries.push((
                e.file_name().to_string_lossy().into_owned(),
                hex::encode(hasher.finalize()),
            ));
        }
    }
    entries.sort();
    let body = entries
        .into_iter()
        .map(|(n, h)| format!("{h}  {n}\n"))
        .collect::<String>();
    fs::write(dir.join("checksums.sha256"), body).context("write checksums")
}

/// Line-oriented NDJSON reader (plain file). Avoids loading the whole file as one string.
pub fn read_ndjson<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<Vec<T>> {
    let file = StdFile::open(path).with_context(|| format!("open ndjson {}", path.display()))?;
    read_ndjson_reader(BufReader::new(file), path)
}

/// NDJSON from a zstd-compressed public bundle (`matched-events.ndjson.zst`).
pub fn read_ndjson_zstd<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<Vec<T>> {
    let file =
        StdFile::open(path).with_context(|| format!("open zstd ndjson {}", path.display()))?;
    let decoder = zstd::stream::read::Decoder::new(file)
        .with_context(|| format!("zstd decoder {}", path.display()))?;
    read_ndjson_reader(BufReader::new(decoder), path)
}

/// Prefer plain `matched-events.ndjson`, else public-bundle `matched-events.ndjson.zst`.
pub fn read_matched_events(dir: &Path) -> Result<Vec<solbench_core::MatchedStreamEvent>> {
    let plain = dir.join("matched-events.ndjson");
    if plain.is_file() {
        return read_ndjson(&plain);
    }
    let zst = dir.join("matched-events.ndjson.zst");
    if zst.is_file() {
        return read_ndjson_zstd(&zst);
    }
    anyhow::bail!(
        "no matched events file: expected matched-events.ndjson or matched-events.ndjson.zst in {}",
        dir.display()
    )
}

fn read_ndjson_reader<R, T>(mut reader: R, path: &Path) -> Result<Vec<T>>
where
    R: BufRead,
    T: for<'de> Deserialize<'de>,
{
    let mut out = Vec::new();
    let mut line = String::new();
    let mut n = 0usize;
    loop {
        line.clear();
        let read = reader
            .read_line(&mut line)
            .with_context(|| format!("read {}", path.display()))?;
        if read == 0 {
            break;
        }
        n += 1;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        out.push(
            serde_json::from_str(trimmed)
                .with_context(|| format!("{} line {n}", path.display()))?,
        );
    }
    Ok(out)
}

/// Count NDJSON records without allocating the full event vector (for verify on large runs).
pub fn count_ndjson_records(path: &Path) -> Result<usize> {
    if path
        .extension()
        .and_then(|e| e.to_str())
        .is_some_and(|e| e == "zst")
    {
        let file = StdFile::open(path)?;
        let decoder = zstd::stream::read::Decoder::new(file)?;
        return count_ndjson_reader(BufReader::new(decoder));
    }
    let file = StdFile::open(path)?;
    count_ndjson_reader(BufReader::new(file))
}

fn count_ndjson_reader<R: BufRead>(mut reader: R) -> Result<usize> {
    let mut buf = String::new();
    let mut n = 0usize;
    loop {
        buf.clear();
        let read = reader.read_line(&mut buf)?;
        if read == 0 {
            break;
        }
        if !buf.trim().is_empty() {
            n += 1;
        }
    }
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn atomic_json_roundtrip() {
        let dir = env::temp_dir().join(format!("solbench-atomic-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("x.json");
        atomic_json(&path, &serde_json::json!({"ok": true})).unwrap();
        let v: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(v["ok"], true);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn ndjson_stream_roundtrip() {
        let dir = env::temp_dir().join(format!("solbench-ndjson-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("rows.ndjson");
        fs::write(&path, "{\"a\":1}\n\n{\"a\":2}\n").unwrap();
        let rows: Vec<serde_json::Value> = read_ndjson(&path).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(count_ndjson_records(&path).unwrap(), 2);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn ndjson_zstd_roundtrip() {
        let dir = env::temp_dir().join(format!("solbench-zstd-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let plain = dir.join("rows.ndjson");
        let zst = dir.join("rows.ndjson.zst");
        fs::write(&plain, "{\"n\":1}\n{\"n\":2}\n").unwrap();
        {
            let input = StdFile::open(&plain).unwrap();
            let output = StdFile::create(&zst).unwrap();
            zstd::stream::copy_encode(input, output, 3).unwrap();
        }
        let rows: Vec<serde_json::Value> = read_ndjson_zstd(&zst).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(count_ndjson_records(&zst).unwrap(), 2);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn matched_events_prefers_plain_then_zst() {
        let dir = env::temp_dir().join(format!("solbench-matched-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        // Minimal MatchedStreamEvent-shaped JSON is heavy; test discovery error path.
        let err = read_matched_events(&dir).unwrap_err().to_string();
        assert!(err.contains("matched-events"));
        let _ = fs::remove_dir_all(&dir);
    }
}
