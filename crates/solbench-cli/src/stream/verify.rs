use super::artifacts::{AttemptStatus, Manifest};
use anyhow::{bail, Context, Result};
use sha2::{Digest, Sha256};
use std::{fs, path::Path};
pub fn verify(dir: &Path) -> Result<()> {
    let manifest: Manifest =
        serde_json::from_slice(&fs::read(dir.join("manifest.json"))?).context("manifest")?;
    if manifest.schema_version != super::artifacts::SCHEMA_VERSION {
        bail!("unsupported manifest schema")
    };
    for line in fs::read_to_string(dir.join("checksums.sha256"))?.lines() {
        let (want, name) = line.split_once("  ").context("invalid checksum line")?;
        let got = hex::encode(Sha256::digest(
            fs::read(dir.join(name)).with_context(|| format!("missing {name}"))?,
        ));
        if got != want {
            bail!("checksum mismatch: {name}")
        }
    }
    let events: Vec<solbench_core::MatchedStreamEvent> =
        super::artifacts::read_ndjson(&dir.join("matched-events.ndjson"))?;
    if manifest.status == AttemptStatus::Completed
        && (manifest.matched_signatures < manifest.target_matched_signatures
            || events.len() < manifest.target_matched_signatures)
    {
        bail!("completed attempt does not meet target")
    };
    if manifest.status == AttemptStatus::Aborted {
        bail!(
            "attempt aborted: {}",
            manifest.failure_reason.as_deref().unwrap_or("unspecified")
        )
    };
    println!(
        "verified {} ({} matched signatures)",
        manifest.attempt_id, manifest.matched_signatures
    );
    Ok(())
}
