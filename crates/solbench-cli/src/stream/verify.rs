use super::artifacts::{AttemptStatus, Manifest};
use anyhow::{bail, Context, Result};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File},
    io::Read,
    path::Path,
};

pub fn verify(dir: &Path) -> Result<()> {
    let manifest: Manifest =
        serde_json::from_slice(&fs::read(dir.join("manifest.json"))?).context("manifest")?;
    if manifest.schema_version != super::artifacts::SCHEMA_VERSION {
        bail!("unsupported manifest schema");
    }

    let checksums_path = dir.join("checksums.sha256");
    if checksums_path.is_file() {
        for line in fs::read_to_string(&checksums_path)?.lines() {
            let (want, name) = line.split_once("  ").context("invalid checksum line")?;
            let got = hex::encode(hash_file(&dir.join(name))?);
            if got != want {
                bail!("checksum mismatch: {name}");
            }
        }
    }

    let event_count = matched_event_count(dir)?;
    if manifest.status == AttemptStatus::Completed
        && (manifest.matched_signatures < manifest.target_matched_signatures
            || event_count < manifest.target_matched_signatures)
    {
        bail!(
            "completed attempt does not meet target (manifest={}, events={}, target={})",
            manifest.matched_signatures,
            event_count,
            manifest.target_matched_signatures
        );
    }
    if manifest.status == AttemptStatus::Aborted {
        bail!(
            "attempt aborted: {}",
            manifest.failure_reason.as_deref().unwrap_or("unspecified")
        );
    }

    let source = if dir.join("matched-events.ndjson").is_file() {
        "matched-events.ndjson"
    } else {
        "matched-events.ndjson.zst"
    };
    println!(
        "verified {} ({} matched signatures via {source})",
        manifest.attempt_id, manifest.matched_signatures
    );
    Ok(())
}

fn matched_event_count(dir: &Path) -> Result<usize> {
    let plain = dir.join("matched-events.ndjson");
    if plain.is_file() {
        return super::artifacts::count_ndjson_records(&plain);
    }
    let zst = dir.join("matched-events.ndjson.zst");
    if zst.is_file() {
        return super::artifacts::count_ndjson_records(&zst);
    }
    bail!(
        "no matched events file (need matched-events.ndjson or matched-events.ndjson.zst) in {}",
        dir.display()
    )
}

fn hash_file(path: &Path) -> Result<sha2::digest::Output<Sha256>> {
    let mut file = File::open(path).with_context(|| format!("missing {}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream::artifacts::{atomic_json, write_checksums, Manifest, PublicSource};
    use crate::stream::config::{MeasurementHost, StreamKind};
    use solbench_core::ClockComparability;
    use std::env;

    fn sample_manifest(status: AttemptStatus, matched: usize, target: usize) -> Manifest {
        Manifest {
            schema_version: super::super::artifacts::SCHEMA_VERSION,
            attempt_id: "stream-test".into(),
            status,
            failure_reason: None,
            started_at: "t0".into(),
            ended_at: Some("t1".into()),
            duration_ns: Some(1),
            config_sha256: "abc".into(),
            profile: "pump_amm_transactions_v1".into(),
            target_matched_signatures: target,
            minimum_match_sources: 2,
            matched_signatures: matched,
            measurement: MeasurementHost {
                public_ip: "1.2.3.4".into(),
                region: "test".into(),
                datacenter: None,
            },
            sources: vec![PublicSource {
                name: "a".into(),
                provider: "x".into(),
                kind: StreamKind::Processed,
                endpoint_host: "example.com".into(),
                resolved_ips: vec![],
                clock_comparability: ClockComparability::Unverified,
            }],
        }
    }

    #[test]
    fn verify_accepts_public_zstd_bundle() {
        let dir = env::temp_dir().join(format!("solbench-verify-zst-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let m = sample_manifest(AttemptStatus::Completed, 2, 2);
        atomic_json(&dir.join("manifest.json"), &m).unwrap();
        let plain = dir.join("matched-events.ndjson");
        // Two empty-shaped events are fine for count; parse not required on verify path.
        fs::write(&plain, "{}\n{}\n").unwrap();
        {
            let input = File::open(&plain).unwrap();
            let output = File::create(dir.join("matched-events.ndjson.zst")).unwrap();
            zstd::stream::copy_encode(input, output, 3).unwrap();
        }
        fs::remove_file(&plain).unwrap();
        write_checksums(&dir).unwrap();

        verify(&dir).unwrap();
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn verify_rejects_aborted() {
        let dir = env::temp_dir().join(format!("solbench-verify-aborted-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let mut m = sample_manifest(AttemptStatus::Aborted, 0, 10);
        m.failure_reason = Some("stall".into());
        atomic_json(&dir.join("manifest.json"), &m).unwrap();
        fs::write(dir.join("matched-events.ndjson"), "").unwrap();
        write_checksums(&dir).unwrap();
        let err = verify(&dir).unwrap_err().to_string();
        assert!(err.contains("aborted"));
        let _ = fs::remove_dir_all(&dir);
    }
}
