use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::{fs, path::Path};
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValidationStatus {
    Corroborates,
    Diverges,
    Inconclusive,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct ValidationAttachment {
    pub tool: String,
    pub repository: String,
    pub commit: String,
    pub attempted_at: String,
    pub measurement_host: String,
    pub profile: String,
    pub sample_size: usize,
    pub scope: String,
    pub status: ValidationStatus,
    pub interpretation: String,
    pub command: String,
    pub public_url: Option<String>,
}
pub fn attach(dir: &Path, path: &Path) -> Result<()> {
    super::verify::verify(dir)?;
    let bytes = fs::read(path)?;
    let a: ValidationAttachment =
        serde_json::from_slice(&bytes).context("validation attachment")?;
    let serialized = String::from_utf8_lossy(&bytes);
    if looks_like_credential_material(&serialized) || looks_like_credential_material(&a.command) {
        bail!("validation attachment contains credential material")
    }
    let out = dir.join("validation").join(safe_name(&a.tool));
    fs::create_dir_all(&out)?;
    fs::write(out.join("attachment.json"), bytes)?;
    super::artifacts::write_checksums(dir)?;
    Ok(())
}
fn safe_name(v: &str) -> String {
    v.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect()
}

/// Best-effort scan for secrets pasted into validation attachments.
fn looks_like_credential_material(s: &str) -> bool {
    let lower = s.to_ascii_lowercase();
    const NEEDLES: &[&str] = &[
        "bearer ",
        "authorization:",
        "x-token",
        "api-key=",
        "api_key=",
        "token=",
        "-----begin ",
        "private_key",
        "secret_key",
        "password=",
    ];
    NEEDLES.iter().any(|n| lower.contains(n))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_api_key_material() {
        assert!(looks_like_credential_material(
            r#"{"command":"run --url https://x/?api-key=abc"}"#
        ));
        assert!(!looks_like_credential_material(
            r#"{"command":"geyserbench --slots 200","tool":"GeyserBench"}"#
        ));
    }
}
