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
    if serialized.contains("Bearer ")
        || serialized.to_ascii_lowercase().contains("x-token")
        || a.command.contains("TOKEN=")
    {
        bail!("validation attachment contains credential material")
    };
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
