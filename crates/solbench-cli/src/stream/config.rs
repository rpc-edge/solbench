use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use solbench_core::ClockComparability;
use std::{collections::HashSet, fs, path::Path};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamConfig {
    pub run: RunConfig,
    pub measurement: MeasurementHost,
    pub sources: Vec<SourceConfig>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunConfig {
    pub profile: String,
    pub target_matched_signatures: usize,
    pub minimum_match_sources: usize,
    pub startup_timeout_seconds: u64,
    pub stall_timeout_seconds: u64,
    pub completion_grace_seconds: u64,
    pub maximum_wall_seconds: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeasurementHost {
    pub public_ip: String,
    pub region: String,
    pub datacenter: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceConfig {
    pub name: String,
    pub provider: String,
    pub kind: StreamKind,
    pub endpoint_env: String,
    pub token_env: String,
    pub required: bool,
    pub clock_comparability: ClockComparability,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamKind {
    Processed,
    Deshred,
}

impl StreamConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let raw =
            fs::read_to_string(path).with_context(|| format!("read config {}", path.display()))?;
        let c: Self = toml::from_str(&raw).context("parse stream config")?;
        c.validate()?;
        Ok(c)
    }
    pub fn validate(&self) -> Result<()> {
        super::profile::profile_by_id(&self.run.profile)?;
        if self.sources.len() < 2 {
            bail!("sources: at least two are required")
        }
        if self.run.minimum_match_sources < 2 || self.run.minimum_match_sources > self.sources.len()
        {
            bail!("run.minimum_match_sources must be between 2 and source count")
        }
        if self.run.target_matched_signatures == 0
            || self.run.completion_grace_seconds == 0
            || self.run.maximum_wall_seconds == 0
        {
            bail!("run target/grace/maximum_wall must be non-zero")
        }
        if self.measurement.public_ip.trim().is_empty() || self.measurement.region.trim().is_empty()
        {
            bail!("measurement.public_ip and measurement.region are required")
        }
        let mut names = HashSet::new();
        for s in &self.sources {
            if !names.insert(&s.name) {
                bail!("sources.name `{}` is duplicated", s.name)
            };
            validate_env_name("endpoint_env", &s.endpoint_env)?;
            validate_env_name("token_env", &s.token_env)?;
        }
        Ok(())
    }
}
fn validate_env_name(field: &str, value: &str) -> Result<()> {
    if value.is_empty()
        || value.contains("://")
        || !value
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
    {
        bail!("sources.{field} must be an environment-variable name")
    };
    Ok(())
}
