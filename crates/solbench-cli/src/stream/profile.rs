use anyhow::{bail, Result};
use serde::Serialize;

pub const PUMP_AMM_PROGRAM: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";

#[derive(Debug, Clone, Serialize)]
pub struct BenchmarkProfile {
    pub id: &'static str,
    pub vote: bool,
    pub account_include: &'static [&'static str],
}

pub fn profile_by_id(id: &str) -> Result<BenchmarkProfile> {
    if id != "pump_amm_transactions_v1" {
        bail!("unsupported profile `{id}`")
    }
    Ok(BenchmarkProfile {
        id: "pump_amm_transactions_v1",
        vote: false,
        account_include: &[PUMP_AMM_PROGRAM],
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn canonical_profile() {
        let p = profile_by_id("pump_amm_transactions_v1").unwrap();
        assert!(!p.vote);
        assert_eq!(p.account_include, [PUMP_AMM_PROGRAM]);
    }
    #[test]
    fn unknown_fails() {
        assert!(profile_by_id("other").is_err())
    }
}
