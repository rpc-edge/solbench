//! `send` — transaction landing benchmark (submit -> on-chain inclusion).
//!
//! Feature-gated (`--features send`) because it pulls `solana-sdk`. DEVNET-first:
//! rpc edge is mainnet-only, and mainnet keypairs belong on your own secure host,
//! never in CI or a repo. Measures actual on-chain inclusion (via
//! getSignatureStatuses), not `sendTransaction`-returned-success.

#[cfg(not(feature = "send"))]
pub fn run(_keypair: Option<String>, _url: Option<String>, _count: usize) -> Result<(), String> {
    Err("built without the `send` feature. Rebuild: cargo build --features send".into())
}

#[cfg(feature = "send")]
pub fn run(keypair: Option<String>, url: Option<String>, count: usize) -> Result<(), String> {
    use crate::util::redact_host;
    use base64::Engine as _;
    use solana_sdk::instruction::Instruction;
    use solana_sdk::message::Message;
    use solana_sdk::pubkey::Pubkey;
    use solana_sdk::signature::read_keypair_file;
    use solana_sdk::signer::Signer;
    use solana_sdk::transaction::Transaction;
    use solbench_core::LandingTracker;
    use std::str::FromStr;
    use std::time::{Duration, Instant};

    let url = url
        .or_else(|| std::env::var("SOLBENCH_SEND_URL").ok())
        .unwrap_or_else(|| "https://api.devnet.solana.com".to_string());
    let kp_path = keypair
        .or_else(|| std::env::var("SOLBENCH_KEYPAIR").ok())
        .ok_or("provide --keypair or SOLBENCH_KEYPAIR (path to a keypair json)")?;
    let payer = read_keypair_file(&kp_path).map_err(|e| format!("read keypair {kp_path}: {e}"))?;

    let agent = ureq::AgentBuilder::new()
        .timeout(Duration::from_secs(20))
        .build();
    // SPL Memo program.
    let memo = Pubkey::from_str("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr").unwrap();

    println!("send: {count} memo tx via {}", redact_host(&url));
    let mut tracker = LandingTracker::new();

    for i in 0..count {
        let blockhash = get_blockhash(&agent, &url)?;
        let note = format!("solbench {i}");
        let ix = Instruction::new_with_bytes(memo, note.as_bytes(), vec![]);
        let msg = Message::new(&[ix], Some(&payer.pubkey()));
        let tx = Transaction::new(&[&payer], msg, blockhash);
        let wire = bincode::serialize(&tx).map_err(|e| e.to_string())?;
        let b64 = base64::engine::general_purpose::STANDARD.encode(wire);

        let submit_slot = get_slot(&agent, &url).unwrap_or(0);
        let started = Instant::now();
        let sig = match send_tx(&agent, &url, &b64) {
            Ok(sig) => sig,
            Err(e) => {
                tracker.record_failed();
                println!("  {i}: submit rejected ({e})");
                continue;
            }
        };

        // Poll for actual on-chain inclusion.
        let mut landed = None;
        while started.elapsed() < Duration::from_secs(30) {
            if let Some(slot) = sig_slot(&agent, &url, &sig)? {
                landed = Some(slot);
                break;
            }
            std::thread::sleep(Duration::from_millis(500));
        }
        match landed {
            Some(slot) => {
                let delta = slot as i64 - submit_slot as i64;
                tracker.record_landed(delta);
                println!(
                    "  {i}: landed slot {slot} (+{delta}) in {:?}",
                    started.elapsed()
                );
            }
            None => {
                tracker.record_failed();
                println!("  {i}: not landed within 30s");
            }
        }
    }

    println!(
        "\nlanding rate: {:.1}%  ({}/{} landed)",
        tracker.landing_rate() * 100.0,
        tracker.landed,
        tracker.attempts
    );
    if let Some(s) = tracker.slot_delta_summary() {
        println!(
            "slots-to-land: min {} / median {} / max {} (mean {})",
            s.min, s.median, s.max, s.mean
        );
    }
    Ok(())
}

#[cfg(feature = "send")]
fn rpc(
    agent: &ureq::Agent,
    url: &str,
    method: &str,
    params: serde_json::Value,
) -> Result<serde_json::Value, String> {
    let body = serde_json::json!({"jsonrpc":"2.0","id":1,"method":method,"params":params});
    let resp = agent
        .post(url)
        .set("content-type", "application/json")
        .send_string(&body.to_string())
        .map_err(|e| format!("{method}: {e}"))?;
    let v: serde_json::Value = resp
        .into_string()
        .ok()
        .and_then(|t| serde_json::from_str(&t).ok())
        .ok_or_else(|| format!("{method}: bad response"))?;
    if let Some(err) = v.get("error") {
        return Err(format!("{method}: {err}"));
    }
    Ok(v.get("result").cloned().unwrap_or(serde_json::Value::Null))
}

#[cfg(feature = "send")]
fn get_blockhash(agent: &ureq::Agent, url: &str) -> Result<solana_sdk::hash::Hash, String> {
    use std::str::FromStr;
    let r = rpc(agent, url, "getLatestBlockhash", serde_json::json!([]))?;
    let bh = r
        .get("value")
        .and_then(|v| v.get("blockhash"))
        .and_then(|b| b.as_str())
        .ok_or("getLatestBlockhash: no blockhash")?;
    solana_sdk::hash::Hash::from_str(bh).map_err(|e| e.to_string())
}

#[cfg(feature = "send")]
fn get_slot(agent: &ureq::Agent, url: &str) -> Result<u64, String> {
    rpc(agent, url, "getSlot", serde_json::json!([]))?
        .as_u64()
        .ok_or_else(|| "getSlot: not a number".into())
}

#[cfg(feature = "send")]
fn send_tx(agent: &ureq::Agent, url: &str, b64: &str) -> Result<String, String> {
    let r = rpc(
        agent,
        url,
        "sendTransaction",
        serde_json::json!([b64, {"encoding":"base64"}]),
    )?;
    r.as_str()
        .map(str::to_string)
        .ok_or_else(|| "sendTransaction: no signature".into())
}

#[cfg(feature = "send")]
fn sig_slot(agent: &ureq::Agent, url: &str, sig: &str) -> Result<Option<u64>, String> {
    let r = rpc(
        agent,
        url,
        "getSignatureStatuses",
        serde_json::json!([[sig], {"searchTransactionHistory": true}]),
    )?;
    let status = r
        .get("value")
        .and_then(|v| v.get(0))
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    if status.is_null() {
        return Ok(None);
    }
    // Landed once confirmed/finalized (and not errored).
    let confirmed = status
        .get("confirmationStatus")
        .and_then(|c| c.as_str())
        .map(|c| c == "confirmed" || c == "finalized")
        .unwrap_or(false);
    if confirmed && status.get("err").map(|e| e.is_null()).unwrap_or(true) {
        Ok(status.get("slot").and_then(|s| s.as_u64()))
    } else {
        Ok(None)
    }
}
