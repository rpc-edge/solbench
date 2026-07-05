//! `grpc` — Yellowstone gRPC slot first-seen race (feature `grpc`).
//!
//! The correct way to measure streaming latency is a *relative* race between two
//! concurrent subscriptions — which endpoint reports the same (slot, status) event
//! first, and by how much — NOT `receive_time - blockTime` (Solana blockTime has
//! 1s precision, so that method reports a spurious ~1s artifact). Endpoints come
//! from SOLBENCH_GRPC_A / SOLBENCH_GRPC_B, with optional x-tokens in
//! SOLBENCH_GRPC_A_TOKEN / SOLBENCH_GRPC_B_TOKEN.

#[cfg(not(feature = "grpc"))]
pub fn race(_slots: usize) -> Result<(), String> {
    Err("built without the `grpc` feature. Rebuild: cargo build --features grpc".into())
}

#[cfg(feature = "grpc")]
fn redact_host(url: &str) -> String {
    let no_scheme = url.split("://").nth(1).unwrap_or(url);
    no_scheme
        .split(['/', '?'])
        .next()
        .unwrap_or(no_scheme)
        .to_string()
}

#[cfg(feature = "grpc")]
pub fn race(target: usize) -> Result<(), String> {
    use solbench_core::LatencyRecorder;
    use std::collections::HashMap;
    use std::time::Instant;
    use tokio::sync::mpsc;

    let a = std::env::var("SOLBENCH_GRPC_A")
        .map_err(|_| "set SOLBENCH_GRPC_A (e.g. https://grpc.rpcedge.com:443)")?;
    let b = std::env::var("SOLBENCH_GRPC_B")
        .map_err(|_| "set SOLBENCH_GRPC_B (a second gRPC endpoint to race against)")?;
    let a_tok = std::env::var("SOLBENCH_GRPC_A_TOKEN").ok();
    let b_tok = std::env::var("SOLBENCH_GRPC_B_TOKEN").ok();
    let (host_a, host_b) = (redact_host(&a), redact_host(&b));

    let rt = tokio::runtime::Runtime::new().map_err(|e| e.to_string())?;
    rt.block_on(async move {
        // (id, slot, status, seen_at)
        let (tx, mut rx) = mpsc::channel::<(u8, u64, i32, Instant)>(2048);
        let ta = tokio::spawn(subscribe_slots(0, a, a_tok, tx.clone()));
        let tb = tokio::spawn(subscribe_slots(1, b, b_tok, tx));

        // (slot, status) -> (first endpoint id, first seen)
        let mut first: HashMap<(u64, i32), (u8, Instant)> = HashMap::new();
        let mut wins = [0usize; 2];
        let mut deltas = LatencyRecorder::new();
        let mut raced = 0usize;

        println!("racing {host_a} (A) vs {host_b} (B) on slot first-seen ...");
        while raced < target {
            let Some((id, slot, status, at)) = rx.recv().await else {
                break; // both streams ended
            };
            match first.get(&(slot, status)) {
                None => {
                    first.insert((slot, status), (id, at));
                }
                Some(&(fid, fat)) if fid != id => {
                    wins[fid as usize] += 1;
                    deltas.record(at.saturating_duration_since(fat));
                    first.remove(&(slot, status));
                    raced += 1;
                    if raced.is_multiple_of(25) {
                        println!("  {raced}/{target} events raced");
                    }
                }
                _ => {} // duplicate from the same endpoint; ignore
            }
        }
        ta.abort();
        tb.abort();

        let total = wins[0] + wins[1];
        if total == 0 {
            return Err("no events seen by BOTH endpoints - check URLs / tokens".to_string());
        }
        let pct = |w: usize| 100.0 * w as f64 / total as f64;
        println!("\nfirst-seen race over {total} shared events:");
        println!(
            "  A  {host_a:<30} {:>5} wins ({:.0}%)",
            wins[0],
            pct(wins[0])
        );
        println!(
            "  B  {host_b:<30} {:>5} wins ({:.0}%)",
            wins[1],
            pct(wins[1])
        );
        if let Some(s) = deltas.summary() {
            let ms = |ns: u64| ns as f64 / 1e6;
            println!(
                "  winner leads by: p50 {:.1}ms  p99 {:.1}ms  max {:.1}ms",
                ms(s.p50_ns),
                ms(s.p99_ns),
                ms(s.max_ns)
            );
        }
        Ok(())
    })
}

#[cfg(feature = "grpc")]
async fn subscribe_slots(
    id: u8,
    endpoint: String,
    token: Option<String>,
    tx: tokio::sync::mpsc::Sender<(u8, u64, i32, std::time::Instant)>,
) {
    use futures_util::StreamExt;
    use std::collections::HashMap;
    use std::time::Instant;
    use yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcClient};
    use yellowstone_grpc_proto::geyser::{
        subscribe_update::UpdateOneof, SubscribeRequest, SubscribeRequestFilterSlots,
    };

    let connect = async {
        let mut client = GeyserGrpcClient::build_from_shared(endpoint.clone())
            .map_err(|e| e.to_string())?
            .x_token(token.clone())
            .map_err(|e| e.to_string())?
            .tls_config(ClientTlsConfig::new().with_native_roots())
            .map_err(|e| e.to_string())?
            .connect()
            .await
            .map_err(|e| e.to_string())?;

        let mut slots = HashMap::new();
        slots.insert("slots".to_string(), SubscribeRequestFilterSlots::default());
        let req = SubscribeRequest {
            slots,
            ..Default::default()
        };
        client.subscribe_once(req).await.map_err(|e| e.to_string())
    };

    match connect.await {
        Ok(mut stream) => {
            while let Some(update) = stream.next().await {
                let Ok(u) = update else { break };
                if let Some(UpdateOneof::Slot(s)) = u.update_oneof {
                    if tx
                        .send((id, s.slot, s.status, Instant::now()))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            }
        }
        Err(e) => eprintln!("grpc endpoint {}: {e}", if id == 0 { "A" } else { "B" }),
    }
}
