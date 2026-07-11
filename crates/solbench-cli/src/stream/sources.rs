use super::{
    config::{SourceConfig, StreamKind},
    profile::BenchmarkProfile,
};
use anyhow::{bail, Context, Result};
use std::{
    collections::HashMap,
    net::ToSocketAddrs,
    time::{Instant, SystemTime, UNIX_EPOCH},
};
#[derive(Debug)]
pub enum SourceEvent {
    Connected {
        source: String,
        endpoint_host: String,
        resolved_ips: Vec<String>,
        connect_ns: u64,
    },
    Observation(solbench_core::StreamObservation),
    Failed {
        source: String,
        reason: String,
    },
}
pub fn resolve_secret(var: &str) -> Result<String> {
    std::env::var(var).with_context(|| format!("required environment variable `{var}` is not set"))
}
pub fn safe_endpoint(raw: &str) -> Result<(String, Vec<String>)> {
    let u = url::Url::parse(raw).context("endpoint must be an absolute URL")?;
    if !u.username().is_empty()
        || u.password().is_some()
        || u.query().is_some()
        || u.fragment().is_some()
    {
        bail!("endpoint URL must not contain userinfo, query, or fragment")
    };
    let host = u.host_str().context("endpoint URL has no host")?;
    let port = u
        .port_or_known_default()
        .context("endpoint URL has no port")?;
    let display = format!("{}://{}:{}", u.scheme(), host, port);
    let ips = (host, port)
        .to_socket_addrs()
        .map(|i| i.map(|a| a.ip().to_string()).collect())
        .unwrap_or_default();
    Ok((display, ips))
}
fn unix_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(u64::MAX as u128) as u64
}
fn proto_ns(v: Option<yellowstone_grpc_proto::prost_types::Timestamp>) -> Option<u64> {
    let v = v?;
    if v.seconds < 0 || v.nanos < 0 {
        return None;
    };
    (v.seconds as u64)
        .checked_mul(1_000_000_000)?
        .checked_add(v.nanos as u64)
}

pub async fn subscribe(
    cfg: SourceConfig,
    profile: BenchmarkProfile,
    epoch: Instant,
    tx: tokio::sync::mpsc::Sender<SourceEvent>,
) {
    let name = cfg.name.clone();
    if let Err(e) = subscribe_inner(cfg, profile, epoch, tx.clone()).await {
        let _ = tx
            .send(SourceEvent::Failed {
                source: name,
                reason: e.to_string(),
            })
            .await;
    }
}
async fn subscribe_inner(
    cfg: SourceConfig,
    profile: BenchmarkProfile,
    epoch: Instant,
    tx: tokio::sync::mpsc::Sender<SourceEvent>,
) -> Result<()> {
    use futures_util::StreamExt;
    use yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcClient};
    let endpoint = resolve_secret(&cfg.endpoint_env)?;
    let token = resolve_secret(&cfg.token_env)
        .ok()
        .filter(|v| !v.is_empty());
    let (host, ips) = safe_endpoint(&endpoint)?;
    let started = Instant::now();
    let mut client = GeyserGrpcClient::build_from_shared(endpoint)?
        .x_token(token)?
        .tls_config(ClientTlsConfig::new().with_native_roots())?
        .connect()
        .await?;
    tx.send(SourceEvent::Connected {
        source: cfg.name.clone(),
        endpoint_host: host,
        resolved_ips: ips,
        connect_ns: started.elapsed().as_nanos() as u64,
    })
    .await
    .ok();
    match cfg.kind {
        StreamKind::Processed => {
            use yellowstone_grpc_proto::geyser::{
                subscribe_update::UpdateOneof, SubscribeRequest, SubscribeRequestFilterTransactions,
            };
            let mut transactions = HashMap::new();
            transactions.insert(
                "pump_amm".into(),
                SubscribeRequestFilterTransactions {
                    vote: Some(profile.vote),
                    failed: None,
                    signature: None,
                    account_include: profile
                        .account_include
                        .iter()
                        .map(|v| v.to_string())
                        .collect(),
                    account_exclude: vec![],
                    account_required: vec![],
                    token_accounts: None,
                },
            );
            let mut stream = client
                .subscribe_once(SubscribeRequest {
                    transactions,
                    ..Default::default()
                })
                .await?;
            while let Some(item) = stream.next().await {
                let u = item?;
                if let Some(UpdateOneof::Transaction(t)) = u.update_oneof {
                    if let Some(info) = t.transaction {
                        let o = solbench_core::StreamObservation {
                            signature: bs58::encode(info.signature).into_string(),
                            slot: t.slot,
                            source: cfg.name.clone(),
                            receive_offset_ns: epoch.elapsed().as_nanos() as u64,
                            receive_unix_ns: unix_ns(),
                            created_at_unix_ns: proto_ns(u.created_at),
                        };
                        if tx.send(SourceEvent::Observation(o)).await.is_err() {
                            return Ok(());
                        }
                    }
                }
            }
        }
        StreamKind::Deshred => {
            use yellowstone_grpc_proto::geyser::{
                subscribe_update_deshred::UpdateOneof, SubscribeDeshredRequest,
                SubscribeRequestFilterDeshredTransactions,
            };
            let mut transactions = HashMap::new();
            transactions.insert(
                "pump_amm".into(),
                SubscribeRequestFilterDeshredTransactions {
                    vote: Some(profile.vote),
                    account_include: profile
                        .account_include
                        .iter()
                        .map(|v| v.to_string())
                        .collect(),
                    account_exclude: vec![],
                    account_required: vec![],
                },
            );
            let mut stream = client
                .subscribe_deshred_once(SubscribeDeshredRequest {
                    deshred_transactions: transactions,
                    ping: None,
                    slots: HashMap::new(),
                })
                .await?;
            while let Some(item) = stream.next().await {
                let u = item?;
                if let Some(UpdateOneof::DeshredTransaction(t)) = u.update_oneof {
                    if let Some(info) = t.transaction {
                        let o = solbench_core::StreamObservation {
                            signature: bs58::encode(info.signature).into_string(),
                            slot: t.slot,
                            source: cfg.name.clone(),
                            receive_offset_ns: epoch.elapsed().as_nanos() as u64,
                            receive_unix_ns: unix_ns(),
                            created_at_unix_ns: proto_ns(u.created_at),
                        };
                        if tx.send(SourceEvent::Observation(o)).await.is_err() {
                            return Ok(());
                        }
                    }
                }
            }
        }
    }
    bail!("stream ended")
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_credential_urls() {
        assert!(safe_endpoint("https://user:pass@example.com/x").is_err());
        assert!(safe_endpoint("https://example.com?token=secret").is_err())
    }
    #[test]
    fn redacts_path() {
        assert_eq!(
            safe_endpoint("https://localhost:443/path").unwrap().0,
            "https://localhost:443"
        )
    }
}
