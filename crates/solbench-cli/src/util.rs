//! Shared CLI helpers (host redaction, formatting).

/// Host portion of a URL with credentials and query stripped for display/logs.
///
/// Examples:
/// - `https://rpc.example.com/?api-key=secret` → `rpc.example.com`
/// - `https://user:pass@rpc.example.com:443/path` → `rpc.example.com:443`
/// - `http://[::1]:8899` → `[::1]:8899` (best-effort; brackets preserved)
pub fn redact_host(url: &str) -> String {
    let rest = url.split_once("://").map(|(_, r)| r).unwrap_or(url);

    // Drop userinfo: user:pass@host
    let after_auth = rest.rsplit_once('@').map(|(_, h)| h).unwrap_or(rest);

    // Authority ends at first `/`, `?`, or `#`
    let authority = after_auth
        .split(['/', '?', '#'])
        .next()
        .unwrap_or(after_auth)
        .trim();

    if authority.is_empty() {
        "unknown".into()
    } else {
        authority.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_query_api_key() {
        assert_eq!(
            redact_host("https://rpc.example.com/?api-key=sekrit"),
            "rpc.example.com"
        );
    }

    #[test]
    fn strips_userinfo() {
        assert_eq!(
            redact_host("https://user:pass@rpc.example.com:443/path"),
            "rpc.example.com:443"
        );
    }

    #[test]
    fn bare_host() {
        assert_eq!(
            redact_host("api.mainnet-beta.solana.com"),
            "api.mainnet-beta.solana.com"
        );
    }

    #[test]
    fn fragment_and_path() {
        assert_eq!(
            redact_host("https://grpc.example.com:443/yellowstone#x"),
            "grpc.example.com:443"
        );
    }
}
