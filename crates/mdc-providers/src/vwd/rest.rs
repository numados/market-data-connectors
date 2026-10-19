//! HTTP REST client and query builders for VWD SNAPQUOTE endpoint.

use bytes::Bytes;

use mdc_core::{MdcError, SourceErrorKind, UnixNanos};
use secrecy::ExposeSecret;
use url::Url;

use crate::vwd::config::{VwdConfig, VwdCredentials, VwdSymbol};
use crate::vwd::normalize::{build_instrument_key, build_inverse_instrument_key};

/// Formats the comma-separated instrument keys parameter from configured symbols.
pub fn build_snapquote_keys(symbols: &[VwdSymbol]) -> String {
    let mut keys = Vec::new();
    for sym in symbols {
        if let Some(direct) = build_instrument_key(sym.symbol.as_str()) {
            keys.push(direct.as_str().to_string());
        }
        if sym.allow_inverse {
            if let Some(inv) = build_inverse_instrument_key(sym.symbol.as_str()) {
                keys.push(inv.as_str().to_string());
            }
        }
    }
    keys.join(",")
}

/// Constructs the complete SNAPQUOTE request URL for the account and keys.
pub fn build_snapquote_url(
    base_url: &Url,
    account_path: &str,
    keys: &str,
) -> Result<Url, MdcError> {
    let clean_acct = account_path.trim_matches('/');
    let relative = format!("{clean_acct}/JSON/QUOTE/SNAPQUOTE/{keys}");
    base_url
        .join(&relative)
        .map_err(|e| MdcError::Protocol(format!("failed to construct SNAPQUOTE URL: {e}")))
}

/// Client result of a SNAPQUOTE poll.
pub struct PollResponse {
    /// Received response body bytes.
    pub body: Bytes,
    /// Ingestion timestamp captured immediately when response headers/body are received.
    pub received_ts: UnixNanos,
}

/// REST HTTP client executing authenticated SNAPQUOTE requests against VWD.
pub struct VwdRestClient {
    client: reqwest::Client,
    base_url: Url,
    account_path: secrecy::SecretString,
    keys: String,
    credentials: VwdCredentials,
}

impl std::fmt::Debug for VwdRestClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VwdRestClient")
            .field("base_url", &self.base_url)
            .field("account_path", &"[REDACTED]")
            .field("keys", &self.keys)
            .field("credentials", &self.credentials)
            .finish()
    }
}

impl VwdRestClient {
    /// Creates a new `VwdRestClient` from validated config.
    pub fn new(config: &VwdConfig) -> Result<Self, MdcError> {
        let keys = build_snapquote_keys(&config.symbols);
        // Validate SNAPQUOTE URL construction upfront
        let _ = build_snapquote_url(&config.base_url, config.account_path.expose_secret(), &keys)?;

        let client = crate::http::build_standard_http_client(config.request_timeout)
            .map_err(|e| MdcError::Transport(Box::new(e)))?;

        Ok(Self {
            client,
            base_url: config.base_url.clone(),
            account_path: config.account_path.clone(),
            keys,
            credentials: config.credentials.clone(),
        })
    }

    /// Fetches the latest batch SNAPQUOTE payload.
    pub async fn fetch_snap_quotes(&self) -> Result<PollResponse, SourceErrorKind> {
        let url = build_snapquote_url(
            &self.base_url,
            self.account_path.expose_secret(),
            &self.keys,
        )
        .map_err(|_| SourceErrorKind::Transport)?;

        let response = self
            .client
            .get(url)
            .basic_auth(
                self.credentials.username.expose_secret(),
                Some(self.credentials.password.expose_secret()),
            )
            .send()
            .await
            .map_err(|_| SourceErrorKind::Transport)?;

        let status = response.status();
        if !status.is_success() {
            return Err(SourceErrorKind::HttpStatus(status.as_u16()));
        }

        let body = crate::http::read_response_bytes_capped(
            response,
            crate::http::DEFAULT_REST_SNAPSHOT_CAP,
        )
        .await
        .map_err(|_| SourceErrorKind::Transport)?;

        let received_ts = crate::now_nanos();

        Ok(PollResponse { body, received_ts })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]

    fn test_build_snapquote_keys() {
        let symbols = vec![
            VwdSymbol::with_inverse("EURUSD"),
            VwdSymbol::direct("GBPUSD"),
        ];
        let keys = build_snapquote_keys(&symbols);
        assert_eq!(
            keys,
            "EURUSD.XRATE.SPOT~X,USDEUR.XRATE.SPOT~X,GBPUSD.XRATE.SPOT~X"
        );
    }

    #[test]
    fn test_build_snapquote_url() {
        let base = Url::parse("https://rest-interface.vwd.com").unwrap();
        let url = build_snapquote_url(&base, "my-account", "EURUSD.XRATE.SPOT~X").unwrap();
        assert_eq!(
            url.as_str(),
            "https://rest-interface.vwd.com/my-account/JSON/QUOTE/SNAPQUOTE/EURUSD.XRATE.SPOT~X"
        );
    }

    #[test]
    fn test_vwd_rest_client_debug_redacts_credentials() {
        let config = VwdConfig::builder()
            .account_path("super-secret-account")
            .credentials(VwdCredentials::new("secret-user", "secret-pass"))
            .add_symbol(VwdSymbol::direct("EURUSD"))
            .build()
            .unwrap();
        let client = VwdRestClient::new(&config).unwrap();
        let debug = format!("{client:?}");
        assert!(!debug.contains("super-secret-account"));
        assert!(!debug.contains("secret-user"));
        assert!(!debug.contains("secret-pass"));
        assert!(debug.contains("[REDACTED]"));
    }
}
