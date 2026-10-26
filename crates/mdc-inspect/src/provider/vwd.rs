//! VWD/Infront authenticated synthetic one-level indicative book adapter.

use std::sync::Arc;
use std::time::Duration;

use mdc_core::{MdcError, Owned};
use mdc_providers::vwd::{VwdConfig, VwdConnector, VwdCredentials, VwdSymbol};
use mdc_sinks::loss_intolerant::LossIntolerantSender;
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::config::SessionConfig;
use crate::error::Error;
use crate::provider::{AbortOnDropHandle, ProviderRun, SessionAdapter};

const ACCOUNT_PATH: &str = "MDC_INSPECT_VWD_ACCOUNT_PATH";
const USERNAME: &str = "MDC_INSPECT_VWD_USERNAME";
const PASSWORD: &str = "MDC_INSPECT_VWD_PASSWORD";

/// Isolated environment lookup used by VWD; tests can provide an in-memory map.
pub trait EnvLookup: Send + Sync {
    /// Returns a Unicode environment value, or `None` when absent/invalid.
    fn get(&self, key: &str) -> Option<String>;
}

struct ProcessEnv;

impl EnvLookup for ProcessEnv {
    fn get(&self, key: &str) -> Option<String> {
        std::env::var(key).ok()
    }
}

/// Adapter for VWD's direct FX pairs and synthetic L2 snapshots.
pub struct VwdAdapter {
    env: Arc<dyn EnvLookup>,
    rest_url: Option<Url>,
}

impl std::fmt::Debug for VwdAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VwdAdapter")
            .field("rest_url", &self.rest_url)
            .finish()
    }
}

impl VwdAdapter {
    /// Uses the process environment and the provider's production endpoint.
    pub fn new() -> Self {
        Self {
            env: Arc::new(ProcessEnv),
            rest_url: None,
        }
    }

    /// Uses an injected environment and endpoint for deterministic tests.
    pub fn with_env<E: EnvLookup + 'static>(env: E, rest_url: Url) -> Self {
        Self {
            env: Arc::new(env),
            rest_url: Some(rest_url),
        }
    }

    fn credentials(&self) -> Result<(String, String, String), Error> {
        let account = self
            .env
            .get(ACCOUNT_PATH)
            .filter(|v| !v.is_empty())
            .ok_or_else(|| Error::Config(ACCOUNT_PATH.to_string()))?;
        let username = self
            .env
            .get(USERNAME)
            .filter(|v| !v.is_empty())
            .ok_or_else(|| Error::Config(USERNAME.to_string()))?;
        let password = self
            .env
            .get(PASSWORD)
            .filter(|v| !v.is_empty())
            .ok_or_else(|| Error::Config(PASSWORD.to_string()))?;
        Ok((account, username, password))
    }

    fn build_config(&self, config: &SessionConfig) -> Result<VwdConfig, Error> {
        config.validate()?;
        let (account, username, password) = self.credentials()?;
        let mut builder = VwdConfig::builder()
            .account_path(account)
            .credentials(VwdCredentials::new(username, password))
            .symbols(
                config
                    .symbols
                    .iter()
                    .map(|symbol| VwdSymbol::direct(symbol.clone())),
            );
        if let Some(url) = &self.rest_url {
            builder = builder
                .base_url(url.clone())
                .poll_interval(Duration::from_millis(50));
        }
        builder
            .build()
            .map_err(|_| Error::Config("VWD configuration is invalid".to_string()))
    }
}

impl Default for VwdAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionAdapter for VwdAdapter {
    fn validate(&self, config: &SessionConfig) -> Result<(), Error> {
        let _ = self.build_config(config)?;
        Ok(())
    }

    fn start(
        &self,
        config: &SessionConfig,
        sink: LossIntolerantSender,
        session_token: CancellationToken,
    ) -> Result<ProviderRun, Error> {
        let provider_config = self.build_config(config)?;
        let connector = VwdConnector::builder(provider_config)
            .build()
            .map_err(|_| Error::Config("VWD configuration is invalid".to_string()))?;
        let (handle, join) = connector.run(Owned::new(sink), session_token);
        let mut owned = AbortOnDropHandle::new(join);
        Ok(ProviderRun {
            future: Box::pin(async move {
                let _handle = handle;
                (&mut owned)
                    .await
                    .map_err(|_| MdcError::Protocol("VWD provider task failed".to_string()))?
            }),
            abort_guards: Vec::new(),
        })
    }
}
