//! Frankfurter dated reference rates, using the provider's default hourly cadence.

use std::time::Duration;

use mdc_core::{MdcError, Owned};
use mdc_providers::frankfurter::{FrankfurterConfig, FrankfurterConnector};
use mdc_sinks::loss_intolerant::LossIntolerantSender;
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::config::SessionConfig;
use crate::error::Error;
use crate::provider::{AbortOnDropHandle, ProviderRun, SessionAdapter};

/// Adapter for reference-rate-only Frankfurter sessions.
#[derive(Debug, Default)]
pub struct FrankfurterAdapter {
    rest_url: Option<Url>,
}

impl FrankfurterAdapter {
    /// Uses production endpoints and the default one-hour poll interval.
    pub fn new() -> Self {
        Self::default()
    }

    /// Redirects REST to a mock and shortens polling to 50 ms for offline tests.
    pub fn with_endpoints(rest_url: Url) -> Self {
        Self {
            rest_url: Some(rest_url),
        }
    }
}

impl SessionAdapter for FrankfurterAdapter {
    fn start(
        &self,
        config: &SessionConfig,
        sink: LossIntolerantSender,
        session_token: CancellationToken,
    ) -> Result<ProviderRun, Error> {
        config.validate()?;
        let base = config
            .base
            .as_ref()
            .ok_or_else(|| Error::Config("Frankfurter requires a base currency".to_string()))?;
        let mut builder = FrankfurterConfig::builder()
            .base_currency(base.to_string())
            .quote_currencies(config.symbols.iter().map(ToString::to_string).collect());
        if let Some(url) = &self.rest_url {
            builder = builder
                .base_url(url.as_str())
                .poll_interval(Duration::from_millis(50));
        }
        let provider_config = builder
            .build()
            .map_err(|_| Error::Config("Frankfurter configuration is invalid".to_string()))?;
        let connector = FrankfurterConnector::builder(provider_config)
            .build()
            .map_err(|_| {
                Error::Config("Frankfurter connector configuration is invalid".to_string())
            })?;
        let (handle, join) = connector.run(Owned::new(sink), session_token);
        // Capture the guard before the future's first poll, including early drop.
        let mut owned = AbortOnDropHandle::new(join);
        Ok(ProviderRun {
            future: Box::pin(async move {
                let _handle = handle;
                (&mut owned).await.map_err(|_| {
                    MdcError::Protocol("Frankfurter provider task failed".to_string())
                })?
            }),
            abort_guards: Vec::new(),
        })
    }
}
